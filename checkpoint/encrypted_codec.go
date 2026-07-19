package checkpoint

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
)

// EncryptedTypePrefix distinguishes AES-GCM ciphertext from its wrapped
// codec's plaintext envelope while retaining the underlying stable type.
const EncryptedTypePrefix = "encrypted+aesgcm/"

const encryptedCodecFormat byte = 1

type encryptedCodecOptions struct {
	random io.Reader
	keyID  string
}

// EncryptedCodecOption configures an encrypted codec wrapper.
type EncryptedCodecOption func(*encryptedCodecOptions) error

// WithEncryptionRandom injects nonce entropy. The wrapper serializes reads,
// so the supplied reader does not need to be concurrency-safe.
func WithEncryptionRandom(reader io.Reader) EncryptedCodecOption {
	return func(options *encryptedCodecOptions) error {
		if reader == nil {
			return fmt.Errorf("%w: random reader is nil", ErrEncryption)
		}
		options.random = reader
		return nil
	}
}

// WithEncryptionKeyID adds a validated key identifier to the ciphertext type.
// It is primarily used by RotatingEncryptedCodec for deterministic key lookup.
func WithEncryptionKeyID(keyID string) EncryptedCodecOption {
	return func(options *encryptedCodecOptions) error {
		if err := validateEncryptionKeyID(keyID); err != nil {
			return err
		}
		options.keyID = keyID
		return nil
	}
}

// EncryptedCodec encrypts the Data produced by another typed Codec with
// AES-GCM. Type, version, and format are authenticated but remain available
// for saver indexing and codec dispatch.
type EncryptedCodec[T any] struct {
	inner  Codec[T]
	aead   cipher.AEAD
	random io.Reader
	keyID  string
	randMu sync.Mutex
}

// NewEncryptedCodec wraps inner using an AES key of 16, 24, or 32 bytes.
// Nil options use crypto/rand.Reader for a fresh nonce on every Encode.
func NewEncryptedCodec[T any](
	inner Codec[T],
	key []byte,
	options ...EncryptedCodecOption,
) (*EncryptedCodec[T], error) {
	if inner == nil {
		return nil, fmt.Errorf("%w: inner codec is nil", ErrEncryption)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: create AES cipher: %v", ErrEncryption, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: create AES-GCM: %v", ErrEncryption, err)
	}
	configured := encryptedCodecOptions{random: rand.Reader}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrEncryption, index)
		}
		if err := option(&configured); err != nil {
			return nil, err
		}
	}
	return &EncryptedCodec[T]{inner: inner, aead: aead, random: configured.random, keyID: configured.keyID}, nil
}

// Encode implements Codec.
func (c *EncryptedCodec[T]) Encode(value T) (EncodedValue, error) {
	encoded, err := c.inner.Encode(value)
	if err != nil {
		return EncodedValue{}, err
	}
	if encoded.Type == "" || encoded.Version <= 0 {
		return EncodedValue{}, fmt.Errorf("%w: inner codec returned invalid identity", ErrEncryption)
	}
	nonce := make([]byte, c.aead.NonceSize())
	c.randMu.Lock()
	_, randomErr := io.ReadFull(c.random, nonce)
	c.randMu.Unlock()
	if randomErr != nil {
		return EncodedValue{}, fmt.Errorf("%w: generate nonce: %w", ErrEncryption, randomErr)
	}
	encryptedType := c.typePrefix() + encoded.Type
	data := make([]byte, 1+c.aead.NonceSize(), 1+c.aead.NonceSize()+len(encoded.Data)+c.aead.Overhead())
	data[0] = encryptedCodecFormat
	copy(data[1:], nonce)
	data = c.aead.Seal(data, nonce, encoded.Data, encryptedCodecAAD(encryptedType, encoded.Version))
	return EncodedValue{Type: encryptedType, Version: encoded.Version, Data: data}, nil
}

// Decode implements Codec and rejects any change to type, version, format,
// nonce, or ciphertext before invoking the wrapped codec.
func (c *EncryptedCodec[T]) Decode(value EncodedValue) (T, error) {
	var zero T
	prefix := c.typePrefix()
	if !strings.HasPrefix(value.Type, prefix) || len(value.Type) == len(prefix) {
		return zero, fmt.Errorf("%w: got encrypted type %q", ErrCodecMismatch, value.Type)
	}
	minimum := 1 + c.aead.NonceSize() + c.aead.Overhead()
	if len(value.Data) < minimum {
		return zero, fmt.Errorf("%w: ciphertext is truncated", ErrEncryption)
	}
	if value.Data[0] != encryptedCodecFormat {
		return zero, fmt.Errorf("%w: unsupported ciphertext format %d", ErrEncryption, value.Data[0])
	}
	nonce := value.Data[1 : 1+c.aead.NonceSize()]
	ciphertext := value.Data[1+c.aead.NonceSize():]
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, encryptedCodecAAD(value.Type, value.Version))
	if err != nil {
		return zero, fmt.Errorf("%w: authenticate ciphertext", ErrEncryption)
	}
	return c.inner.Decode(EncodedValue{
		Type: strings.TrimPrefix(value.Type, prefix), Version: value.Version, Data: plaintext,
	})
}

func (c *EncryptedCodec[T]) typePrefix() string {
	if c.keyID == "" {
		return EncryptedTypePrefix
	}
	return EncryptedTypePrefix + c.keyID + "/"
}

// RotatingEncryptedCodec encrypts with one active key ID while retaining a
// keyring for deterministic decryption of older ciphertext.
type RotatingEncryptedCodec[T any] struct {
	activeID string
	codecs   map[string]*EncryptedCodec[T]
}

// EncryptionKeyResolver resolves AES key material by immutable ciphertext key
// ID. Implementations can bridge KMS, Vault, HSM, or an application cache.
type EncryptionKeyResolver interface {
	ResolveEncryptionKey(keyID string) ([]byte, error)
}

// EncryptionKeyResolverFunc adapts a function to EncryptionKeyResolver.
type EncryptionKeyResolverFunc func(string) ([]byte, error)

func (resolve EncryptionKeyResolverFunc) ResolveEncryptionKey(keyID string) ([]byte, error) {
	return resolve(keyID)
}

// ResolvingEncryptedCodec resolves the active key on Encode and the embedded
// historical key ID on Decode. Key bytes are copied and cleared after the
// operation; resolvers retain ownership of their returned storage.
type ResolvingEncryptedCodec[T any] struct {
	inner    Codec[T]
	activeID string
	resolver EncryptionKeyResolver
	random   io.Reader
	encodeMu sync.Mutex
}

// NewResolvingEncryptedCodec constructs a lazy key-ID AES-GCM wrapper.
// WithEncryptionRandom is supported for deterministic tests; key IDs must be
// supplied through activeID rather than WithEncryptionKeyID.
func NewResolvingEncryptedCodec[T any](
	inner Codec[T],
	activeID string,
	resolver EncryptionKeyResolver,
	options ...EncryptedCodecOption,
) (*ResolvingEncryptedCodec[T], error) {
	if inner == nil || resolver == nil {
		return nil, fmt.Errorf("%w: inner codec and key resolver are required", ErrEncryption)
	}
	if err := validateEncryptionKeyID(activeID); err != nil {
		return nil, fmt.Errorf("%w: active key ID: %v", ErrEncryption, err)
	}
	configured := encryptedCodecOptions{random: rand.Reader}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrEncryption, index)
		}
		if err := option(&configured); err != nil {
			return nil, err
		}
	}
	if configured.keyID != "" {
		return nil, fmt.Errorf("%w: resolver codec key ID is controlled by activeID", ErrEncryption)
	}
	return &ResolvingEncryptedCodec[T]{
		inner: inner, activeID: activeID, resolver: resolver, random: configured.random,
	}, nil
}

// Encode implements Codec using only the configured active key ID.
func (c *ResolvingEncryptedCodec[T]) Encode(value T) (EncodedValue, error) {
	c.encodeMu.Lock()
	defer c.encodeMu.Unlock()
	codec, key, err := c.resolve(c.activeID, true)
	if err != nil {
		return EncodedValue{}, err
	}
	defer clear(key)
	return codec.Encode(value)
}

// Decode implements Codec by resolving exactly the authenticated type key ID.
func (c *ResolvingEncryptedCodec[T]) Decode(value EncodedValue) (T, error) {
	var zero T
	keyID, err := encryptedKeyID(value.Type)
	if err != nil {
		return zero, err
	}
	codec, key, err := c.resolve(keyID, false)
	if err != nil {
		return zero, err
	}
	defer clear(key)
	return codec.Decode(value)
}

func (c *ResolvingEncryptedCodec[T]) resolve(keyID string, forEncode bool) (*EncryptedCodec[T], []byte, error) {
	key, err := c.resolver.ResolveEncryptionKey(keyID)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: resolve key %q: %w", ErrEncryption, keyID, err)
	}
	owned := append([]byte(nil), key...)
	options := []EncryptedCodecOption{WithEncryptionKeyID(keyID)}
	if forEncode {
		options = append(options, WithEncryptionRandom(c.random))
	}
	codec, err := NewEncryptedCodec(c.inner, owned, options...)
	if err != nil {
		clear(owned)
		return nil, nil, fmt.Errorf("%w: resolved key %q: %v", ErrEncryption, keyID, err)
	}
	return codec, owned, nil
}

// NewRotatingEncryptedCodec constructs a key-ID based AES-GCM keyring. Every
// key is validated eagerly and activeID must exist in keys.
func NewRotatingEncryptedCodec[T any](
	inner Codec[T],
	activeID string,
	keys map[string][]byte,
) (*RotatingEncryptedCodec[T], error) {
	if inner == nil {
		return nil, fmt.Errorf("%w: inner codec is nil", ErrEncryption)
	}
	if err := validateEncryptionKeyID(activeID); err != nil {
		return nil, fmt.Errorf("%w: active key ID: %v", ErrEncryption, err)
	}
	if _, exists := keys[activeID]; !exists {
		return nil, fmt.Errorf("%w: active key %q is absent", ErrEncryption, activeID)
	}
	codecs := make(map[string]*EncryptedCodec[T], len(keys))
	for keyID, key := range keys {
		if err := validateEncryptionKeyID(keyID); err != nil {
			return nil, err
		}
		codec, err := NewEncryptedCodec(inner, key, WithEncryptionKeyID(keyID))
		if err != nil {
			return nil, fmt.Errorf("%w: key %q: %v", ErrEncryption, keyID, err)
		}
		codecs[keyID] = codec
	}
	return &RotatingEncryptedCodec[T]{activeID: activeID, codecs: codecs}, nil
}

// Encode implements Codec using the active key.
func (c *RotatingEncryptedCodec[T]) Encode(value T) (EncodedValue, error) {
	return c.codecs[c.activeID].Encode(value)
}

// Decode implements Codec by selecting exactly the key ID carried in Type.
func (c *RotatingEncryptedCodec[T]) Decode(value EncodedValue) (T, error) {
	var zero T
	keyID, err := encryptedKeyID(value.Type)
	if err != nil {
		return zero, err
	}
	codec, exists := c.codecs[keyID]
	if !exists {
		return zero, fmt.Errorf("%w: key %q is unavailable", ErrEncryption, keyID)
	}
	return codec.Decode(value)
}

func encryptedKeyID(typeName string) (string, error) {
	if !strings.HasPrefix(typeName, EncryptedTypePrefix) {
		return "", fmt.Errorf("%w: got encrypted type %q", ErrCodecMismatch, typeName)
	}
	remainder := strings.TrimPrefix(typeName, EncryptedTypePrefix)
	separator := strings.IndexByte(remainder, '/')
	if separator <= 0 {
		return "", fmt.Errorf("%w: ciphertext type has no key ID", ErrEncryption)
	}
	keyID := remainder[:separator]
	if err := validateEncryptionKeyID(keyID); err != nil {
		return "", err
	}
	return keyID, nil
}

func validateEncryptionKeyID(keyID string) error {
	if keyID == "" || len(keyID) > 128 {
		return fmt.Errorf("%w: key ID must contain 1 to 128 characters", ErrEncryption)
	}
	for _, character := range keyID {
		valid := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.'
		if !valid {
			return fmt.Errorf("%w: key ID %q contains an invalid character", ErrEncryption, keyID)
		}
	}
	return nil
}

func encryptedCodecAAD(typeName string, version int) []byte {
	const domain = "langgraph-go/checkpoint/aes-gcm/v1"
	result := make([]byte, len(domain)+1+4+len(typeName)+8)
	copy(result, domain)
	offset := len(domain)
	result[offset] = encryptedCodecFormat
	offset++
	binary.BigEndian.PutUint32(result[offset:], uint32(len(typeName)))
	offset += 4
	copy(result[offset:], typeName)
	offset += len(typeName)
	binary.BigEndian.PutUint64(result[offset:], uint64(version))
	return result
}
