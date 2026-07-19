package prebuilt

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ContentType discriminates provider-neutral multimodal content blocks.
type ContentType string

const (
	// ContentText carries UTF-8 text.
	ContentText ContentType = "text"
	// ContentImage carries an image URL or MIME-typed bytes.
	ContentImage ContentType = "image"
	// ContentAudio carries MIME-typed audio bytes.
	ContentAudio ContentType = "audio"
	// ContentFile carries a file URL or MIME-typed bytes.
	ContentFile ContentType = "file"
	// ContentJSON carries one valid JSON value.
	ContentJSON ContentType = "json"
)

// ErrInvalidContentBlock classifies malformed discriminated content.
var ErrInvalidContentBlock = errors.New("invalid content block")

// ContentBlock carries one text, media, file, or structured JSON part.
type ContentBlock struct {
	Type     ContentType     `json:"type"`
	Text     string          `json:"text,omitempty"`
	URL      string          `json:"url,omitempty"`
	Name     string          `json:"name,omitempty"`
	MIMEType string          `json:"mime_type,omitempty"`
	Data     []byte          `json:"data,omitempty"`
	JSON     json.RawMessage `json:"json,omitempty"`
}

// TextBlock constructs a text content block.
func TextBlock(text string) ContentBlock {
	return ContentBlock{Type: ContentText, Text: text}
}

// ImageURLBlock constructs an externally hosted image block.
func ImageURLBlock(url string) ContentBlock {
	return ContentBlock{Type: ContentImage, URL: url}
}

// ImageDataBlock constructs an inline image block with copied bytes.
func ImageDataBlock(mimeType string, data []byte) ContentBlock {
	return ContentBlock{Type: ContentImage, MIMEType: mimeType, Data: append([]byte(nil), data...)}
}

// AudioDataBlock constructs an inline audio block with copied bytes.
func AudioDataBlock(mimeType string, data []byte) ContentBlock {
	return ContentBlock{Type: ContentAudio, MIMEType: mimeType, Data: append([]byte(nil), data...)}
}

// FileDataBlock constructs an inline file block with copied bytes.
func FileDataBlock(name, mimeType string, data []byte) ContentBlock {
	return ContentBlock{Type: ContentFile, Name: name, MIMEType: mimeType, Data: append([]byte(nil), data...)}
}

// JSONBlock constructs a structured block with copied raw JSON.
func JSONBlock(value json.RawMessage) ContentBlock {
	return ContentBlock{Type: ContentJSON, JSON: append(json.RawMessage(nil), value...)}
}

// ValidateContentBlocks checks each discriminated block payload.
func ValidateContentBlocks(blocks []ContentBlock) error {
	for index, block := range blocks {
		var err error
		switch block.Type {
		case ContentText:
		case ContentImage:
			hasURL, hasData := block.URL != "", len(block.Data) > 0
			if hasURL == hasData || hasData && block.MIMEType == "" {
				err = errors.New("image requires exactly one URL or MIME-typed data payload")
			}
		case ContentAudio:
			if len(block.Data) == 0 || block.MIMEType == "" {
				err = errors.New("audio requires MIME-typed data")
			}
		case ContentFile:
			hasURL, hasData := block.URL != "", len(block.Data) > 0
			if hasURL == hasData || hasData && block.MIMEType == "" {
				err = errors.New("file requires exactly one URL or MIME-typed data payload")
			}
		case ContentJSON:
			if len(block.JSON) == 0 || !json.Valid(block.JSON) {
				err = errors.New("json block requires valid JSON")
			}
		default:
			err = fmt.Errorf("unknown content type %q", block.Type)
		}
		if err != nil {
			return fmt.Errorf("%w at index %d: %v", ErrInvalidContentBlock, index, err)
		}
	}
	return nil
}

// CloneContentBlocks returns a deep copy of block byte payloads.
func CloneContentBlocks(blocks []ContentBlock) []ContentBlock {
	if blocks == nil {
		return nil
	}
	result := make([]ContentBlock, len(blocks))
	for index, block := range blocks {
		result[index] = block
		result[index].Data = append([]byte(nil), block.Data...)
		result[index].JSON = append(json.RawMessage(nil), block.JSON...)
	}
	return result
}
