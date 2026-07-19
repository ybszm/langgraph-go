package prebuilt_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/wahanbo/langgraph-go/prebuilt"
)

func TestContentBlocksValidateAndClone(t *testing.T) {
	blocks := []prebuilt.ContentBlock{
		prebuilt.TextBlock("hello"),
		prebuilt.ImageURLBlock("https://example.test/image.png"),
		prebuilt.ImageDataBlock("image/png", []byte{1, 2, 3}),
		prebuilt.AudioDataBlock("audio/wav", []byte{4, 5}),
		prebuilt.FileDataBlock("report.pdf", "application/pdf", []byte{6, 7}),
		prebuilt.JSONBlock(json.RawMessage(`{"ok":true}`)),
	}
	if err := prebuilt.ValidateContentBlocks(blocks); err != nil {
		t.Fatal(err)
	}
	cloned := prebuilt.CloneContentBlocks(blocks)
	cloned[2].Data[0] = 9
	cloned[5].JSON[0] = '['
	if blocks[2].Data[0] != 1 || blocks[5].JSON[0] != '{' {
		t.Fatal("content block clone aliases source bytes")
	}
}

func TestContentBlocksRejectInvalidDiscriminatedPayloads(t *testing.T) {
	tests := []prebuilt.ContentBlock{
		{Type: "unknown"},
		{Type: prebuilt.ContentImage},
		{Type: prebuilt.ContentImage, URL: "https://example.test", Data: []byte{1}, MIMEType: "image/png"},
		{Type: prebuilt.ContentAudio, Data: []byte{1}},
		{Type: prebuilt.ContentJSON, JSON: json.RawMessage(`{`)},
	}
	for _, block := range tests {
		if err := prebuilt.ValidateContentBlocks([]prebuilt.ContentBlock{block}); !errors.Is(err, prebuilt.ErrInvalidContentBlock) {
			t.Fatalf("block=%+v error=%v", block, err)
		}
	}
}

func TestMessageCloneIsolatesContentBlocks(t *testing.T) {
	message := prebuilt.AssistantMessage{ID: "one", ContentBlocks: []prebuilt.ContentBlock{prebuilt.ImageDataBlock("image/png", []byte{1})}}
	cloned := message.CloneMessage().(prebuilt.AssistantMessage)
	cloned.ContentBlocks[0].Data[0] = 2
	if message.ContentBlocks[0].Data[0] != 1 {
		t.Fatal("message content blocks were aliased")
	}
}

func TestToolArtifactResultPreservesContentAndArtifact(t *testing.T) {
	artifact := map[string]any{"document_id": "doc-1"}
	result := prebuilt.ArtifactResult[reactDelta]("summary", artifact)
	if result.Message == nil || result.Message.Content != "summary" || !reflect.DeepEqual(result.Message.Artifact, artifact) {
		t.Fatalf("result=%+v", result)
	}
}
