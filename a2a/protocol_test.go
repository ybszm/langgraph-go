package a2a_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/ybszm/langgraph-go/a2a"
)

func TestServerCardAndSend(t *testing.T) {
	handler := &a2a.Server{
		Card: a2a.AgentCard{Name: "demo", Skills: []string{"echo"}},
		Handler: func(_ context.Context, req a2a.SendRequest) (a2a.SendResponse, error) {
			return a2a.SendResponse{
				SessionID: req.SessionID,
				Message:   a2a.Message{Role: "agent", Content: "echo:" + req.Message.Content},
			}, nil
		},
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	client := &a2a.Client{BaseURL: server.URL}
	card, err := client.GetCard(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if card.Name != "demo" {
		t.Fatalf("card=%+v", card)
	}
	resp, err := client.Send(context.Background(), a2a.SendRequest{
		SessionID: "s1",
		Message:   a2a.Message{Content: "ping"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Message.Content != "echo:ping" || resp.SessionID != "s1" {
		t.Fatalf("resp=%+v", resp)
	}
}
