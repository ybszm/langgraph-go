package checkpoint_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/savertest"
	checkpointredis "github.com/ybszm/langgraph-go/redis/checkpoint"
)

func TestSaverContract(t *testing.T) {
	server := miniredis.RunT(t)
	savertest.Run(t, func(t *testing.T) checkpoint.Saver {
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		server.FlushAll()
		saver, err := checkpointredis.New(client, checkpointredis.Options{Prefix: "contract"})
		if err != nil {
			t.Fatal(err)
		}
		return saver
	})
}
