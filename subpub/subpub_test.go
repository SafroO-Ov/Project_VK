package subpub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/SafroO-Ov/Project_VK/subpub" // замените на актуальный модульный путь
)

func TestBasicPublishSubscribe(t *testing.T) {
	sp := subpub.NewSubPub()
	var wg sync.WaitGroup
	received := make(chan string, 1)

	wg.Add(1)
	_, err := sp.Subscribe("topic1", func(msg interface{}) {
		if str, ok := msg.(string); ok {
			received <- str
		}
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = sp.Publish("topic1", "hello")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case msg := <-received:
		if msg != "hello" {
			t.Errorf("Unexpected message: %s", msg)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for message")
	}

	wg.Wait()
	sp.Close(context.Background())
}

func TestUnsubscribe(t *testing.T) {
	sp := subpub.NewSubPub()
	received := make(chan string, 1)

	sub, err := sp.Subscribe("topic2", func(msg interface{}) {
		if str, ok := msg.(string); ok {
			received <- str
		}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	sub.Unsubscribe()

	err = sp.Publish("topic2", "should not receive")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case msg := <-received:
		t.Errorf("Received message after unsubscribe: %s", msg)
	case <-time.After(300 * time.Millisecond):
		// Всё правильно — сообщений быть не должно
	}

	sp.Close(context.Background())
}

func TestCloseWithContext(t *testing.T) {
	sp := subpub.NewSubPub()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := sp.Close(ctx)
	if err != nil {
		t.Errorf("Unexpected error on close: %v", err)
	}
}
