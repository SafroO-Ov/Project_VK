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

func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	sp := subpub.NewSubPub()

	var fastReceived, slowReceived []string
	var mu sync.Mutex
	done := make(chan struct{})

	// Медленный подписчик (ждёт перед приёмом)
	_, err := sp.Subscribe("topic", func(msg interface{}) {
		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		slowReceived = append(slowReceived, msg.(string))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Быстрый подписчик
	_, err = sp.Subscribe("topic", func(msg interface{}) {
		mu.Lock()
		fastReceived = append(fastReceived, msg.(string))
		mu.Unlock()
		if len(fastReceived) == 3 {
			close(done)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	msgs := []string{"one", "two", "three"}
	for _, m := range msgs {
		if err := sp.Publish("topic", m); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Fast subscriber did not receive all messages in time")
	}

	sp.Close(context.Background())

	mu.Lock()
	defer mu.Unlock()

	if len(fastReceived) != 3 {
		t.Errorf("Fast subscriber got %d messages, want 3", len(fastReceived))
	}
	if len(slowReceived) != 3 {
		t.Errorf("Slow subscriber got %d messages, want 3", len(slowReceived))
	}
}

func TestFIFOOrderPreserved(t *testing.T) {
	sp := subpub.NewSubPub()
	var mu sync.Mutex
	var received []string
	done := make(chan struct{})

	_, err := sp.Subscribe("order", func(msg interface{}) {
		mu.Lock()
		received = append(received, msg.(string))
		if len(received) == 5 {
			close(done)
		}
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	expected := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	for _, m := range expected {
		if err := sp.Publish("order", m); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for all messages")
	}

	mu.Lock()
	defer mu.Unlock()
	for i, msg := range expected {
		if received[i] != msg {
			t.Errorf("Expected %s at index %d, got %s", msg, i, received[i])
		}
	}
}
