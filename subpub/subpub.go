package subpub

import (
	"context"
	"errors"
	"log"
	"sync"
)

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg interface{}) error
	Close(ctx context.Context) error
}

type subscriber struct {
	handler MessageHandler
	ch      chan interface{}
	stop    chan struct{}
}

type subscription struct {
	sp      *subpub
	subject string
	sub     *subscriber
}

type subpub struct {
	mu          sync.RWMutex
	subscribers map[string][]*subscriber
	closed      bool
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

func NewSubPub() SubPub {
	log.Println("SubPub initialized")
	return &subpub{
		subscribers: make(map[string][]*subscriber),
		closeCh:     make(chan struct{}),
	}
}

func (sp *subpub) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.closed {
		return nil, errors.New("subpub is closed")
	}

	sub := &subscriber{
		handler: cb,
		ch:      make(chan interface{}, 16),
		stop:    make(chan struct{}),
	}

	sp.subscribers[subject] = append(sp.subscribers[subject], sub)
	log.Printf("Subscribed to subject: %s\n", subject)

	sp.wg.Add(1)
	go sp.run(sub)

	return &subscription{sp: sp, subject: subject, sub: sub}, nil
}

func (sp *subpub) run(sub *subscriber) {
	defer sp.wg.Done()
	for {
		select {
		case msg := <-sub.ch:
			sub.handler(msg)
		case <-sub.stop:
			log.Println("Subscriber stopped")
			return
		case <-sp.closeCh:
			log.Println("SubPub is closing; terminating subscriber")
			return
		}
	}
}

func (sp *subpub) Publish(subject string, msg interface{}) error {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	if sp.closed {
		return errors.New("subpub is closed")
	}

	log.Printf("Publishing message to subject: %s\n", subject)
	for _, sub := range sp.subscribers[subject] {
		select {
		case sub.ch <- msg:
			log.Printf("Message for subject %s deliveredr\n", subject)
		default:
			log.Printf("Dropping message for subject %s: subscriber channel full\n", subject)
		}
	}
	return nil
}

func (s *subscription) Unsubscribe() {
	s.sp.mu.Lock()
	defer s.sp.mu.Unlock()

	subs := s.sp.subscribers[s.subject]
	for i, sub := range subs {
		if sub == s.sub {
			s.sp.subscribers[s.subject] = append(subs[:i], subs[i+1:]...)
			close(sub.stop)
			log.Printf("Unsubscribed from subject: %s\n", s.subject)
			break
		}
	}
}

func (sp *subpub) Close(ctx context.Context) error {
	sp.mu.Lock()
	if sp.closed {
		sp.mu.Unlock()
		return nil
	}
	sp.closed = true
	close(sp.closeCh)
	log.Println("SubPub is closing")
	sp.mu.Unlock()

	ch := make(chan struct{})
	go func() {
		sp.wg.Wait()
		close(ch)
	}()

	select {
	case <-ctx.Done():
		log.Println("SubPub close aborted: context canceled")
		return ctx.Err()
	case <-ch:
		log.Println("SubPub closed successfully")
		return nil
	}
}
