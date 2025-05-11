package main

import (
	"context"
	"log"
	"sync"

	"github.com/SafroO-Ov/Project_VK/subpub"
	pb "github.com/SafroO-Ov/Project_VK/subpub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pubSubServer struct {
	pb.UnimplementedPubSubServer
	bus subpub.SubPub
}

// NewPubSubServer создает новый gRPC PubSub-сервер
func NewPubSubServer(bus subpub.SubPub) pb.PubSubServer {
	return &pubSubServer{
		bus: bus,
	}
}

func (s *pubSubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	log.Printf("New subscriber for key: %s", req.Key)

	ctx := stream.Context()
	wg := sync.WaitGroup{}
	wg.Add(1)

	sub, err := s.bus.Subscribe(req.Key, func(msg interface{}) {
		// Преобразуем сообщение к строке и отправляем через gRPC stream
		str, ok := msg.(string)
		if !ok {
			log.Printf("invalid message type: %T", msg)
			return
		}

		if err := stream.Send(&pb.Event{Data: str}); err != nil {
			log.Printf("failed to send to subscriber: %v", err)
		}
	})
	if err != nil {
		log.Printf("Subscribe error: %v", err)
		return status.Errorf(codes.Internal, "subscribe failed: %v", err)
	}

	// Ждем завершения или отмены
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Printf("Unsubscribing key: %s", req.Key)
		sub.Unsubscribe()
	}()

	wg.Wait()
	return nil
}

func (s *pubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.Empty, error) {
	log.Printf("Publishing to key %s: %s", req.Key, req.Data)
	err := s.bus.Publish(req.Key, req.Data)
	if err != nil {
		log.Printf("Publish error: %v", err)
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}
	return &pb.Empty{}, nil
}
