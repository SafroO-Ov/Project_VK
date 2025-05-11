package server

import (
	"context"
	"log"
	"sync"

	"github.com/SafroO-Ov/Project_VK/subpub"
	pb "github.com/SafroO-Ov/Project_VK/subpub/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PubSubServer struct {
	pb.UnimplementedPubSubServer
	subpub subpub.SubPub
}

// Subscribe реализует стриминг событий для клиента
func (s *PubSubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	key := req.GetKey()
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}

	ctx := stream.Context()
	var wg sync.WaitGroup
	done := make(chan struct{})

	sub, err := s.subpub.Subscribe(key, func(msg interface{}) {
		event, ok := msg.(string)
		if !ok {
			log.Printf("invalid message type: %T", msg)
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := stream.Send(&pb.Event{Data: event}); err != nil {
				log.Printf("failed to send event: %v", err)
			}
		}()
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe: %v", err)
	}

	// Завершаем при отмене контекста
	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
		close(done)
	}()

	<-done
	wg.Wait()
	return nil
}

// Publish реализует публикацию события по ключу
func (s *PubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if err := s.subpub.Publish(req.GetKey(), req.GetData()); err != nil {
		log.Printf("publish error: %v", err)
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}

	return &emptypb.Empty{}, nil
}
