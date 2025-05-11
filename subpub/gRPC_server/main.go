package server

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/SafroO-Ov/Project_VK/subpub"
	pb "github.com/SafroO-Ov/Project_VK/subpub/proto"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Port string `yaml:"port"`
}

func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg Config
	err = yaml.NewDecoder(f).Decode(&cfg)
	return &cfg, err
}

func main() {
	cfg, err := loadConfig("server/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	bus := subpub.NewSubPub()
	pubSubSrv := server.NewPubSubServer(bus)

	pb.RegisterPubSubServer(grpcServer, pubSubSrv)

	// graceful shutdown
	go func() {
		log.Printf("gRPC server listening on :%s", cfg.Port)
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// ловим SIGINT / SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down...")
	if err := bus.Close(context.Background()); err != nil {
		log.Printf("failed to close pubsub: %v", err)
	}
	grpcServer.GracefulStop()
}
