package main

import (
	"log"
	"os"

	"fmt"
	"net"

	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
	exporter "scow-slurm-adapter/resource"

	"scow-slurm-adapter/services/account"
	"scow-slurm-adapter/services/app"
	"scow-slurm-adapter/services/config"
	"scow-slurm-adapter/services/job"
	"scow-slurm-adapter/services/user"
	"scow-slurm-adapter/services/version"

	"google.golang.org/grpc"
)

func main() {
	// Get new TopologyUpdater instance
	instance, err := exporter.NewResourceExporter()
	if err != nil {
		caller.Logger.Warnf("failed to initialize topology updater instance, %v", err)
	}
	go instance.Run()
	defer instance.Stop()

	os.Setenv("SLURM_TIME_FORMAT", "standard") // 新加slurm环境变量
	// 启动服务
	portString := fmt.Sprintf(":%d", caller.ConfigValue.Service.Port)
	lis, err := net.Listen("tcp", portString)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}

	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024), // 最大接受size 1GB
		grpc.MaxSendMsgSize(1024*1024*1024), // 最大发送size 1GB
	) // 创建gRPC服务器
	pb.RegisterUserServiceServer(s, &user.ServerUser{})
	pb.RegisterAccountServiceServer(s, &account.ServerAccount{})
	pb.RegisterConfigServiceServer(s, &config.ServerConfig{})
	pb.RegisterJobServiceServer(s, &job.ServerJob{})
	pb.RegisterVersionServiceServer(s, &version.ServerVersion{})
	pb.RegisterAppServiceServer(s, &app.ServerAppServer{})

	if err = s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
