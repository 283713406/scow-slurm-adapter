package resource

import (
	"bufio"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os/exec"
	"scow-slurm-adapter/caller"
	pb "scow-slurm-adapter/gen/go"
	"scow-slurm-adapter/utils"
	"strconv"
	"strings"
	"time"
)

type Exporter struct {
	stop        chan struct{}
	eventSource <-chan Info
}

var (
	slurmConfigPath = "/etc/slurm"
	sleepInterval   = time.Duration(60) * time.Second

	NodeInfos = make(map[string]*pb.NodeInfo)
)

// NewResourceExporter creates a new ResourceExporter instance.
func NewResourceExporter() (*Exporter, error) {
	eventSource := make(chan Info)
	ntf, err := newNotifier(sleepInterval, eventSource, slurmConfigPath)
	if err != nil {
		return nil, err
	}
	go ntf.Run()

	exp := &Exporter{
		stop:        make(chan struct{}, 1),
		eventSource: eventSource,
	}

	return exp, nil
}

func (w *Exporter) Run() {
	w.updateNodeResource()
	for {
		select {
		case info := <-w.eventSource:
			caller.Logger.Tracef("event received, scanning... event %v", info.Event)
			w.updateNodeResource()

		case <-w.stop:
			caller.Logger.Tracef("shutting down node-topology-updater from stop")
		}
	}
}

func (w *Exporter) Stop() {
	select {
	case w.stop <- struct{}{}:
	default:
	}
}

func (w *Exporter) updateNodeResource() {
	getNodesInfoCmd := "scontrol show nodes --oneliner" // 获取全部计算节点主机名
	cmd := exec.Command("bash", "-c", getNodesInfoCmd)
	output, err := cmd.CombinedOutput()
	if err != nil {
		errInfo := &errdetails.ErrorInfo{
			Reason: "COMMAND_EXEC_FAILED",
		}
		st := status.New(codes.Internal, "Exec command failed or slurmctld down.")
		st, _ = st.WithDetails(errInfo)
		caller.Logger.Tracef("timer update received %v", st.Err())
	}
	// 按行分割输出
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		nodeInfo := extractNodeInfo(line)
		if _, ok := NodeInfos[nodeInfo.NodeName]; ok {
			continue
		}
		NodeInfos[nodeInfo.NodeName] = nodeInfo
	}

	for _, item := range NodeInfos {
		caller.Logger.Tracef("NodeName: %s, State: %d, GpuCount: %d\n", item.NodeName, item.State, item.GpuCount)
	}
}

func extractNodeInfo(info string) *pb.NodeInfo {
	var (
		partitionList []string
		totalGpusInt  int
		allocGpusInt  int
		nodeState     pb.NodeInfo_NodeState
	)

	nodeName := utils.ExtractValue(info, "NodeName")
	partitions := utils.ExtractValue(info, "Partitions")
	partitionList = append(partitionList, strings.Split(partitions, ",")...)
	state := utils.ExtractValue(info, "State") // 这个地方要改
	switch state {
	case "IDLE", "IDLE+PLANNED":
		nodeState = pb.NodeInfo_IDLE
	case "DOWN", "DOWN+NOT_RESPONDING", "ALLOCATED+DRAIN", "IDLE+DRAIN", "IDLE+DRAIN+NOT_RESPONDING", "DOWN+DRAIN+INVALID_REG", "IDLE+NOT_RESPONDING":
		nodeState = pb.NodeInfo_NOT_AVAILABLE
	case "ALLOCATED", "MIXED":
		nodeState = pb.NodeInfo_RUNNING
	default: // 其他不知道的状态默认为不可用的状态
		nodeState = pb.NodeInfo_NOT_AVAILABLE
	}
	totalMem := utils.ExtractValue(info, "RealMemory")
	totalMemInt, _ := strconv.Atoi(totalMem)
	AllocMem := utils.ExtractValue(info, "AllocMem")
	AllocMemInt, _ := strconv.Atoi(AllocMem)
	totalCpuCores := utils.ExtractValue(info, "CPUTot")
	totalCpuCoresInt, _ := strconv.Atoi(totalCpuCores)
	allocCpuCores := utils.ExtractValue(info, "CPUAlloc")
	allocCpuCoresInt, _ := strconv.Atoi(allocCpuCores)
	totalGpus := utils.ExtractValue(info, "Gres")
	if totalGpus == "(null)" {
		totalGpusInt = 0
	} else {
		totalGpusStr := strings.Split(totalGpus, ":")[1]
		totalGpusInt, _ = strconv.Atoi(totalGpusStr)
	}
	allocGpus := utils.ExtractValue(info, "AllocTRES")
	if allocGpus == "" {
		allocGpusInt = 0
	} else {
		if strings.Contains(allocGpus, "gpu") {
			allocRes := strings.Split(allocGpus, ",")
			for _, res := range allocRes {
				if strings.Contains(res, "gpu") {
					gpuAllocResStr := strings.Split(res, "=")[1]
					allocGpusInt, _ = strconv.Atoi(gpuAllocResStr)
					break
				}
			}
		} else {
			allocGpusInt = 0
		}
	}

	return &pb.NodeInfo{
		NodeName:          nodeName,
		Partitions:        partitionList,
		State:             nodeState,
		CpuCoreCount:      uint32(totalCpuCoresInt),
		AllocCpuCoreCount: uint32(allocCpuCoresInt),
		IdleCpuCoreCount:  uint32(totalCpuCoresInt) - uint32(allocCpuCoresInt),
		TotalMemMb:        uint32(totalMemInt),
		AllocMemMb:        uint32(AllocMemInt),
		IdleMemMb:         uint32(totalMemInt) - uint32(AllocMemInt),
		GpuCount:          uint32(totalGpusInt),
		AllocGpuCount:     uint32(allocGpusInt),
		IdleGpuCount:      uint32(totalGpusInt) - uint32(allocGpusInt),
	}
}
