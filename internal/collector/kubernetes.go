package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	podResourcesAPI "k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
)

const (
	PodResourceSocket  = "/var/lib/kubelet/pod-resources/kubelet.sock"
	RBLNResourcePrefix = "rebellions.ai"
	SysfsDriverPools   = "/sys/bus/pci/drivers/rebellions/%s/pools"
)

type PodResourceInfo struct {
	Name          string
	Namespace     string
	ContainerName string
}

type PodResourceMapper struct{}

func NewPodResourceMapper() *PodResourceMapper {
	return &PodResourceMapper{}
}

func (p *PodResourceMapper) GetResourcesInfo() (map[string]PodResourceInfo, error) {
	podResourcesInfo := make(map[string]PodResourceInfo)

	podResources, err := p.getPodResources()
	if err != nil {
		return nil, err
	}

	for _, pod := range podResources.GetPodResources() {
		for _, container := range pod.GetContainers() {
			for _, containerDevice := range container.GetDevices() {
				if strings.HasPrefix(containerDevice.GetResourceName(), RBLNResourcePrefix) {
					for _, deviceID := range containerDevice.GetDeviceIds() {
						deviceName, err := getDeviceName(deviceID)
						if err != nil {
							return nil, err
						}
						podResourcesInfo[deviceName] = PodResourceInfo{
							Name:          pod.Name,
							Namespace:     pod.Namespace,
							ContainerName: container.Name,
						}
					}
				}
			}
		}
	}

	return podResourcesInfo, nil
}

func (p *PodResourceMapper) getPodResources() (*podResourcesAPI.ListPodResourcesResponse, error) {
	client, cleanup, err := newKubeletClient()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	podResourcesClient := podResourcesAPI.NewPodResourcesListerClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := podResourcesClient.List(ctx, &podResourcesAPI.ListPodResourcesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod resources; err: %w", err)
	}
	return resp, nil
}

func newKubeletClient() (*grpc.ClientConn, func(), error) {
	if _, err := os.Stat(PodResourceSocket); err != nil {
		slog.Error("kubelet pod-resources socket unavailable", "socket", PodResourceSocket, "err", err)
		return nil, func() {}, fmt.Errorf("kubelet pod-resources socket unavailable, %w", err)
	}
	conn, err := grpc.NewClient("unix://"+PodResourceSocket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("failed to create kubelet client", "err", err)
		return nil, func() {}, fmt.Errorf("failed to create kubelet client, %w", err)
	}
	return conn, func() {
		_ = conn.Close()
	}, nil
}

func getDeviceName(pciAddress string) (string, error) {
	poolsFilePath := fmt.Sprintf(SysfsDriverPools, pciAddress)
	poolsFile, err := os.ReadFile(poolsFilePath)
	if err != nil {
		slog.Error("Failed to read", "file", poolsFilePath, "err", err)
		return "", fmt.Errorf("failed to read %s, %w", poolsFilePath, err)
	}

	deviceName := strings.Split(strings.Split(string(poolsFile), "\n")[1], " ")[0]
	return deviceName, nil
}

func IsKubernetes() bool {
	if s := os.Getenv("KUBERNETES_SERVICE_HOST"); s != "" {
		return true
	}
	if _, err := os.Stat(PodResourceSocket); err == nil {
		return true
	}
	return false
}
