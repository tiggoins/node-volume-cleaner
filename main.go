package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/tiggoins/node-volume-cleaner/pkg/config"
	"github.com/tiggoins/node-volume-cleaner/pkg/controller"
)

var (
	kubeconfig = flag.String("kubeconfig", "", "Path to kubeconfig file")
	configFile = flag.String("config", "", "Path to config file")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		klog.Fatalf("Failed to load config: %v", err)
	}

	// Create Kubernetes client
	client, err := createKubernetesClient(*kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to create kubernetes client: %v", err)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start controller
	ctrl, err := controller.NewNodeVolumeController(client, cfg)
	if err != nil {
		klog.Fatalf("Failed to create controller: %v", err)
	}

	// Start controller
	go func() {
		if err := ctrl.Run(ctx); err != nil {
			klog.Errorf("Controller error: %v", err)
			cancel()
		}
	}()

	// Wait for signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		klog.Info("Received shutdown signal")
	case <-ctx.Done():
		klog.Info("Context cancelled")
	}

	// Graceful shutdown
	klog.Info("Shutting down...")
	cancel()
	time.Sleep(2 * time.Second) // Wait for cleanup
	klog.Info("Shutdown complete")
}

func createKubernetesClient(kubeconfig string) (kubernetes.Interface, error) {
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create config: %w", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return client, nil
}
