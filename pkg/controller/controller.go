package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/tiggoins/node-volume-cleaner/pkg/cleaner"
	"github.com/tiggoins/node-volume-cleaner/pkg/config"
	"github.com/tiggoins/node-volume-cleaner/pkg/tracker"
	"github.com/tiggoins/node-volume-cleaner/pkg/types"
)

// NodeVolumeController is the main controller
type NodeVolumeController struct {
	client         kubernetes.Interface
	config         *config.Config
	nodeTracker    *tracker.NodeTrackerManager
	volumeCleaner  *cleaner.VolumeCleaner
	
	// Informers
	nodeInformer cache.SharedIndexInformer
	
	// Work queue
	workqueue workqueue.RateLimitingInterface
	
	// Metrics
	metrics *types.Metrics
}

// NewNodeVolumeController creates a new controller
func NewNodeVolumeController(client kubernetes.Interface, cfg *config.Config) (*NodeVolumeController, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	
	controller := &NodeVolumeController{
		client:        client,
		config:        cfg,
		nodeTracker:   tracker.NewNodeTrackerManager(),
		volumeCleaner: cleaner.NewVolumeCleaner(client, cfg),
		workqueue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[any](), "node-volume-cleaner"),
		metrics:       &types.Metrics{},
	}
	
	// Set up informers
	if err := controller.setupInformers(); err != nil {
		return nil, fmt.Errorf("failed to setup informers: %w", err)
	}
	
	// Set up callbacks
	controller.nodeTracker.SetCallbacks(
		controller.onNodeNotReady,
		controller.onNodeReady,
	)
	
	return controller, nil
}

// Run starts the controller
func (c *NodeVolumeController) Run(ctx context.Context) error {
	klog.Info("Starting Node Volume Controller")
	
	// Start the informer
	go c.nodeInformer.Run(ctx.Done())
	
	// Wait for caches to sync
	klog.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), c.nodeInformer.HasSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	klog.Info("Informer caches synced")
	
	// Start workers
	klog.Info("Starting workers")
	go c.runWorker(ctx)
	
	// Start metrics server if enabled
	if c.config.EnableMetrics {
		go c.startMetricsServer(ctx)
	}
	
	klog.Info("Node Volume Controller started")
	
	// Wait for context cancellation
	<-ctx.Done()
	
	// Cleanup
	klog.Info("Shutting down Node Volume Controller")
	c.workqueue.ShutDown()
	c.nodeTracker.Cleanup()
	
	return nil
}

// setupInformers sets up the Kubernetes informers
func (c *NodeVolumeController) setupInformers() error {
	// Create informer factory
	informerFactory := informers.NewSharedInformerFactory(c.client, time.Minute*10)
	
	// Node informer
	c.nodeInformer = informerFactory.Core().V1().Nodes().Informer()
	
	// Add event handlers
	c.nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := obj.(*corev1.Node)
			klog.V(4).Infof("Node added: %s", node.Name)
			c.handleNodeUpdate(node)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			node := newObj.(*corev1.Node)
			klog.V(4).Infof("Node updated: %s", node.Name)
			c.handleNodeUpdate(node)
		},
		DeleteFunc: func(obj interface{}) {
			node := obj.(*corev1.Node)
			klog.Infof("Node deleted: %s", node.Name)
			c.nodeTracker.RemoveTracker(node.Name)
		},
	})
	
	return nil
}

// handleNodeUpdate handles node update events
func (c *NodeVolumeController) handleNodeUpdate(node *corev1.Node) {
	// Skip nodes with exclude labels
	if c.shouldExcludeNode(node) {
		klog.V(4).Infof("Skipping node %s due to exclude labels", node.Name)
		return
	}
	
	// Update node tracker
	c.nodeTracker.UpdateNode(node, c.config.WaitTimeout)
}

// shouldExcludeNode checks if a node should be excluded from processing
func (c *NodeVolumeController) shouldExcludeNode(node *corev1.Node) bool {
	for labelKey, labelValue := range c.config.ExcludeNodeLabels {
		if nodeValue, exists := node.Labels[labelKey]; exists && nodeValue == labelValue {
			return true
		}
	}
	return false
}

// onNodeNotReady is called when a node has been NotReady for the configured timeout
func (c *NodeVolumeController) onNodeNotReady(nodeName string, duration time.Duration) {
	klog.Infof("Node %s has been NotReady for %v, queuing for cleanup", nodeName, duration)
	
	// Add to work queue
	c.workqueue.Add(nodeName)
	
	// Update metrics
	c.metrics.NodesProcessed++
}

// onNodeReady is called when a node becomes ready again
func (c *NodeVolumeController) onNodeReady(nodeName string) {
	klog.Infof("Node %s became ready again", nodeName)
	
	// Remove from work queue if present
	// Note: workqueue doesn't have a direct remove method, but it will handle duplicates
}

// runWorker runs the work queue worker
func (c *NodeVolumeController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem processes the next item in the work queue
func (c *NodeVolumeController) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}
	
	defer c.workqueue.Done(obj)
	
	nodeName, ok := obj.(string)
	if !ok {
		klog.Errorf("Expected string in workqueue but got %#v", obj)
		c.workqueue.Forget(obj)
		return true
	}
	
	// Process the node
	if err := c.processNode(ctx, nodeName); err != nil {
		klog.Errorf("Error processing node %s: %v", nodeName, err)
		
		// Rate limit retries
		if c.workqueue.NumRequeues(obj) < c.config.MaxRetries {
			klog.Infof("Retrying node %s (%d/%d)", nodeName, c.workqueue.NumRequeues(obj)+1, c.config.MaxRetries)
			c.workqueue.AddRateLimited(obj)
		} else {
			klog.Errorf("Giving up on node %s after %d retries", nodeName, c.config.MaxRetries)
			c.workqueue.Forget(obj)
			c.metrics.OperationErrors++
		}
		return true
	}
	
	// Success
	c.workqueue.Forget(obj)
	return true
}

// processNode processes a node for volume cleanup
func (c *NodeVolumeController) processNode(ctx context.Context, nodeName string) error {
	startTime := time.Now()
	defer func() {
		c.metrics.ProcessingDuration = time.Since(startTime)
	}()
	
	klog.Infof("Processing node %s for volume cleanup", nodeName)
	
	// Get node tracker
	tracker, exists := c.nodeTracker.GetTracker(nodeName)
	if !exists {
		return fmt.Errorf("node tracker not found for %s", nodeName)
	}
	
	// Check if node is still in processing state
	if tracker.GetStatus() != types.NodeStatusProcessing {
		klog.Infof("Node %s is no longer in processing state, skipping", nodeName)
		return nil
	}
	
	// Perform volume cleanup
	task, err := c.volumeCleaner.CleanupNodeVolumes(ctx, nodeName)
	if err != nil {
		return fmt.Errorf("failed to cleanup volumes for node %s: %w", nodeName, err)
	}
	
	if task != nil {
		klog.Infof("Completed volume cleanup for node %s: %d volume attachments processed", 
			nodeName, len(task.VolumeAttachments))
		c.metrics.VolumeAttachmentsDeleted += int64(len(task.VolumeAttachments))
	}
	
	return nil
}

// startMetricsServer starts the metrics server
func (c *NodeVolumeController) startMetricsServer(ctx context.Context) {
	// This would typically set up a Prometheus metrics server
	// For now, we'll just log metrics periodically
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			c.logMetrics()
		case <-ctx.Done():
			return
		}
	}
}

// logMetrics logs current metrics
func (c *NodeVolumeController) logMetrics() {
	stats := c.nodeTracker.GetNodeStats()
	
	klog.Infof("Metrics - Nodes processed: %d, VolumeAttachments deleted: %d, Errors: %d", 
		c.metrics.NodesProcessed, c.metrics.VolumeAttachmentsDeleted, c.metrics.OperationErrors)
	
	klog.Infof("Node stats - Ready: %d, NotReady: %d, Processing: %d, Recovering: %d",
		stats[types.NodeStatusReady], stats[types.NodeStatusNotReady], 
		stats[types.NodeStatusProcessing], stats[types.NodeStatusRecovering])
}
