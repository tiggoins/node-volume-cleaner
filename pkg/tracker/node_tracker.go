package tracker

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/tiggoins/node-volume-cleaner/pkg/types"
)

// NodeTrackerManager manages all node trackers
type NodeTrackerManager struct {
	trackers map[string]*types.NodeTracker
	mutex    sync.RWMutex
	
	// Callbacks
	onNodeNotReady func(string, time.Duration)
	onNodeReady    func(string)
}

// NewNodeTrackerManager creates a new NodeTrackerManager
func NewNodeTrackerManager() *NodeTrackerManager {
	return &NodeTrackerManager{
		trackers: make(map[string]*types.NodeTracker),
	}
}

// SetCallbacks sets the callback functions
func (ntm *NodeTrackerManager) SetCallbacks(
	onNotReady func(string, time.Duration),
	onReady func(string),
) {
	ntm.onNodeNotReady = onNotReady
	ntm.onNodeReady = onReady
}

// UpdateNode updates the node status and handles state transitions
func (ntm *NodeTrackerManager) UpdateNode(node *corev1.Node, waitTimeout time.Duration) {
	nodeName := node.Name
	isReady := ntm.isNodeReady(node)
	
	ntm.mutex.Lock()
	tracker, exists := ntm.trackers[nodeName]
	if !exists {
		tracker = types.NewNodeTracker(nodeName)
		ntm.trackers[nodeName] = tracker
	}
	ntm.mutex.Unlock()
	
	currentStatus := tracker.GetStatus()
	
	if isReady {
		// Node is ready
		if currentStatus == types.NodeStatusNotReady || currentStatus == types.NodeStatusProcessing {
			klog.Infof("Node %s recovered from NotReady state", nodeName)
			tracker.CancelTimer()
			tracker.SetStatus(types.NodeStatusReady)
			
			if ntm.onNodeReady != nil {
				ntm.onNodeReady(nodeName)
			}
		}
	} else {
		// Node is not ready
		if currentStatus == types.NodeStatusReady {
			klog.Infof("Node %s became NotReady, starting timer", nodeName)
			notReadyTime := time.Now()
			tracker.SetNotReadyTime(notReadyTime)
			tracker.SetStatus(types.NodeStatusNotReady)
			
			// Start timer
			ctx, cancel := context.WithCancel(context.Background())
			tracker.SetTimer(cancel)
			
			go ntm.startNodeTimer(ctx, nodeName, waitTimeout)
		}
	}
}

// startNodeTimer starts a timer for the node and triggers cleanup after timeout
func (ntm *NodeTrackerManager) startNodeTimer(ctx context.Context, nodeName string, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	select {
	case <-timer.C:
		// Timer expired, check if node is still NotReady
		ntm.mutex.RLock()
		tracker, exists := ntm.trackers[nodeName]
		ntm.mutex.RUnlock()
		
		if !exists {
			return
		}
		
		if tracker.GetStatus() == types.NodeStatusNotReady {
			klog.Infof("Node %s has been NotReady for %v, triggering cleanup", nodeName, timeout)
			tracker.SetStatus(types.NodeStatusProcessing)
			
			if ntm.onNodeNotReady != nil {
				ntm.onNodeNotReady(nodeName, timeout)
			}
		}
		
	case <-ctx.Done():
		// Timer cancelled, node recovered
		klog.Infof("Timer cancelled for node %s", nodeName)
		return
	}
}

// GetTracker returns the tracker for a specific node
func (ntm *NodeTrackerManager) GetTracker(nodeName string) (*types.NodeTracker, bool) {
	ntm.mutex.RLock()
	defer ntm.mutex.RUnlock()
	tracker, exists := ntm.trackers[nodeName]
	return tracker, exists
}

// RemoveTracker removes a tracker for a node
func (ntm *NodeTrackerManager) RemoveTracker(nodeName string) {
	ntm.mutex.Lock()
	defer ntm.mutex.Unlock()
	
	if tracker, exists := ntm.trackers[nodeName]; exists {
		tracker.CancelTimer()
		delete(ntm.trackers, nodeName)
		klog.Infof("Removed tracker for node %s", nodeName)
	}
}

// GetAllTrackers returns all current trackers
func (ntm *NodeTrackerManager) GetAllTrackers() map[string]*types.NodeTracker {
	ntm.mutex.RLock()
	defer ntm.mutex.RUnlock()
	
	result := make(map[string]*types.NodeTracker)
	for name, tracker := range ntm.trackers {
		result[name] = tracker
	}
	return result
}

// Cleanup cancels all timers and clears trackers
func (ntm *NodeTrackerManager) Cleanup() {
	ntm.mutex.Lock()
	defer ntm.mutex.Unlock()
	
	for nodeName, tracker := range ntm.trackers {
		tracker.CancelTimer()
		klog.Infof("Cleaned up tracker for node %s", nodeName)
	}
	
	ntm.trackers = make(map[string]*types.NodeTracker)
}

// isNodeReady checks if a node is in Ready condition
func (ntm *NodeTrackerManager) isNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// GetNodeStats returns statistics about tracked nodes
func (ntm *NodeTrackerManager) GetNodeStats() map[types.NodeStatus]int {
	ntm.mutex.RLock()
	defer ntm.mutex.RUnlock()
	
	stats := make(map[types.NodeStatus]int)
	for _, tracker := range ntm.trackers {
		status := tracker.GetStatus()
		stats[status]++
	}
	return stats
}
