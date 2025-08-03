package types

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

// NodeStatus represents the status of a node
type NodeStatus string

const (
	NodeStatusReady      NodeStatus = "Ready"
	NodeStatusNotReady   NodeStatus = "NotReady"
	NodeStatusProcessing NodeStatus = "Processing"
	NodeStatusRecovering NodeStatus = "Recovering"
)

// NodeTracker tracks the status and timing of nodes
type NodeTracker struct {
	NodeName     string
	Status       NodeStatus
	NotReadyTime time.Time
	LastSeen     time.Time
	TimerCancel  context.CancelFunc
	ProcessedVAs map[string]bool // Track processed VolumeAttachments
	mutex        sync.RWMutex
}

// NewNodeTracker creates a new NodeTracker
func NewNodeTracker(nodeName string) *NodeTracker {
	return &NodeTracker{
		NodeName:     nodeName,
		Status:       NodeStatusReady,
		ProcessedVAs: make(map[string]bool),
	}
}

// SetStatus sets the node status with proper locking
func (nt *NodeTracker) SetStatus(status NodeStatus) {
	nt.mutex.Lock()
	defer nt.mutex.Unlock()
	nt.Status = status
	nt.LastSeen = time.Now()
}

// GetStatus gets the node status with proper locking
func (nt *NodeTracker) GetStatus() NodeStatus {
	nt.mutex.RLock()
	defer nt.mutex.RUnlock()
	return nt.Status
}

// SetNotReadyTime sets the NotReady timestamp
func (nt *NodeTracker) SetNotReadyTime(t time.Time) {
	nt.mutex.Lock()
	defer nt.mutex.Unlock()
	nt.NotReadyTime = t
}

// GetNotReadyTime gets the NotReady timestamp
func (nt *NodeTracker) GetNotReadyTime() time.Time {
	nt.mutex.RLock()
	defer nt.mutex.RUnlock()
	return nt.NotReadyTime
}

// IsProcessed checks if a VolumeAttachment has been processed
func (nt *NodeTracker) IsProcessed(vaName string) bool {
	nt.mutex.RLock()
	defer nt.mutex.RUnlock()
	return nt.ProcessedVAs[vaName]
}

// MarkProcessed marks a VolumeAttachment as processed
func (nt *NodeTracker) MarkProcessed(vaName string) {
	nt.mutex.Lock()
	defer nt.mutex.Unlock()
	nt.ProcessedVAs[vaName] = true
}

// CancelTimer cancels the node timer if it exists
func (nt *NodeTracker) CancelTimer() {
	nt.mutex.Lock()
	defer nt.mutex.Unlock()
	if nt.TimerCancel != nil {
		nt.TimerCancel()
		nt.TimerCancel = nil
	}
}

// SetTimer sets a new timer with cancel function
func (nt *NodeTracker) SetTimer(cancel context.CancelFunc) {
	nt.mutex.Lock()
	defer nt.mutex.Unlock()
	nt.TimerCancel = cancel
}

// PodVolumeInfo contains information about a pod's volumes
type PodVolumeInfo struct {
	Pod                *corev1.Pod
	VolumeAttachments  []*storagev1.VolumeAttachment
	PersistentVolumes  []string
}

// CleanupTask represents a volume cleanup task
type CleanupTask struct {
	NodeName          string
	VolumeAttachments []*storagev1.VolumeAttachment
	Pods              []*corev1.Pod
	Timestamp         time.Time
}

// OperationResult represents the result of a cleanup operation
type OperationResult struct {
	Success     bool
	Error       error
	VAName      string
	NodeName    string
	PodName     string
	Namespace   string
	Timestamp   time.Time
}

// Metrics holds the metrics for the controller
type Metrics struct {
	NodesProcessed         int64
	VolumeAttachmentsDeleted int64
	OperationErrors        int64
	ProcessingDuration     time.Duration
}
