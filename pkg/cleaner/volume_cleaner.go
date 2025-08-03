package cleaner

import (
	"context"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/tiggoins/node-volume-cleaner/pkg/config"
	"github.com/tiggoins/node-volume-cleaner/pkg/types"
)

// VolumeCleaner handles the cleanup of volume attachments
type VolumeCleaner struct {
	client kubernetes.Interface
	config *config.Config
}

// NewVolumeCleaner creates a new VolumeCleaner
func NewVolumeCleaner(client kubernetes.Interface, cfg *config.Config) *VolumeCleaner {
	return &VolumeCleaner{
		client: client,
		config: cfg,
	}
}

// CleanupNodeVolumes cleans up volumes for a failed node
func (vc *VolumeCleaner) CleanupNodeVolumes(ctx context.Context, nodeName string) (*types.CleanupTask, error) {
	klog.Infof("Starting volume cleanup for node %s", nodeName)

	// Double-check node status before cleanup
	if ready, err := vc.isNodeStillNotReady(ctx, nodeName); err != nil {
		return nil, fmt.Errorf("failed to check node status: %w", err)
	} else if ready {
		klog.Infof("Node %s has recovered, skipping cleanup", nodeName)
		return nil, nil
	}

	// Get pods on the node
	pods, err := vc.getPodsOnNode(ctx, nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pods on node %s: %w", nodeName, err)
	}

	klog.Infof("Found %d pods on node %s", len(pods), nodeName)

	// Filter pods that need volume cleanup
	podsWithVolumes := vc.filterPodsWithVolumes(pods)
	klog.Infof("Found %d pods with volumes on node %s", len(podsWithVolumes), nodeName)

	// Get volume attachments for these pods
	volumeAttachments, err := vc.getVolumeAttachmentsForPods(ctx, podsWithVolumes, nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume attachments: %w", err)
	}

	klog.Infof("Found %d volume attachments to clean up for node %s", len(volumeAttachments), nodeName)

	// Create cleanup task
	task := &types.CleanupTask{
		NodeName:          nodeName,
		VolumeAttachments: volumeAttachments,
		Pods:              podsWithVolumes,
		Timestamp:         time.Now(),
	}

	// Perform cleanup
	results := vc.performCleanup(ctx, task)

	// Log results
	vc.logCleanupResults(results)

	return task, nil
}

// isNodeStillNotReady double-checks if the node is still not ready
func (vc *VolumeCleaner) isNodeStillNotReady(ctx context.Context, nodeName string) (bool, error) {
	node, err := vc.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status != corev1.ConditionTrue, nil
		}
	}

	return true, nil // Assume not ready if no Ready condition found
}

// getPodsOnNode gets all pods running on a specific node
func (vc *VolumeCleaner) getPodsOnNode(ctx context.Context, nodeName string) ([]*corev1.Pod, error) {
	podList, err := vc.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return nil, err
	}

	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pods = append(pods, &podList.Items[i])
	}

	return pods, nil
}

// filterPodsWithVolumes filters pods that have persistent volume claims
func (vc *VolumeCleaner) filterPodsWithVolumes(pods []*corev1.Pod) []*corev1.Pod {
	filtered := make([]*corev1.Pod, 0)

	for _, pod := range pods {
		// Skip pods in excluded namespaces
		if vc.isNamespaceExcluded(pod.Namespace) {
			continue
		}

		// Skip DaemonSet pods (they usually use local storage)
		if vc.isDaemonSetPod(pod) {
			continue
		}

		// Check if pod has PVC volumes
		if vc.hasPVCVolumes(pod) {
			filtered = append(filtered, pod)
		}
	}

	return filtered
}

// isNamespaceExcluded checks if a namespace should be excluded
func (vc *VolumeCleaner) isNamespaceExcluded(namespace string) bool {
	return slices.Contains(vc.config.ExcludeNamespaces, namespace)
}

// isDaemonSetPod checks if a pod belongs to a DaemonSet
func (vc *VolumeCleaner) isDaemonSetPod(pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// hasPVCVolumes checks if a pod has persistent volume claim volumes
func (vc *VolumeCleaner) hasPVCVolumes(pod *corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			return true
		}
	}
	return false
}

// getVolumeAttachmentsForPods gets volume attachments for pods
func (vc *VolumeCleaner) getVolumeAttachmentsForPods(ctx context.Context, pods []*corev1.Pod, nodeName string) ([]*storagev1.VolumeAttachment, error) {
	// Get all volume attachments for the node
	vaList, err := vc.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// Create a map of PV names used by pods
	pvNames := make(map[string]bool)
	for _, pod := range pods {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				// Get PVC to find PV name，PVC -> PV
				pvc, err := vc.client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, volume.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
				if err != nil {
					klog.Warningf("Failed to get PVC %s/%s: %v", pod.Namespace, volume.PersistentVolumeClaim.ClaimName, err)
					continue
				}
				if pvc.Spec.VolumeName != "" {
					pvNames[pvc.Spec.VolumeName] = true
				}
			}
		}
	}

	// Filter volume attachments for this node and these PVs
	attachments := make([]*storagev1.VolumeAttachment, 0)
	for i := range vaList.Items {
		va := &vaList.Items[i]

		// Check if attached to the node
		if va.Spec.NodeName != nodeName {
			continue
		}

		// Check if the volume is used by our pods
		volumeHandle := vc.getVolumeHandle(va)
		if volumeHandle == "" {
			continue
		}

		// For RBD volumes, the volume handle might be different from PV name
		// We need to match by volume handle or PV name
		if vc.isVolumeUsedByPods(ctx, va, pvNames) {
			attachments = append(attachments, va)
		}
	}

	return attachments, nil
}

// getVolumeHandle extracts volume handle from VolumeAttachment
func (vc *VolumeCleaner) getVolumeHandle(va *storagev1.VolumeAttachment) string {
	if va.Spec.Source.PersistentVolumeName != nil {
		return *va.Spec.Source.PersistentVolumeName
	}
	return ""
}

// isVolumeUsedByPods checks if a volume attachment is used by our pods
func (vc *VolumeCleaner) isVolumeUsedByPods(ctx context.Context, va *storagev1.VolumeAttachment, pvNames map[string]bool) bool {
	volumeHandle := vc.getVolumeHandle(va)
	if volumeHandle == "" {
		return false
	}

	// Direct PV name match
	if pvNames[volumeHandle] {
		return true
	}

	// For CSI volumes, we might need to check the PV spec
	if va.Spec.Source.PersistentVolumeName != nil {
		pv, err := vc.client.CoreV1().PersistentVolumes().Get(ctx, *va.Spec.Source.PersistentVolumeName, metav1.GetOptions{})
		if err != nil {
			return false
		}

		// Check if this PV is in our list
		return pvNames[pv.Name]
	}

	return false
}

// performCleanup performs the actual cleanup of volume attachments
func (vc *VolumeCleaner) performCleanup(ctx context.Context, task *types.CleanupTask) []*types.OperationResult {
	results := make([]*types.OperationResult, 0, len(task.VolumeAttachments))

	for _, va := range task.VolumeAttachments {
		result := &types.OperationResult{
			VAName:    va.Name,
			NodeName:  task.NodeName,
			Timestamp: time.Now(),
		}

		if vc.config.DryRun {
			klog.Infof("DRY RUN: Would delete VolumeAttachment %s", va.Name)
			result.Success = true
		} else {
			err := vc.deleteVolumeAttachment(ctx, va.Name)
			if err != nil {
				klog.Errorf("Failed to delete VolumeAttachment %s: %v", va.Name, err)
				result.Error = err
				result.Success = false
			} else {
				klog.Infof("Successfully deleted VolumeAttachment %s", va.Name)
				result.Success = true
			}
		}

		results = append(results, result)
	}

	return results
}

// deleteVolumeAttachment deletes a volume attachment with retries
func (vc *VolumeCleaner) deleteVolumeAttachment(ctx context.Context, vaName string) error {
	var lastErr error

	// force delete ? 
	for i := 0; i < vc.config.MaxRetries; i++ {
		err := vc.client.StorageV1().VolumeAttachments().Delete(ctx, vaName, metav1.DeleteOptions{})
		if err == nil {
			return nil
		}

		lastErr = err
		klog.Warningf("Attempt %d/%d to delete VolumeAttachment %s failed: %v", i+1, vc.config.MaxRetries, vaName, err)

		if i < vc.config.MaxRetries-1 {
			time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
		}
	}

	return fmt.Errorf("failed to delete VolumeAttachment after %d attempts: %w", vc.config.MaxRetries, lastErr)
}

// logCleanupResults logs the results of cleanup operations
func (vc *VolumeCleaner) logCleanupResults(results []*types.OperationResult) {
	successCount := 0
	errorCount := 0

	for _, result := range results {
		if result.Success {
			successCount++
		} else {
			errorCount++
			klog.Errorf("Cleanup failed for VA %s on node %s: %v", result.VAName, result.NodeName, result.Error)
		}
	}

	klog.Infof("Cleanup completed: %d successful, %d failed", successCount, errorCount)
}
