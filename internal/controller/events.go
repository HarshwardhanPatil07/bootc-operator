// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	bootcv1alpha1 "github.com/bootc-dev/bootc-operator/api/v1alpha1"
)

const (
	eventReasonImageUpdateAvailable = "ImageUpdateAvailable"
	eventReasonRolloutStarted       = "RolloutStarted"
	eventReasonRolloutCompleted     = "RolloutCompleted"

	eventActionResolveImage = "ResolveImage"
	eventActionRollout      = "Rollout"
	eventActionPoolDegraded = "PoolDegraded"

	eventNoteLimit  = 1024
	eventNoteSuffix = "..."
)

// EventNote renders the human-readable message of a Kubernetes Event. Each
// implementation owns a small set of well-defined fields and is responsible for
// producing a message that fits the events.k8s.io/v1 1 KiB note limit. Notes
// assembled entirely from bounded fields (image references, digests) are safe by
// construction; notes that embed unbounded free text (condition messages, error
// strings) cap themselves with capNote.
type EventNote interface {
	Note() string
}

// shortDigest abbreviates a "sha256:<hex>" digest to a human-friendly prefix
// ("sha256:" plus 12 hex characters), which is enough to disambiguate images in
// event messages while keeping them concise. Values that are already short are
// returned unchanged.
func shortDigest(digest string) string {
	const shortLen = len("sha256:") + 12
	if len(digest) > shortLen {
		return digest[:shortLen]
	}
	return digest
}

type poolImageUpdateNote struct {
	ImageRef       string
	NewDigest      string
	PreviousDigest string
}

func (n poolImageUpdateNote) Note() string {
	return fmt.Sprintf(
		"Image tag %s resolved to new digest %s (previously %s)",
		n.ImageRef,
		shortDigest(n.NewDigest),
		shortDigest(n.PreviousDigest),
	)
}

type poolRolloutStartedNote struct {
	TargetDigest string
}

func (n poolRolloutStartedNote) Note() string {
	return fmt.Sprintf("Rollout started toward digest %s", shortDigest(n.TargetDigest))
}

type poolRolloutCompletedNote struct {
	TargetDigest string
}

func (n poolRolloutCompletedNote) Note() string {
	return fmt.Sprintf("Rollout completed at digest %s", shortDigest(n.TargetDigest))
}

// poolDegradedNote wraps a pool's Degraded condition message, which is not
// length-bounded, so it caps itself.
type poolDegradedNote struct {
	Message string
}

func (n poolDegradedNote) Note() string {
	return capNote(n.Message)
}

func (r *BootcNodePoolReconciler) recordPoolEvents(
	pool *bootcv1alpha1.BootcNodePool,
	previous *bootcv1alpha1.BootcNodePoolStatus,
	tagTargetChanged bool,
) {
	if tagTargetChanged {
		r.recordEvent(
			pool,
			nil,
			corev1.EventTypeNormal,
			eventReasonImageUpdateAvailable,
			eventActionResolveImage,
			poolImageUpdateNote{
				ImageRef:       pool.Spec.Image.Ref,
				NewDigest:      pool.Status.TargetDigest,
				PreviousDigest: previous.TargetDigest,
			},
		)
	}

	oldUpToDate := apimeta.FindStatusCondition(previous.Conditions, bootcv1alpha1.PoolUpToDate)
	newUpToDate := apimeta.FindStatusCondition(pool.Status.Conditions, bootcv1alpha1.PoolUpToDate)
	targetChanged := previous.TargetDigest != "" &&
		previous.TargetDigest != pool.Status.TargetDigest &&
		pool.Status.TargetDigest != ""
	poolRolloutInProgress := conditionEnteredReason(
		oldUpToDate,
		newUpToDate,
		metav1.ConditionFalse,
		bootcv1alpha1.PoolRolloutInProgress,
	) || (targetChanged && newUpToDate != nil &&
		newUpToDate.Status == metav1.ConditionFalse &&
		newUpToDate.Reason == bootcv1alpha1.PoolRolloutInProgress)
	if poolRolloutInProgress {
		r.recordEvent(
			pool,
			nil,
			corev1.EventTypeNormal,
			eventReasonRolloutStarted,
			eventActionRollout,
			poolRolloutStartedNote{TargetDigest: pool.Status.TargetDigest},
		)
	}

	rolloutCompleted := oldUpToDate != nil &&
		oldUpToDate.Status != metav1.ConditionTrue &&
		newUpToDate != nil &&
		newUpToDate.Status == metav1.ConditionTrue
	if rolloutCompleted {
		r.recordEvent(
			pool,
			nil,
			corev1.EventTypeNormal,
			eventReasonRolloutCompleted,
			eventActionRollout,
			poolRolloutCompletedNote{TargetDigest: pool.Status.TargetDigest},
		)
	}

	oldDegraded := apimeta.FindStatusCondition(previous.Conditions, bootcv1alpha1.PoolDegraded)
	newDegraded := apimeta.FindStatusCondition(pool.Status.Conditions, bootcv1alpha1.PoolDegraded)
	if degradedConditionChanged(oldDegraded, newDegraded) {
		r.recordEvent(
			pool,
			nil,
			corev1.EventTypeWarning,
			newDegraded.Reason,
			eventActionPoolDegraded,
			poolDegradedNote{Message: newDegraded.Message},
		)
	}
}

func conditionEnteredReason(
	previous, current *metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
) bool {
	if current == nil || current.Status != status || current.Reason != reason {
		return false
	}
	return previous == nil || previous.Status != status || previous.Reason != reason
}

func degradedConditionChanged(previous, current *metav1.Condition) bool {
	if current == nil || current.Status != metav1.ConditionTrue {
		return false
	}
	return previous == nil ||
		previous.Status != metav1.ConditionTrue ||
		previous.Reason != current.Reason ||
		previous.Message != current.Message
}

func (r *BootcNodePoolReconciler) recordEvent(
	regarding, related runtime.Object,
	eventType, reason, action string,
	note EventNote,
) {
	r.Recorder.Eventf(regarding, related, eventType, reason, action, "%s", note.Note())
}

// capNote keeps a note within the events.k8s.io/v1 1 KiB limit and never splits
// a UTF-8 sequence. It is only needed for notes built from unbounded free text
// (condition messages, error strings); notes assembled from bounded fields fit
// by construction.
func capNote(note string) string {
	note = strings.ToValidUTF8(note, "�")
	if len(note) <= eventNoteLimit {
		return note
	}

	limit := eventNoteLimit - len(eventNoteSuffix)
	for limit > 0 && !utf8.RuneStart(note[limit]) {
		limit--
	}
	return note[:limit] + eventNoteSuffix
}
