package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// activation is what the shim sends to a warm pod's wrapper once an actor claims it.
type activation struct {
	Command     []string          `json:"command"`
	Env         map[string]string `json:"env"`
	IdentityDir string            `json:"identityDir"`
	Files       map[string]string `json:"files"`
	DataDir     string            `json:"dataDir"`
	Restore     *snapshotRef      `json:"restore,omitempty"`
}

type warmEntry struct {
	template *ateapipb.ActorTemplate
	lastUsed time.Time
}

// warmPool keeps idle pods that already run on Fargate, so a new session only pays
// the container start instead of the microVM start. A pod serves one actor and is
// never reused. Pools are keyed by everything that shapes the pod except its env,
// so agents that share a runtime image share a pool.
type warmPool struct {
	size    int
	idleTTL time.Duration
	now     func() time.Time
	mu      sync.Mutex
	entries map[string]*warmEntry
	claimMu sync.Mutex
	images  sync.Map
}

func newWarmPool(size int, idleTTL time.Duration) *warmPool {
	return &warmPool{size: size, idleTTL: idleTTL, now: time.Now, entries: map[string]*warmEntry{}}
}

func specKey(template *ateapipb.ActorTemplate, settings podSettings) string {
	c := template.GetContainers()[0]
	h := sha256.New()
	marshal := proto.MarshalOptions{Deterministic: true}
	fmt.Fprint(h, template.GetMetadata().GetAtespace(), "|", c.GetImage(), "|", strings.Join(c.GetCommand(), "\x00"), "|", strings.Join(c.GetArgs(), "\x00"), "|")
	fmt.Fprint(h, settings.CPU, settings.Memory, settings.ServiceAccount, settings.RunAsRoot, settings.HelperImage, settings.Persistence, "|")
	for _, m := range []proto.Message{c.GetWakeupProbe(), c.GetSecurityContext()} {
		raw, _ := marshal.Marshal(m)
		h.Write(raw)
	}
	for _, v := range template.GetVolumes() {
		raw, _ := marshal.Marshal(v)
		h.Write(raw)
	}
	for _, m := range c.GetVolumeMounts() {
		raw, _ := marshal.Marshal(m)
		h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil))[:10]
}

func (w *warmPool) touch(template *ateapipb.ActorTemplate, settings podSettings) {
	if len(template.GetContainers()) != 1 {
		return
	}
	key := specKey(template, settings)
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.entries[key]; ok {
		e.lastUsed = w.now()
		return
	}
	w.entries[key] = &warmEntry{template: proto.Clone(template).(*ateapipb.ActorTemplate), lastUsed: w.now()}
}

func (w *warmPool) liveWarmPods(ctx context.Context, p *k8sPods, namespace, key string) ([]corev1.Pod, error) {
	list, err := p.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: warmKeyLabel + "=" + key})
	if err != nil {
		return nil, err
	}
	var live []corev1.Pod
	for _, pod := range list.Items {
		if pod.DeletionTimestamp == nil && (pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning) {
			live = append(live, pod)
		}
	}
	return live, nil
}

// maintain tops every pool up to its size and drops pools nobody has used for a while.
func (w *warmPool) maintain(ctx context.Context, p *k8sPods) {
	defer w.removeStale(ctx, p)
	w.mu.Lock()
	entries := make(map[string]*warmEntry, len(w.entries))
	for key, e := range w.entries {
		entries[key] = e
	}
	w.mu.Unlock()
	for key, e := range entries {
		namespace := e.template.GetMetadata().GetAtespace()
		live, err := w.liveWarmPods(ctx, p, namespace, key)
		if err != nil {
			slog.Warn("list warm pods", "key", key, "error", err)
			continue
		}
		if w.now().Sub(e.lastUsed) > w.idleTTL {
			for _, pod := range live {
				_ = p.Delete(ctx, namespace, pod.Name)
			}
			w.mu.Lock()
			delete(w.entries, key)
			w.mu.Unlock()
			continue
		}
		if len(live) >= w.size {
			continue
		}
		if err := p.ensureTrustBundle(ctx, namespace); err != nil {
			slog.Warn("trust bundle", "namespace", namespace, "error", err)
			continue
		}
		for i := len(live); i < w.size; i++ {
			pod, err := buildPod(podOptions{
				Name: fmt.Sprintf("warm-%s-%s", key, randSuffix()), Template: e.template, Settings: p.settings, WarmKey: key,
			})
			if err == nil {
				_, err = p.client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
			}
			if err != nil {
				slog.Warn("create warm pod", "key", key, "error", err)
				break
			}
		}
	}
}

// removeStale deletes warm pods whose pool no longer exists, for example after the helper
// image changed. The age check leaves pods alone while a restarted shim re-registers pools.
func (w *warmPool) removeStale(ctx context.Context, p *k8sPods) {
	infos, err := p.List(ctx)
	if err != nil {
		return
	}
	for _, info := range infos {
		if info.warmKey == "" || w.now().Sub(info.created) < 3*time.Minute {
			continue
		}
		w.mu.Lock()
		_, known := w.entries[info.warmKey]
		w.mu.Unlock()
		if !known {
			slog.Info("deleting stale warm pod", "pod", info.ref.String())
			_ = p.Delete(ctx, info.ref.atespace, info.ref.name)
		}
	}
}

func randSuffix() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 5)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

// claim hands an idle warm pod to the actor. It reports false when no pod is ready,
// and the caller then starts a cold pod.
func (w *warmPool) claim(ctx context.Context, p *k8sPods, req startRequest) (string, bool) {
	template := req.Template
	if len(template.GetContainers()) != 1 {
		return "", false
	}
	key := specKey(template, p.settings)
	w.touch(template, p.settings)
	namespace := req.Actor.GetMetadata().GetAtespace()
	w.claimMu.Lock()
	defer w.claimMu.Unlock()
	pods, err := w.liveWarmPods(ctx, p, namespace, key)
	if err != nil {
		return "", false
	}
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" || !wrapperWaiting(ctx, pod.Status.PodIP) {
			continue
		}
		act, err := w.activationFor(ctx, req)
		if err != nil {
			slog.Warn("build activation", "error", err)
			return "", false
		}
		patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{
			"labels": map[string]any{warmKeyLabel: nil},
			"annotations": map[string]string{
				actorNameAnno: req.Actor.GetMetadata().GetName(), actorAtespaceAnno: namespace, actorUIDAnno: req.Actor.GetMetadata().GetUid(),
			},
		}})
		if _, err := p.client.CoreV1().Pods(namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			continue
		}
		if err := postActivation(ctx, pod.Status.PodIP, act); err != nil {
			slog.Warn("activate warm pod", "pod", pod.Name, "error", err)
			_ = p.Delete(ctx, namespace, pod.Name)
			continue
		}
		slog.Info("claimed warm pod", "pod", pod.Name, "actor", req.Actor.GetMetadata().GetName())
		return pod.Name, true
	}
	return "", false
}

func (w *warmPool) activationFor(ctx context.Context, req startRequest) (activation, error) {
	c := req.Template.GetContainers()[0]
	command, err := w.resolveCommand(ctx, c)
	if err != nil {
		return activation{}, err
	}
	act := activation{Command: command, Env: map[string]string{}, Files: map[string]string{}, Restore: req.Snapshot}
	for _, e := range c.GetEnv() {
		act.Env[e.GetName()] = e.GetValue()
	}
	_, act.IdentityDir, _ = identityVolume(req.Template)
	_, act.DataDir = durableVolume(req.Template)
	_, _, fields := identityVolume(req.Template)
	meta := req.Actor.GetMetadata()
	for path, field := range fields {
		switch field {
		case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME:
			act.Files[path] = meta.GetName()
		case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE:
			act.Files[path] = meta.GetAtespace()
		case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID:
			act.Files[path] = meta.GetUid()
		}
	}
	return act, nil
}

// resolveCommand applies the Kubernetes rules: command replaces the image entrypoint,
// args replace the image cmd.
func (w *warmPool) resolveCommand(ctx context.Context, c *ateapipb.Container) ([]string, error) {
	entry, args := c.GetCommand(), c.GetArgs()
	if len(entry) == 0 {
		imageEntry, imageCmd, err := w.imageConfig(ctx, c.GetImage())
		if err != nil {
			return nil, err
		}
		entry = imageEntry
		if len(args) == 0 {
			args = imageCmd
		}
	}
	full := append(append([]string{}, entry...), args...)
	if len(full) == 0 {
		return nil, fmt.Errorf("image %s has no entrypoint and the template sets no command", c.GetImage())
	}
	return full, nil
}

func (w *warmPool) imageConfig(ctx context.Context, image string) (entry, cmd []string, err error) {
	if cached, ok := w.images.Load(image); ok {
		both := cached.([2][]string)
		return both[0], both[1], nil
	}
	ref, err := name.ParseReference(image)
	if err != nil {
		return nil, nil, err
	}
	img, err := remote.Image(ref, remote.WithContext(ctx))
	if err != nil {
		return nil, nil, fmt.Errorf("read image config: %w", err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, nil, err
	}
	w.images.Store(image, [2][]string{cfg.Config.Entrypoint, cfg.Config.Cmd})
	return cfg.Config.Entrypoint, cfg.Config.Cmd, nil
}

func wrapperWaiting(ctx context.Context, podIP string) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d/healthz", podIP, wrapperPort), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func postActivation(ctx context.Context, podIP string, act activation) error {
	raw, err := json.Marshal(act)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s:%d/activate", podIP, wrapperPort), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("activation rejected: %s", resp.Status)
	}
	return nil
}
