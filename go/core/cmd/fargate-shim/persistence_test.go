package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var testSecret = []byte("test-secret")

func newStore(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	srv := httptest.NewServer(storeHandler(dir, testSecret))
	t.Cleanup(srv.Close)
	return srv, dir
}

func doReq(t *testing.T, method, url, bearer, admin string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if admin != "" {
		req.Header.Set("X-Ate-Admin", admin)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestStoreKeepsEveryActorsDataApart(t *testing.T) {
	srv, _ := newStore(t)
	a, b := uuid.NewString(), uuid.NewString()
	urlA, urlB := srv.URL+"/v1/data/"+a, srv.URL+"/v1/data/"+b
	if resp := doReq(t, http.MethodPut, urlA, tokenFor(testSecret, a), "", strings.NewReader("data of a")); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("own put: %s", resp.Status)
	}
	for name, tc := range map[string]struct{ method, url, bearer string }{
		"a token cannot read b":  {http.MethodGet, urlB, tokenFor(testSecret, a)},
		"a token cannot write b": {http.MethodPut, urlB, tokenFor(testSecret, a)},
		"no token":               {http.MethodGet, urlA, ""},
		"a wrong token":          {http.MethodGet, urlA, "nope"},
		"an actor cannot delete": {http.MethodDelete, urlA, tokenFor(testSecret, a)},
	} {
		if resp := doReq(t, tc.method, tc.url, tc.bearer, "", strings.NewReader("x")); resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s: %s", name, resp.Status)
		}
	}
	if resp := doReq(t, http.MethodGet, urlB, tokenFor(testSecret, b), "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("b has no data yet, got %s", resp.Status)
	}
	resp := doReq(t, http.MethodGet, urlA, tokenFor(testSecret, a), "", nil)
	if got, _ := io.ReadAll(resp.Body); string(got) != "data of a" {
		t.Fatalf("a reads %q", got)
	}
	if resp := doReq(t, http.MethodGet, srv.URL+"/v1/data/..%2Fsecret", tokenFor(testSecret, a), "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("a non-uuid key: %s", resp.Status)
	}
	if resp := doReq(t, http.MethodDelete, urlA, "", tokenFor(testSecret, "admin"), nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("admin delete: %s", resp.Status)
	}
	if resp := doReq(t, http.MethodGet, urlA, tokenFor(testSecret, a), "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted data: %s", resp.Status)
	}
}

func TestDataRoundTripAndUnsafeArchives(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sessions.db"), []byte("conversation"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "file.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tarDir(&buf, src); err != nil {
		t.Fatal(err)
	}
	if err := untar(&buf, dst); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"sessions.db": "conversation", "sub/file.txt": "nested"} {
		if got, err := os.ReadFile(filepath.Join(dst, path)); err != nil || string(got) != want {
			t.Errorf("%s: %q %v", path, got, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(dst, "link")); err == nil {
		t.Error("symlinks are not saved")
	}
	for _, name := range []string{"../escape", "/abs/path"} {
		var evil bytes.Buffer
		gz := gzip.NewWriter(&evil)
		tw := tar.NewWriter(gz)
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: 1, Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte("x"))
		tw.Close()
		gz.Close()
		if err := untar(&evil, t.TempDir()); err == nil {
			t.Errorf("%q must be rejected", name)
		}
	}
}

func TestRestoreAfterSaveThroughTheStore(t *testing.T) {
	srv, _ := newStore(t)
	uid := uuid.NewString()
	ref := snapshotRef{URL: srv.URL + "/v1/data/" + uid, Token: tokenFor(testSecret, uid)}
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "sessions.db"), []byte("turn 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := saveData(context.Background(), ref, src); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := restoreData(context.Background(), ref, dst); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "sessions.db")); string(got) != "turn 1" {
		t.Fatalf("restored %q", got)
	}
	other := uuid.NewString()
	empty := t.TempDir()
	if err := restoreData(context.Background(), snapshotRef{URL: srv.URL + "/v1/data/" + other, Token: tokenFor(testSecret, other)}, empty); err != nil {
		t.Fatalf("a first start has no snapshot and must not fail: %v", err)
	}
	if entries, _ := os.ReadDir(empty); len(entries) != 0 {
		t.Fatal("another actor must not see this data")
	}
}

func newPersistentServer(t *testing.T, pods podManager, storeURL string) *server {
	t.Helper()
	s := newTestServer(t, pods)
	s.cfg.Snapshots = &snapshots{baseURL: storeURL, secret: testSecret}
	return s
}

func TestSuspendSavesDataFirst(t *testing.T) {
	pods := newFakePods(true)
	s := newPersistentServer(t, pods, "http://store")
	var saved []snapshotRef
	var failing bool
	s.save = func(_ context.Context, ip string, ref snapshotRef) error {
		if failing {
			return errors.New("store is down")
		}
		if ip != "10.0.0.7" {
			t.Errorf("saves through the pod's sidecar, got %s", ip)
		}
		saved = append(saved, ref)
		return nil
	}
	ctx := context.Background()
	actor := createActor(t, s)
	obj := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	if _, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: obj}); err != nil {
		t.Fatal(err)
	}
	failing = true
	if _, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: obj}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a failed save keeps the actor running so kagent retries: %v", err)
	}
	if view, _ := s.GetActor(ctx, &ateapipb.GetActorRequest{Actor: obj}); view.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("state after a failed save: %s", view.GetStatus().GetState())
	}
	failing = false
	if _, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: obj}); err != nil {
		t.Fatal(err)
	}
	uid := actor.GetMetadata().GetUid()
	if len(saved) != 1 || saved[0].URL != "http://store/v1/data/"+uid || saved[0].Token != tokenFor(testSecret, uid) {
		t.Fatalf("snapshots are keyed by the actor uid, not the name: %+v", saved)
	}
}

func TestStartPassesTheActorsOwnSnapshot(t *testing.T) {
	pods := newFakePods(true)
	s := newPersistentServer(t, pods, "http://store")
	actor := createActor(t, s)
	if _, err := s.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}}); err != nil {
		t.Fatal(err)
	}
	if len(pods.started) != 1 || pods.started[0].Snapshot == nil || !strings.HasSuffix(pods.started[0].Snapshot.URL, actor.GetMetadata().GetUid()) {
		t.Fatalf("start request: %+v", pods.started)
	}
}

func TestDeleteActorDeletesItsSnapshot(t *testing.T) {
	srv, _ := newStore(t)
	s := newPersistentServer(t, newFakePods(true), srv.URL)
	actor := createActor(t, s)
	uid := actor.GetMetadata().GetUid()
	ref := s.cfg.Snapshots.refFor(uid)
	if resp := doReq(t, http.MethodPut, ref.URL, ref.Token, "", strings.NewReader("x")); resp.StatusCode != http.StatusNoContent {
		t.Fatal(resp.Status)
	}
	if _, err := s.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}}); err != nil {
		t.Fatal(err)
	}
	if resp := doReq(t, http.MethodGet, ref.URL, ref.Token, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("snapshot must go with the actor: %s", resp.Status)
	}
}

func TestEagerStartLeavesTheActorSuspended(t *testing.T) {
	pods := newFakePods(true)
	s := newTestServer(t, pods)
	s.cfg.EagerStart = true
	actor := createActor(t, s)
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("create reports %s", actor.GetStatus().GetState())
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if state, _ := pods.State(context.Background(), "ns", "session-1"); state.Exists {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state, _ := pods.State(context.Background(), "ns", "session-1"); !state.Exists {
		t.Fatal("the pod starts at creation")
	}
	view, err := s.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}})
	if err != nil || view.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("kagent expects SUSPENDED until the first request: %v %v", view, err)
	}
	if pods.hints != 1 {
		t.Fatalf("the warm pool is told about the template: %d", pods.hints)
	}
}

func TestSpecKeyIgnoresEnvButNotImage(t *testing.T) {
	settings := podSettings{CPU: "500m", Memory: "1Gi"}
	base := testTemplate("ns", "a")
	sameRuntime := testTemplate("ns", "b")
	sameRuntime.Containers[0].Env = []*ateapipb.EnvVar{{Name: "KAGENT_CONFIG_JSON", Value: "other agent"}}
	otherImage := testTemplate("ns", "c")
	otherImage.Containers[0].Image = "example.com/runtime@sha256:def"
	otherNamespace := testTemplate("other", "d")
	if specKey(base, settings) != specKey(sameRuntime, settings) {
		t.Error("agents that share a runtime image share a pool")
	}
	if specKey(base, settings) == specKey(otherImage, settings) || specKey(base, settings) == specKey(otherNamespace, settings) {
		t.Error("a different image or namespace needs its own pool")
	}
	if specKey(base, settings) == specKey(base, podSettings{CPU: "1", Memory: "1Gi"}) {
		t.Error("a different pod size needs its own pool")
	}
}

func TestBuildColdPodRestoresAndSavesData(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "session-1", Uid: "uid-1"}}
	pod, err := buildPod(podOptions{
		Name: "session-1", Actor: actor, Template: testTemplate("ns", "tpl"),
		Settings: podSettings{CPU: "400m", Memory: "1Gi", HelperImage: "helper@sha256:1", Persistence: true},
		Snapshot: &snapshotRef{URL: "http://store/v1/data/uid-1", Token: "t"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "restore-data" ||
		!strings.Contains(strings.Join(pod.Spec.InitContainers[0].Command, " "), "--dest=/data") {
		t.Fatalf("init containers: %+v", pod.Spec.InitContainers)
	}
	if len(pod.Spec.Containers) != 2 || pod.Spec.Containers[1].Name != "snapshotter" || pod.Spec.Containers[1].VolumeMounts[0].MountPath != "/data" {
		t.Fatalf("containers: %+v", pod.Spec.Containers)
	}
	if len(pod.Spec.Containers[0].Env) != 1 {
		t.Fatal("a cold pod gets the template env at creation")
	}
}

func TestBuildWarmPodWaitsForItsActor(t *testing.T) {
	pod, err := buildPod(podOptions{
		Name: "warm-abc-12345", Template: testTemplate("ns", "tpl"), WarmKey: "abc",
		Settings: podSettings{CPU: "400m", Memory: "1Gi", HelperImage: "helper@sha256:1", Persistence: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := pod.Spec.Containers[0]
	if pod.Labels[warmKeyLabel] != "abc" || pod.Labels["compute-type"] != "fargate" || pod.Namespace != "ns" {
		t.Fatalf("labels %v namespace %s", pod.Labels, pod.Namespace)
	}
	if len(c.Command) != 2 || c.Command[0] != wrapperPath || len(c.Env) != 0 {
		t.Fatalf("a warm pod starts the wrapper without actor env: %+v", c)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "install-wrapper" {
		t.Fatalf("init containers: %+v", pod.Spec.InitContainers)
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name == "identity" && v.EmptyDir == nil {
			t.Fatal("the wrapper writes the identity files into a writable volume")
		}
	}
	if _, ok := pod.Annotations[actorUIDAnno]; ok {
		t.Fatal("an unclaimed pod belongs to no actor")
	}
}

func TestActivationCarriesIdentityEnvAndSnapshot(t *testing.T) {
	template := testTemplate("ns", "tpl")
	template.Containers[0].Command = []string{"/app"}
	template.Containers[0].Args = []string{"--serve"}
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "session-1", Uid: "uid-1"}}
	act, err := newWarmPool(1, time.Minute).activationFor(context.Background(), startRequest{Actor: actor, Template: template, Snapshot: &snapshotRef{URL: "u", Token: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(act.Command, " ") != "/app --serve" || act.Env["A"] != "b" || act.DataDir != "/data" || act.IdentityDir != "/run/kagent/identity" ||
		act.Files["name"] != "session-1" || act.Files["uid"] != "uid-1" || act.Restore == nil {
		t.Fatalf("activation: %+v", act)
	}
}

func TestPrepareWritesIdentityAndRejectsUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	if err := prepare(context.Background(), activation{IdentityDir: dir, Files: map[string]string{"name": "session-1", "uid": "u"}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "name")); string(got) != "session-1" {
		t.Fatalf("identity file: %q", got)
	}
	if err := prepare(context.Background(), activation{IdentityDir: dir, Files: map[string]string{"../escape": "x"}}); err == nil {
		t.Fatal("a file outside the identity directory must be rejected")
	}
}

func TestPodNameSurvivesRestart(t *testing.T) {
	var saved []byte
	pods := newFakePods(true)
	s := newServer(pods, func(raw []byte) error { saved = raw; return nil }, config{SuspendGrace: time.Minute})
	ctx := context.Background()
	if _, err := s.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns"}}}); err != nil {
		t.Fatal(err)
	}
	createActor(t, s)
	s.mu.Lock()
	s.actors[ref{"ns", "session-1"}].podName = "warm-abc-12345"
	if err := s.saveLocked(); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	restarted := newServer(pods, nil, config{})
	if err := restarted.load(saved); err != nil {
		t.Fatal(err)
	}
	if got := restarted.actors[ref{"ns", "session-1"}].pod(); got != "warm-abc-12345" {
		t.Fatalf("pod name after restart: %s", got)
	}
}

func TestReapKeepsWarmPodsAndClaimedPods(t *testing.T) {
	pods := &warmAwarePods{fakePods: newFakePods(true)}
	s := newTestServer(t, pods)
	createActor(t, s)
	s.mu.Lock()
	rec := s.actors[ref{"ns", "session-1"}]
	rec.podName, rec.wanted = "warm-abc-12345", ateapipb.ActorState_ACTOR_STATE_RUNNING
	s.mu.Unlock()
	pods.pods[ref{"ns", "warm-abc-12345"}] = podState{Exists: true}
	pods.pods[ref{"ns", "warm-abc-idle00"}] = podState{Exists: true}
	pods.pods[ref{"ns", "stray"}] = podState{Exists: true}
	s.reap(context.Background())
	if state, _ := pods.State(context.Background(), "ns", "warm-abc-12345"); !state.Exists {
		t.Error("a claimed pod belongs to its actor")
	}
	if state, _ := pods.State(context.Background(), "ns", "warm-abc-idle00"); !state.Exists {
		t.Error("an unclaimed warm pod belongs to the pool")
	}
	if state, _ := pods.State(context.Background(), "ns", "stray"); state.Exists {
		t.Error("a pod without an actor is removed")
	}
}

// TestReapSparesAWarmPodWhileItsClaimIsInFlight covers the window in which a claimed warm
// pod has lost its warm label but the actor record does not point at it yet.
func TestReapSparesAWarmPodWhileItsClaimIsInFlight(t *testing.T) {
	pods := &claimingPods{fakePods: newFakePods(true)}
	s := newTestServer(t, pods)
	createActor(t, s)
	pods.pods[ref{"ns", "warm-abc-claim"}] = podState{Exists: true}
	s.mu.Lock()
	rec := s.actors[ref{"ns", "session-1"}]
	rec.starting = true
	s.mu.Unlock()
	s.reap(context.Background())
	if state, _ := pods.State(context.Background(), "ns", "warm-abc-claim"); !state.Exists {
		t.Fatal("a pod being claimed for a known actor is not an orphan")
	}
	s.mu.Lock()
	rec.starting = false
	s.mu.Unlock()
	s.reap(context.Background())
	if state, _ := pods.State(context.Background(), "ns", "warm-abc-claim"); state.Exists {
		t.Fatal("once no start is in flight, a pod the actor does not use is an orphan")
	}
}

// claimingPods reports warm-abc-claim as annotated for session-1 without a warm label.
type claimingPods struct{ *fakePods }

func (c *claimingPods) List(ctx context.Context) ([]podInfo, error) {
	infos, err := c.fakePods.List(ctx)
	for i := range infos {
		if infos[i].ref.name == "warm-abc-claim" {
			infos[i].actor = ref{"ns", "session-1"}
		}
	}
	return infos, err
}

type warmAwarePods struct{ *fakePods }

func (w *warmAwarePods) List(context.Context) ([]podInfo, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []podInfo
	for key := range w.pods {
		info := podInfo{ref: key}
		if strings.HasPrefix(key.name, "warm-abc-idle") {
			info.warmKey = "abc"
		}
		out = append(out, info)
	}
	return out, nil
}

func TestNewServerSavesThroughTheSidecarByDefault(t *testing.T) {
	if newServer(newFakePods(true), nil, config{}).save == nil {
		t.Fatal("suspend would panic without a save function")
	}
}

func TestMaintainTopsUpPoolsAndDropsStaleOnes(t *testing.T) {
	client := fake.NewSimpleClientset()
	pods := &k8sPods{client: client, settings: podSettings{CPU: "400m", Memory: "1Gi", HelperImage: "helper@sha256:1"}, warm: newWarmPool(2, time.Hour)}
	template := testTemplate("ns", "tpl")
	pods.Hint(template)
	key := specKey(template, pods.settings)
	ctx := context.Background()
	if _, err := os.Stat(trustBundleSrcPath); err != nil {
		t.Skip("no system CA bundle on this machine")
	}
	pods.Maintain(ctx)
	list, _ := client.CoreV1().Pods("ns").List(ctx, metav1.ListOptions{LabelSelector: warmKeyLabel + "=" + key})
	if len(list.Items) != 2 {
		t.Fatalf("a pool of 2 keeps 2 idle pods, got %d", len(list.Items))
	}
	for _, p := range list.Items {
		// The API server defaults a new pod to Pending; the fake clientset does not.
		p.Status.Phase = corev1.PodPending
		if _, err := client.CoreV1().Pods("ns").Update(ctx, &p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pods.Maintain(ctx)
	if list, _ = client.CoreV1().Pods("ns").List(ctx, metav1.ListOptions{LabelSelector: warmKeyLabel + "=" + key}); len(list.Items) != 2 {
		t.Fatalf("topping up twice must not add pods, got %d", len(list.Items))
	}
	old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "warm-gone-aaaaa", Namespace: "ns", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		Labels: map[string]string{managedByLabel: managedByValue, warmKeyLabel: "gone"},
	}}
	young := old.DeepCopy()
	young.Name, young.CreationTimestamp = "warm-gone-bbbbb", metav1.Now()
	for _, p := range []*corev1.Pod{old, young} {
		if _, err := client.CoreV1().Pods("ns").Create(ctx, p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pods.Maintain(ctx)
	if _, err := client.CoreV1().Pods("ns").Get(ctx, "warm-gone-aaaaa", metav1.GetOptions{}); err == nil {
		t.Error("an old pod of an unknown pool is deleted")
	}
	if _, err := client.CoreV1().Pods("ns").Get(ctx, "warm-gone-bbbbb", metav1.GetOptions{}); err != nil {
		t.Error("a young pod is left alone")
	}
}

func warmPod(name, key string, phase corev1.PodPhase, created time.Time) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{managedByLabel: managedByValue, warmKeyLabel: key},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func TestSurplusWarmPodsPrefersUnreadyThenNewest(t *testing.T) {
	now := time.Now()
	live := []corev1.Pod{
		warmPod("old-running", "k", corev1.PodRunning, now.Add(-3*time.Minute)),
		warmPod("new-running", "k", corev1.PodRunning, now.Add(-time.Minute)),
		warmPod("pending", "k", corev1.PodPending, now.Add(-2*time.Minute)),
		warmPod("claiming", "k", corev1.PodRunning, now),
	}
	claiming := func(p *corev1.Pod) bool { return p.Name == "claiming" }
	names := func(pods []corev1.Pod) []string {
		var out []string
		for _, p := range pods {
			out = append(out, p.Name)
		}
		return out
	}
	for _, tc := range []struct {
		size int
		want []string
	}{
		{size: 4, want: nil},
		{size: 3, want: []string{"pending"}},
		{size: 2, want: []string{"pending", "new-running"}},
		{size: 0, want: []string{"pending", "new-running", "old-running"}},
	} {
		if got := names(surplusWarmPods(live, tc.size, claiming)); strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("size %d: deleted %v, want %v", tc.size, got, tc.want)
		}
	}
}

func TestMaintainDeletesSurplusWarmPods(t *testing.T) {
	client := fake.NewSimpleClientset()
	pods := &k8sPods{client: client, settings: podSettings{CPU: "400m", Memory: "1Gi", HelperImage: "helper@sha256:1"}, warm: newWarmPool(1, time.Hour)}
	template := testTemplate("ns", "tpl")
	pods.Hint(template)
	key := specKey(template, pods.settings)
	ctx := context.Background()
	for i, name := range []string{"warm-a", "warm-b", "warm-c"} {
		pod := warmPod(name, key, corev1.PodRunning, time.Now().Add(time.Duration(i)*time.Second))
		if _, err := client.CoreV1().Pods("ns").Create(ctx, &pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pods.Maintain(ctx)
	list, _ := client.CoreV1().Pods("ns").List(ctx, metav1.ListOptions{LabelSelector: warmKeyLabel + "=" + key})
	if len(list.Items) != 1 || list.Items[0].Name != "warm-a" {
		t.Fatalf("lowering the pool to 1 keeps only the oldest ready pod, got %v", list.Items)
	}
}

func TestMaintainWithThePoolOffRemovesOnlyIdleWarmPods(t *testing.T) {
	client := fake.NewSimpleClientset()
	pods := &k8sPods{client: client}
	ctx := context.Background()
	idle := warmPod("warm-idle", "k", corev1.PodRunning, time.Now())
	claimed := warmPod("warm-claimed", "", corev1.PodRunning, time.Now())
	delete(claimed.Labels, warmKeyLabel)
	for _, p := range []corev1.Pod{idle, claimed} {
		if _, err := client.CoreV1().Pods("ns").Create(ctx, &p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pods.Maintain(ctx)
	if _, err := client.CoreV1().Pods("ns").Get(ctx, "warm-idle", metav1.GetOptions{}); err == nil {
		t.Error("an idle warm pod is deleted once the pool is off")
	}
	if _, err := client.CoreV1().Pods("ns").Get(ctx, "warm-claimed", metav1.GetOptions{}); err != nil {
		t.Error("a claimed pod belongs to its actor and stays")
	}
}

// TestConcurrentClaimsActivateInParallel runs one fake wrapper per loopback address on
// the wrapper port. Each activation takes 300ms; serial claims would take 1.2s for 4.
func TestConcurrentClaimsActivateInParallel(t *testing.T) {
	const n, delay = 4, 300 * time.Millisecond
	client := fake.NewSimpleClientset()
	pods := &k8sPods{client: client, settings: podSettings{CPU: "400m", Memory: "1Gi", HelperImage: "helper@sha256:1"}, warm: newWarmPool(n, time.Hour)}
	template := testTemplate("ns", "tpl")
	template.Containers[0].Command = []string{"/runtime"}
	key := specKey(template, pods.settings)
	ctx := context.Background()
	activations := make([]int, n)
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		ip := fmt.Sprintf("127.0.0.%d", i+2)
		ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, wrapperPort))
		if err != nil {
			t.Skipf("cannot listen on %s:%d: %v", ip, wrapperPort, err)
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				time.Sleep(delay)
				mu.Lock()
				activations[i]++
				mu.Unlock()
			}
		})}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
		pod := warmPod(fmt.Sprintf("warm-%d", i), key, corev1.PodRunning, time.Now())
		pod.Status.PodIP = ip
		if _, err := client.CoreV1().Pods("ns").Create(ctx, &pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	claimed := make([]string, n)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: fmt.Sprintf("session-%d", i), Uid: uuid.NewString()}}
			claimed[i], _ = pods.warm.claim(ctx, pods, startRequest{Actor: actor, Template: template})
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	seen := map[string]bool{}
	for i, name := range claimed {
		if name == "" || seen[name] {
			t.Fatalf("claim %d got %q; every claim needs its own pod: %v", i, name, claimed)
		}
		seen[name] = true
	}
	for i, count := range activations {
		if count != 1 {
			t.Errorf("wrapper %d was activated %d times", i, count)
		}
	}
	if elapsed > 2*delay {
		t.Errorf("%d claims took %v; they should overlap, not queue", n, elapsed)
	}
}

func TestPauseSavesDataButKeepsThePod(t *testing.T) {
	pods := newFakePods(true)
	s := newPersistentServer(t, pods, "http://store")
	var saved int
	s.save = func(context.Context, string, snapshotRef) error { saved++; return nil }
	createActor(t, s)
	obj := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	if _, err := s.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: obj}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.PauseActor(context.Background(), &ateapipb.PauseActorRequest{Actor: obj})
	if err != nil || resp.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		t.Fatalf("pause: %v %v", resp, err)
	}
	if saved != 1 {
		t.Fatalf("pause saves /data once, saved %d times", saved)
	}
	if state, _ := pods.State(context.Background(), "ns", "session-1"); !state.Exists {
		t.Fatal("pause keeps the pod, so the paused operation resumes instantly")
	}
}
