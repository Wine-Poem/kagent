package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakePods struct {
	mu      sync.Mutex
	pods    map[ref]podState
	started []startRequest
	ready   bool
	hints   int
}

func newFakePods(ready bool) *fakePods { return &fakePods{pods: map[ref]podState{}, ready: ready} }

func (f *fakePods) Start(_ context.Context, req startRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := ref{req.Actor.GetMetadata().GetAtespace(), req.Actor.GetMetadata().GetName()}
	if _, ok := f.pods[key]; !ok {
		f.started = append(f.started, req)
		f.pods[key] = podState{Exists: true, Ready: f.ready, IP: "10.0.0.7", Node: "fargate-ip-10-0-0-7"}
	}
	return key.name, nil
}

func (f *fakePods) State(_ context.Context, atespace, name string) (podState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pods[ref{atespace, name}], nil
}

func (f *fakePods) Delete(_ context.Context, atespace, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pods, ref{atespace, name})
	return nil
}

func (f *fakePods) List(context.Context) ([]podInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []podInfo
	for key := range f.pods {
		out = append(out, podInfo{ref: key})
	}
	return out, nil
}

func (f *fakePods) Hint(*ateapipb.ActorTemplate) { f.mu.Lock(); f.hints++; f.mu.Unlock() }
func (f *fakePods) Maintain(context.Context)     {}

func testTemplate(atespace, name string) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Containers: []*ateapipb.Container{{
			Name: "agent", Image: "example.com/runtime@sha256:abc", Env: []*ateapipb.EnvVar{{Name: "A", Value: "b"}},
			WakeupProbe:  &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Path: "/readyz", Port: 8081}},
			VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/data"}, {Name: "identity", MountPath: "/run/kagent/identity"}, {Name: "trust", MountPath: "/run/kagent/egress"}},
		}},
		Volumes: []*ateapipb.Volume{
			{Name: "data", DurableDir: &ateapipb.DurableDirVolumeSource{}},
			{Name: "identity", SystemInfo: &ateapipb.SystemInfoVolumeSource{DataSources: []*ateapipb.SystemInfoDataSource{{ActorMetadata: &ateapipb.ActorMetadataDataSource{Items: []*ateapipb.ActorMetadataItem{
				{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "name"},
				{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "uid"},
			}}}}}},
			{Name: "trust", SystemInfo: &ateapipb.SystemInfoVolumeSource{DataSources: []*ateapipb.SystemInfoDataSource{{TrustBundle: &ateapipb.TrustBundleDataSource{Names: []string{"egress-mitm.ate.dev"}, Path: "trust-bundle.pem"}}}}},
		},
	}
}

func newTestServer(t *testing.T, pods podManager) *server {
	t.Helper()
	s := newServer(pods, func([]byte) error { return nil }, config{SuspendGrace: time.Minute, ResumeWait: 3 * time.Second, RouterWait: 3 * time.Second})
	ctx := context.Background()
	if _, err := s.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns"}}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func createActor(t *testing.T, s *server) *ateapipb.Actor {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: testTemplate("ns", "tpl")}); err != nil {
		t.Fatal(err)
	}
	actor, err := s.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns", Name: "session-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "tpl"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func TestActorTemplateKeepsSpecAndReportsGoldenTag(t *testing.T) {
	s := newTestServer(t, newFakePods(true))
	ctx := context.Background()
	want := testTemplate("ns", "tpl")
	got, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: want})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() == nil {
		t.Fatal("controller waits for a golden tag")
	}
	if !proto.Equal(got.GetContainers()[0], want.GetContainers()[0]) || len(got.GetVolumes()) != len(want.GetVolumes()) {
		t.Fatal("the stored template must keep the compiled spec unchanged")
	}
	if _, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: want}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate template: %v", err)
	}
}

func TestActorLifecycle(t *testing.T) {
	pods := newFakePods(true)
	s := newTestServer(t, pods)
	ctx := context.Background()
	actor := createActor(t, s)
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || actor.GetMetadata().GetUid() == "" {
		t.Fatalf("a new actor is suspended and has a uid: %v", actor)
	}
	if len(pods.started) != 0 {
		t.Fatal("creating an actor must not start a pod")
	}
	ref := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	resumed, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	if err != nil || resumed.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || resumed.GetActor().GetStatus().GetAssignedNode() == "" {
		t.Fatalf("resume: %v %v", resumed, err)
	}
	suspended, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	if err != nil || suspended.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || suspended.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri() == "" {
		t.Fatalf("suspend must report SUSPENDED with a snapshot marker: %v %v", suspended, err)
	}
	paused, err := s.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref})
	if err != nil || paused.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		t.Fatalf("pause: %v %v", paused, err)
	}
	if _, err := s.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if state, _ := pods.State(ctx, "ns", "session-1"); state.Exists {
		t.Fatal("deleting an actor deletes its pod")
	}
	if _, err := s.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted actor: %v", err)
	}
}

func TestResumeIsAcceptedWhilePodStarts(t *testing.T) {
	s := newTestServer(t, newFakePods(false))
	s.cfg.ResumeWait = 1100 * time.Millisecond
	createActor(t, s)
	resp, err := s.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("an accepted resume reports RUNNING while the router holds requests, got %s", resp.GetActor().GetStatus().GetState())
	}
	view, err := s.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}})
	if err != nil || view.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("a read still shows the truth, a pod that is not ready is RESUMING: %v %v", view.GetStatus(), err)
	}
}

func TestSuspendedActorKeepsPodOnlyForTheGracePeriod(t *testing.T) {
	pods := newFakePods(true)
	s := newTestServer(t, pods)
	ctx := context.Background()
	createActor(t, s)
	ref := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	if _, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	s.reap(ctx)
	if state, _ := pods.State(ctx, "ns", "session-1"); !state.Exists {
		t.Fatal("pod must stay during the grace period")
	}
	now := time.Now().Add(2 * time.Minute)
	s.now = func() time.Time { return now }
	s.reap(ctx)
	if state, _ := pods.State(ctx, "ns", "session-1"); state.Exists {
		t.Fatal("pod must be deleted after the grace period")
	}
}

func TestEgressPolicyIsImmutable(t *testing.T) {
	s := newTestServer(t, newFakePods(true))
	ctx := context.Background()
	createActor(t, s)
	req := &ateapipb.CreateActorEgressPolicyRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}, EgressPolicy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{}}}}
	if _, err := s.CreateActorEgressPolicy(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateActorEgressPolicy(ctx, req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second create: %v", err)
	}
	got, err := s.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: req.GetActor()})
	if err != nil || len(got.GetRules()) != 1 {
		t.Fatalf("get: %v %v", got, err)
	}
}

func TestRestoreFromTagIsRejected(t *testing.T) {
	s := newTestServer(t, newFakePods(true))
	if _, err := s.CreateActorTemplate(context.Background(), &ateapipb.CreateActorTemplateRequest{ActorTemplate: testTemplate("ns", "tpl")}); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns", Name: "fork"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "tpl"},
		SourceTag:     &ateapipb.ObjectRef{Atespace: "ns", Name: "tag"},
	}})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("restore from a tag: %v", err)
	}
}

func TestStateSurvivesRestart(t *testing.T) {
	var saved []byte
	pods := newFakePods(true)
	s := newServer(pods, func(raw []byte) error { saved = raw; return nil }, config{SuspendGrace: time.Minute})
	ctx := context.Background()
	if _, err := s.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns"}}}); err != nil {
		t.Fatal(err)
	}
	createActor(t, s)
	restarted := newServer(pods, nil, config{SuspendGrace: time.Minute})
	if err := restarted.load(saved); err != nil {
		t.Fatal(err)
	}
	actor, err := restarted.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}})
	if err != nil || actor.GetMetadata().GetUid() == "" {
		t.Fatalf("actor after restart: %v %v", actor, err)
	}
}

func TestBuildPodForFargate(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "session-1", Uid: "uid-1"}}
	pod, err := buildPod(podOptions{Name: "session-1", Actor: actor, Template: testTemplate("ns", "tpl"), Settings: podSettings{CPU: "500m", Memory: "1Gi"}})
	if err != nil {
		t.Fatal(err)
	}
	if pod.Labels["compute-type"] != "fargate" || pod.Namespace != "ns" || pod.Name != "session-1" {
		t.Fatalf("a Fargate profile selects the pod by this label: %v", pod.ObjectMeta)
	}
	c := pod.Spec.Containers[0]
	if c.SecurityContext != nil {
		t.Fatalf("root is opt-in: %+v", c.SecurityContext)
	}
	if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet.Port.IntValue() != 8081 || c.Env[0].Name != "A" {
		t.Fatalf("container: %+v", c)
	}
	byName := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		byName[v.Name] = true
		switch v.Name {
		case "data":
			if v.EmptyDir == nil {
				t.Fatal("durable dir becomes an emptyDir")
			}
		case "identity":
			if v.DownwardAPI == nil || len(v.DownwardAPI.Items) != 2 {
				t.Fatalf("identity files come from annotations: %+v", v)
			}
		case "trust":
			if v.ConfigMap == nil || v.ConfigMap.Items[0].Path != "trust-bundle.pem" {
				t.Fatalf("trust bundle: %+v", v)
			}
		}
	}
	if len(byName) != 3 || pod.Annotations["ate.dev/actor-uid"] != "uid-1" {
		t.Fatalf("volumes %v annotations %v", byName, pod.Annotations)
	}
}

func TestBuildPodRejectsUnsupportedVolumes(t *testing.T) {
	template := testTemplate("ns", "tpl")
	template.Volumes = append(template.Volumes, &ateapipb.Volume{Name: "guest", Image: &ateapipb.ImageVolumeSource{Reference: "x"}})
	if _, err := buildPod(podOptions{Name: "a", Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "a"}}, Template: template, Settings: podSettings{CPU: "1", Memory: "1Gi"}}); err == nil {
		t.Fatal("image volumes are not supported on Fargate")
	}
}

func TestParseTarget(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    ref
		wantErr bool
	}{
		{"kagent/session-1", ref{"kagent", "session-1"}, false},
		{"", ref{}, true},
		{"kagent", ref{}, true},
		{"kagent/a/b", ref{}, true},
		{"/a", ref{}, true},
	} {
		got, err := parseTarget(tc.in)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("parseTarget(%q) = %v, %v", tc.in, got, err)
		}
	}
}

func TestBuildPodCanRunAsRoot(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "session-1", Uid: "uid-1"}}
	pod, err := buildPod(podOptions{Name: "session-1", Actor: actor, Template: testTemplate("ns", "tpl"), Settings: podSettings{CPU: "500m", Memory: "1Gi", RunAsRoot: true}})
	if err != nil {
		t.Fatal(err)
	}
	if sc := pod.Spec.Containers[0].SecurityContext; sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 0 {
		t.Fatalf("security context: %+v", sc)
	}
}
