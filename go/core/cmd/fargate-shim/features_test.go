package main

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLostPodIsReportedSuspendedAndRestarts(t *testing.T) {
	pods := newFakePods(true)
	s := newTestServer(t, pods)
	ctx := context.Background()
	createActor(t, s)
	obj := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	if _, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: obj}); err != nil {
		t.Fatal(err)
	}
	if err := pods.Delete(ctx, "ns", "session-1"); err != nil {
		t.Fatal(err)
	}
	view, err := s.GetActor(ctx, &ateapipb.GetActorRequest{Actor: obj})
	if err != nil || view.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || view.GetStatus().GetExternalSnapshot().GetSnapshotUri() == "" {
		t.Fatalf("kagent can only resume SUSPENDED actors, got %v %v", view.GetStatus(), err)
	}
	if _, err := s.ensureRunning(ctx, ref{"ns", "session-1"}, time.Second); err != nil {
		t.Fatalf("the next request starts a new pod: %v", err)
	}
}

func TestSnapshotScopeFollowsTheStore(t *testing.T) {
	without := newTestServer(t, newFakePods(true))
	createActor(t, without)
	view, _ := without.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}})
	if view.GetStatus().GetExternalSnapshot().GetContentScope() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		t.Error("without a store nothing can be restored, so kagent must not fork")
	}
	with := newPersistentServer(t, newFakePods(true), "http://store")
	createActor(t, with)
	view, _ = with.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}})
	if view.GetStatus().GetExternalSnapshot().GetContentScope() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		t.Error("with a store /data is restored, which is a DATA snapshot")
	}
}

func suspendedWithData(t *testing.T) (*server, string) {
	t.Helper()
	srv, _ := newStore(t)
	s := newPersistentServer(t, newFakePods(true), srv.URL)
	s.save = func(ctx context.Context, _ string, ref snapshotRef) error {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "sessions.db"), []byte("conversation of the source"), 0o644); err != nil {
			return err
		}
		return saveData(ctx, ref, dir)
	}
	actor := createActor(t, s)
	obj := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	if _, err := s.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: obj}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: obj}); err != nil {
		t.Fatal(err)
	}
	return s, actor.GetMetadata().GetUid()
}

func TestCheckpointAndForkCarryTheData(t *testing.T) {
	s, sourceUID := suspendedWithData(t)
	ctx := context.Background()
	source := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	suspended, _ := s.GetActor(ctx, &ateapipb.GetActorRequest{Actor: source})
	tag, err := s.CreateTag(ctx, &ateapipb.CreateTagRequest{Tag: &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "checkpoint-1"}, SourceActor: source, Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if tag.GetMetadata().GetUid() == "" || tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" ||
		tag.GetStatus().GetActorTemplateUid() != suspended.GetStatus().GetExternalSnapshot().GetActorTemplateUid() ||
		tag.GetStatus().GetSnapshot().GetContentScope() != suspended.GetStatus().GetExternalSnapshot().GetContentScope() {
		t.Fatalf("kagent checks these fields of the tag: %v", tag)
	}
	if _, err := s.CreateTag(ctx, &ateapipb.CreateTagRequest{Tag: tag}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second create: %v", err)
	}
	fork, err := s.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns", Name: "session-fork"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "tpl"},
		SourceTag:     &ateapipb.ObjectRef{Atespace: "ns", Name: "checkpoint-1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if fork.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED ||
		fork.GetStatus().GetExternalSnapshot().GetSnapshotUri() != tag.GetStatus().GetSnapshot().GetSnapshotUri() ||
		fork.GetSourceTag().GetName() != "checkpoint-1" || fork.GetMetadata().GetUid() == sourceUID {
		t.Fatalf("a fork starts SUSPENDED from the tag's snapshot with its own identity: %v", fork)
	}
	forkRef := s.cfg.Snapshots.refFor(fork.GetMetadata().GetUid())
	dst := t.TempDir()
	if err := restoreData(ctx, *forkRef, dst); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "sessions.db")); string(got) != "conversation of the source" {
		t.Fatalf("the fork restores the source's conversation, got %q", got)
	}
	// The fork owns a copy: deleting the source and the tag must not remove it.
	if _, err := s.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: source}); err != nil {
		t.Fatal(err)
	}
	if err := restoreData(ctx, *forkRef, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns", Name: "checkpoint-1"}}); err != nil {
		t.Fatal(err)
	}
	again := t.TempDir()
	if err := restoreData(ctx, *forkRef, again); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(again, "sessions.db")); string(got) != "conversation of the source" {
		t.Fatalf("the fork keeps its data after the tag is gone, got %q", got)
	}
	if _, err := s.GetTag(ctx, &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns", Name: "checkpoint-1"}}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted tag: %v", err)
	}
}

func TestTagNeedsASuspendedActorAndAMatchingTemplate(t *testing.T) {
	s, _ := suspendedWithData(t)
	ctx := context.Background()
	source := &ateapipb.ObjectRef{Atespace: "ns", Name: "session-1"}
	if _, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: source}); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateTag(ctx, &ateapipb.CreateTagRequest{Tag: &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "running"}, SourceActor: source, Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE,
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a running actor cannot be tagged: %v", err)
	}
	if _, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: source}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTag(ctx, &ateapipb.CreateTagRequest{Tag: &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "t"}, SourceActor: source, Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE,
	}}); err != nil {
		t.Fatal(err)
	}
	other := testTemplate("ns", "other")
	if _, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: other}); err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns", Name: "wrong"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "other"},
		SourceTag:     &ateapipb.ObjectRef{Atespace: "ns", Name: "t"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a tag only restores into the template revision it came from: %v", err)
	}
}

func TestStoreCopyIsAdminOnly(t *testing.T) {
	srv, _ := newStore(t)
	a, b := uuid.NewString(), uuid.NewString()
	if resp := doReq(t, http.MethodPut, srv.URL+"/v1/data/"+a, tokenFor(testSecret, a), "", strings.NewReader("data of a")); resp.StatusCode != http.StatusNoContent {
		t.Fatal(resp.Status)
	}
	target := srv.URL + "/v1/data/" + b + "?copy-from=" + a
	if resp := doReq(t, http.MethodPost, target, tokenFor(testSecret, b), "", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("an actor cannot copy: %s", resp.Status)
	}
	if resp := doReq(t, http.MethodPost, target, "", tokenFor(testSecret, "admin"), nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("admin copy: %s", resp.Status)
	}
	resp := doReq(t, http.MethodGet, srv.URL+"/v1/data/"+b, tokenFor(testSecret, b), "", nil)
	if got, _ := io.ReadAll(resp.Body); string(got) != "data of a" {
		t.Fatalf("copy: %q", got)
	}
	empty := uuid.NewString()
	if resp := doReq(t, http.MethodPost, srv.URL+"/v1/data/"+b+"?copy-from="+empty, "", tokenFor(testSecret, "admin"), nil); resp.StatusCode != http.StatusNoContent {
		t.Fatal(resp.Status)
	}
	if resp := doReq(t, http.MethodGet, srv.URL+"/v1/data/"+b, tokenFor(testSecret, b), "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("a source without data empties the destination: %s", resp.Status)
	}
}

func sandboxTemplate() *ateapipb.ActorTemplate {
	t := testTemplate("ns", "sandbox")
	t.Containers[0].Command = []string{"/run/kagent/guest/usr/local/bin/kagent-sandbox-guest"}
	t.Containers[0].VolumeMounts = append(t.Containers[0].VolumeMounts, &ateapipb.VolumeMount{Name: "guest", MountPath: "/run/kagent/guest"})
	t.Volumes = append(t.Volumes, &ateapipb.Volume{Name: "guest", Image: &ateapipb.ImageVolumeSource{Reference: "example.com/guest@sha256:abc"}})
	t.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "1"}, {Name: "memory", Quantity: "2Gi"}}}
	return t
}

func TestBuildPodUnpacksImageVolumesAndHonoursTemplateLimits(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "sandbox-1", Uid: "u"}}
	pod, err := buildPod(podOptions{Name: "sandbox-1", Actor: actor, Template: sandboxTemplate(), Settings: podSettings{CPU: "400m", Memory: "1Gi", HelperImage: "helper@sha256:1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "extract-guest" ||
		!strings.Contains(strings.Join(pod.Spec.InitContainers[0].Command, " "), "--image=example.com/guest@sha256:abc") {
		t.Fatalf("init containers: %+v", pod.Spec.InitContainers)
	}
	var guest bool
	for _, v := range pod.Spec.Volumes {
		guest = guest || (v.Name == "guest" && v.EmptyDir != nil)
	}
	if !guest {
		t.Fatal("the image volume becomes an emptyDir")
	}
	c := pod.Spec.Containers[0]
	if c.Resources.Limits.Cpu().String() != "1" || c.Resources.Limits.Memory().String() != "2Gi" {
		t.Fatalf("limits of the template apply: %v", c.Resources.Limits)
	}
	if _, err := buildPod(podOptions{Name: "x", Actor: actor, Template: sandboxTemplate(), Settings: podSettings{CPU: "1", Memory: "1Gi"}}); err == nil {
		t.Fatal("without the helper image an image volume cannot be unpacked")
	}
	if specKey(sandboxTemplate(), podSettings{CPU: "1", Memory: "1Gi"}) == specKey(testTemplate("ns", "sandbox"), podSettings{CPU: "1", Memory: "1Gi"}) {
		t.Error("sandbox and agent pods need different pools")
	}
}

func TestUntarEntriesSkipsSymlinksOnlyWhenNotStrict(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Name: "usr/local/bin/guest", Mode: 0o755, Size: 2, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte("ok"))
	_ = tw.WriteHeader(&tar.Header{Name: "bin/sh", Linkname: "/bin/busybox", Typeflag: tar.TypeSymlink})
	tw.Close()
	dir := t.TempDir()
	if err := untarEntries(tar.NewReader(bytes.NewReader(buf.Bytes())), dir, true); err == nil {
		t.Fatal("strict mode rejects symlinks")
	}
	dir = t.TempDir()
	if err := untarEntries(tar.NewReader(bytes.NewReader(buf.Bytes())), dir, false); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "usr/local/bin/guest"))
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("the guest binary keeps its mode: %v %v", info, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "bin/sh")); err == nil {
		t.Fatal("symlinks are skipped")
	}
}
