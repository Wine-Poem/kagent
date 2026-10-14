package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ref struct{ atespace, name string }

func (r ref) String() string { return r.atespace + "/" + r.name }

func refOf(o *ateapipb.ObjectRef) ref { return ref{o.GetAtespace(), o.GetName()} }

type podState struct {
	Exists bool
	Ready  bool
	IP     string
	Node   string
}

type podManager interface {
	// Start returns the name of the pod that serves the actor. That is a new pod named
	// like the actor, or a warm pod the actor claimed.
	Start(ctx context.Context, req startRequest) (string, error)
	State(ctx context.Context, atespace, pod string) (podState, error)
	Delete(ctx context.Context, atespace, pod string) error
	List(ctx context.Context) ([]podInfo, error)
	// Hint tells the warm pool that a template is in use; Maintain refills the pools.
	Hint(template *ateapipb.ActorTemplate)
	Maintain(ctx context.Context)
}

type config struct {
	SuspendGrace time.Duration
	ResumeWait   time.Duration
	RouterWait   time.Duration
	// EagerStart starts the pod when the actor is created instead of at the first request.
	EagerStart bool
	Snapshots  *snapshots
}

type actorRecord struct {
	actor       *ateapipb.Actor
	policy      *ateapipb.EgressPolicy
	wanted      ateapipb.ActorState
	snapshotN   int
	suspendedAt time.Time
	// podName is empty until a pod was started; a claimed warm pod keeps its own name.
	podName string
	// startMu serializes pod starts per actor, so an eager start and a resume cannot claim two warm pods.
	startMu sync.Mutex
}

func (r *actorRecord) pod() string {
	if r.podName != "" {
		return r.podName
	}
	return r.actor.GetMetadata().GetName()
}

// server implements the subset of the ate-api Control service that kagent uses.
// Every Actor is a Fargate pod; suspend keeps the pod for a grace period.
type server struct {
	ateapipb.UnimplementedControlServer
	mu        sync.Mutex
	atespaces map[string]*ateapipb.Atespace
	templates map[ref]*ateapipb.ActorTemplate
	actors    map[ref]*actorRecord
	tags      map[ref]*ateapipb.Tag
	pods      podManager
	persist   func([]byte) error
	cfg       config
	now       func() time.Time
	save      func(ctx context.Context, podIP string, ref snapshotRef) error
}

func newServer(pods podManager, persist func([]byte) error, cfg config) *server {
	return &server{
		atespaces: map[string]*ateapipb.Atespace{},
		templates: map[ref]*ateapipb.ActorTemplate{},
		actors:    map[ref]*actorRecord{},
		tags:      map[ref]*ateapipb.Tag{},
		pods:      pods, persist: persist, cfg: cfg, now: time.Now, save: requestSave,
	}
}

func meta(r ref) *ateapipb.ResourceMetadata {
	now := timestamppb.Now()
	return &ateapipb.ResourceMetadata{Atespace: r.atespace, Name: r.name, Uid: uuid.NewString(), Version: 1, CreateTime: now, UpdateTime: now}
}

func (s *server) CreateAtespace(_ context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	name := req.GetAtespace().GetMetadata().GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.atespaces[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "atespace exists")
	}
	created := &ateapipb.Atespace{Metadata: meta(ref{name: name})}
	s.atespaces[name] = created
	return proto.Clone(created).(*ateapipb.Atespace), s.saveLocked()
}

func (s *server) GetAtespace(_ context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, ok := s.atespaces[req.GetAtespace().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "atespace not found")
	}
	return proto.Clone(found).(*ateapipb.Atespace), nil
}

func (s *server) ListAtespaces(context.Context, *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &ateapipb.ListAtespacesResponse{}
	for _, a := range s.atespaces {
		resp.Atespaces = append(resp.Atespaces, proto.Clone(a).(*ateapipb.Atespace))
	}
	return resp, nil
}

func (s *server) CreateActorTemplate(_ context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	template := req.GetActorTemplate()
	key := ref{template.GetMetadata().GetAtespace(), template.GetMetadata().GetName()}
	if key.atespace == "" || key.name == "" || len(template.GetContainers()) != 1 || template.GetContainers()[0].GetImage() == "" {
		return nil, status.Error(codes.InvalidArgument, "template needs an atespace, a name and exactly one container with an image")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.atespaces[key.atespace]; !ok {
		return nil, status.Error(codes.FailedPrecondition, "atespace does not exist")
	}
	if _, ok := s.templates[key]; ok {
		return nil, status.Error(codes.AlreadyExists, "template exists")
	}
	stored := proto.Clone(template).(*ateapipb.ActorTemplate)
	stored.Metadata = meta(key)
	// No snapshot is taken: the golden tag only signals readiness to the controller.
	stored.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
		GoldenTag:            &ateapipb.ObjectRef{Atespace: key.atespace, Name: key.name + "-golden"},
		TakeGoldenSnapshotAt: timestamppb.Now(),
	}}
	s.templates[key] = stored
	return proto.Clone(stored).(*ateapipb.ActorTemplate), s.saveLocked()
}

func (s *server) GetActorTemplate(_ context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, ok := s.templates[refOf(req.GetActorTemplate())]
	if !ok {
		return nil, status.Error(codes.NotFound, "template not found")
	}
	return proto.Clone(found).(*ateapipb.ActorTemplate), nil
}

func (s *server) ListActorTemplates(_ context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &ateapipb.ListActorTemplatesResponse{}
	for key, t := range s.templates {
		if req.GetAtespace() == "" || req.GetAtespace() == key.atespace {
			resp.ActorTemplates = append(resp.ActorTemplates, proto.Clone(t).(*ateapipb.ActorTemplate))
		}
	}
	sort.Slice(resp.ActorTemplates, func(i, j int) bool {
		return resp.ActorTemplates[i].GetMetadata().GetName() < resp.ActorTemplates[j].GetMetadata().GetName()
	})
	return resp, nil
}

func (s *server) DeleteActorTemplate(_ context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := refOf(req.GetActorTemplate())
	found, ok := s.templates[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "template not found")
	}
	delete(s.templates, key)
	return found, s.saveLocked()
}

func (s *server) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	actor := req.GetActor()
	key := ref{actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName()}
	if key.atespace == "" || key.name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor needs an atespace and a name")
	}
	s.mu.Lock()
	if _, ok := s.atespaces[key.atespace]; !ok {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "atespace does not exist")
	}
	template, ok := s.templates[refOf(actor.GetActorTemplate())]
	if !ok {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "actor template not found")
	}
	if _, ok := s.actors[key]; ok {
		s.mu.Unlock()
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	stored := &ateapipb.Actor{
		Metadata:       meta(key),
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: actor.GetActorTemplate().GetAtespace(), Name: actor.GetActorTemplate().GetName()},
		WorkerSelector: template.GetWorkerSelector(),
	}
	var tag *ateapipb.Tag
	if source := actor.GetSourceTag(); source != nil {
		tag = s.tags[refOf(source)]
		switch {
		case !s.cfg.Snapshots.enabled():
			s.mu.Unlock()
			return nil, status.Error(codes.Unimplemented, "forking needs the snapshot store")
		case tag == nil:
			s.mu.Unlock()
			return nil, status.Error(codes.NotFound, "tag not found")
		case tag.GetStatus().GetActorTemplateUid() != template.GetMetadata().GetUid():
			s.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "the tag was taken from a different template revision")
		}
		stored.SourceTag = &ateapipb.ObjectRef{Atespace: source.GetAtespace(), Name: source.GetName()}
	}
	s.mu.Unlock()
	if tag != nil {
		// The new Actor starts from its own copy, so later saves never touch the tag.
		if err := s.cfg.Snapshots.copy(ctx, tag.GetMetadata().GetUid(), stored.GetMetadata().GetUid()); err != nil {
			return nil, status.Errorf(codes.Unavailable, "copy the tag's data: %v", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.actors[key]; ok {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	rec := &actorRecord{actor: stored, wanted: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, suspendedAt: s.now()}
	s.actors[key] = rec
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	s.pods.Hint(template)
	if s.cfg.EagerStart {
		go s.eagerStart(key)
	}
	return s.viewLocked(context.Background(), rec), nil
}

func (s *server) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[refOf(req.GetActor())]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	return s.viewLocked(ctx, rec), nil
}

func (s *server) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &ateapipb.ListActorsResponse{}
	for key, rec := range s.actors {
		if req.GetAtespace() == "" || req.GetAtespace() == key.atespace {
			resp.Actors = append(resp.Actors, s.viewLocked(ctx, rec))
		}
	}
	sort.Slice(resp.Actors, func(i, j int) bool {
		return resp.Actors[i].GetMetadata().GetName() < resp.Actors[j].GetMetadata().GetName()
	})
	return resp, nil
}

func (s *server) ListWorkers(context.Context, *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	return &ateapipb.ListWorkersResponse{}, nil
}

func (s *server) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	key := refOf(req.GetActor())
	if _, err := s.ensureRunning(ctx, key, s.cfg.ResumeWait); err != nil && status.Code(err) != codes.DeadlineExceeded {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	view := s.viewLocked(ctx, rec)
	if view.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RESUMING {
		// kagent gives a resume call 30 seconds, a cold Fargate pod needs about 45. The resume is
		// accepted: the pod is on its way and the router holds requests until it is ready.
		view.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	}
	return &ateapipb.ResumeActorResponse{Actor: view, Resumed: true}, nil
}

func (s *server) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	key := refOf(req.GetActor())
	// /data is saved before the state changes, so a failed save leaves the actor running and kagent retries.
	if err := s.saveData(ctx, key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	if rec.wanted != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		rec.wanted, rec.suspendedAt = ateapipb.ActorState_ACTOR_STATE_SUSPENDED, s.now()
		rec.snapshotN++
	}
	return &ateapipb.SuspendActorResponse{Actor: s.viewLocked(ctx, rec)}, s.saveLocked()
}

func (s *server) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	key := refOf(req.GetActor())
	// A paused task waits for a human, possibly for long. Saving /data lets a new pod continue it
	// if this pod is lost in the meantime; the pod itself stays, so the usual resume is instant.
	if err := s.saveData(ctx, key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	rec.wanted = ateapipb.ActorState_ACTOR_STATE_PAUSED
	return &ateapipb.PauseActorResponse{Actor: s.viewLocked(ctx, rec)}, s.saveLocked()
}

func (s *server) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	key := refOf(req.GetActor())
	s.mu.Lock()
	rec, ok := s.actors[key]
	if !ok {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	deleted := s.viewLocked(ctx, rec)
	podName, uid := rec.pod(), rec.actor.GetMetadata().GetUid()
	delete(s.actors, key)
	err := s.saveLocked()
	s.mu.Unlock()
	if perr := s.pods.Delete(ctx, key.atespace, podName); perr != nil {
		return nil, status.Errorf(codes.Internal, "delete pod: %v", perr)
	}
	if serr := s.cfg.Snapshots.delete(ctx, uid); serr != nil {
		slog.Warn("delete snapshot", "actor", key.String(), "error", serr)
	}
	deleted.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
	return deleted, err
}

func (s *server) CreateActorEgressPolicy(_ context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[refOf(req.GetActor())]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	if rec.policy != nil {
		return nil, status.Error(codes.AlreadyExists, "egress policy exists")
	}
	// The policy is stored for API compatibility only; Fargate offers no per-actor egress gateway.
	rec.policy = proto.Clone(req.GetEgressPolicy()).(*ateapipb.EgressPolicy)
	rec.policy.Metadata = meta(refOf(req.GetActor()))
	return proto.Clone(rec.policy).(*ateapipb.EgressPolicy), s.saveLocked()
}

func (s *server) GetActorEgressPolicy(_ context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[refOf(req.GetActor())]
	if !ok || rec.policy == nil {
		return nil, status.Error(codes.NotFound, "egress policy not found")
	}
	return proto.Clone(rec.policy).(*ateapipb.EgressPolicy), nil
}

// startPod makes sure a pod exists for the actor and remembers its name. With run set it
// also marks the actor RUNNING; the eager start at creation leaves the state alone.
func (s *server) startPod(ctx context.Context, key ref, run bool) error {
	s.mu.Lock()
	rec, ok := s.actors[key]
	if !ok {
		s.mu.Unlock()
		return status.Error(codes.NotFound, "actor not found")
	}
	template, ok := s.templates[refOf(rec.actor.GetActorTemplate())]
	if !ok {
		s.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "actor template is gone")
	}
	actor := proto.Clone(rec.actor).(*ateapipb.Actor)
	template = proto.Clone(template).(*ateapipb.ActorTemplate)
	if run && rec.wanted != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		rec.wanted = ateapipb.ActorState_ACTOR_STATE_RUNNING
		if err := s.saveLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Unlock()

	rec.startMu.Lock()
	defer rec.startMu.Unlock()
	s.mu.Lock()
	podName := rec.pod()
	s.mu.Unlock()
	if state, err := s.pods.State(ctx, key.atespace, podName); err == nil && state.Exists {
		return nil
	}
	name, err := s.pods.Start(ctx, startRequest{Actor: actor, Template: template, Snapshot: s.cfg.Snapshots.refFor(actor.GetMetadata().GetUid())})
	if err != nil {
		return status.Errorf(codes.Unavailable, "start pod: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actors[key] == rec && rec.podName != name {
		rec.podName = name
		return s.saveLocked()
	}
	return nil
}

func (s *server) eagerStart(key ref) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := s.startPod(ctx, key, false); err != nil {
		slog.Warn("eager start", "actor", key.String(), "error", err)
	}
}

// ensureRunning starts the actor's pod when needed and waits until it is ready or
// the timeout passes. The router calls it to resume suspended actors on demand.
func (s *server) ensureRunning(ctx context.Context, key ref, wait time.Duration) (podState, error) {
	if err := s.startPod(ctx, key, true); err != nil {
		return podState{}, err
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		s.mu.Lock()
		rec, ok := s.actors[key]
		podName := ""
		if ok {
			podName = rec.pod()
		}
		s.mu.Unlock()
		if !ok {
			return podState{}, status.Error(codes.NotFound, "actor not found")
		}
		state, err := s.pods.State(ctx, key.atespace, podName)
		if err == nil && state.Ready && state.IP != "" {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return podState{}, status.FromContextError(ctx.Err()).Err()
		case <-deadline.C:
			return podState{}, status.Error(codes.DeadlineExceeded, "pod is not ready yet")
		case <-tick.C:
		}
	}
}

// saveData asks the actor's snapshot sidecar to upload /data. It does nothing without a
// snapshot store or without a ready pod.
func (s *server) saveData(ctx context.Context, key ref) error {
	s.mu.Lock()
	rec, ok := s.actors[key]
	if !ok {
		s.mu.Unlock()
		return status.Error(codes.NotFound, "actor not found")
	}
	podName, uid := rec.pod(), rec.actor.GetMetadata().GetUid()
	s.mu.Unlock()
	snapshot := s.cfg.Snapshots.refFor(uid)
	if snapshot == nil {
		return nil
	}
	state, err := s.pods.State(ctx, key.atespace, podName)
	if err != nil || !state.Ready || state.IP == "" {
		return nil
	}
	if err := s.save(ctx, state.IP, *snapshot); err != nil {
		return status.Errorf(codes.Unavailable, "save /data: %v", err)
	}
	return nil
}

// snapshotLocked describes what a suspended actor resumes from. It is a marker: the pod may
// linger for the grace period, and a new pod restores /data from the store but not process
// memory. With a store the scope is DATA, which lets kagent checkpoint and fork the actor;
// without one the scope is FULL, which kagent refuses to fork.
func (s *server) snapshotLocked(rec *actorRecord) *ateapipb.ExternalSnapshot {
	scope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	if s.cfg.Snapshots.enabled() {
		scope = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	}
	templateUID := s.templates[refOf(rec.actor.GetActorTemplate())].GetMetadata().GetUid()
	if source := rec.actor.GetSourceTag(); source != nil && rec.snapshotN == 0 {
		if tag := s.tags[refOf(source)]; tag != nil {
			return proto.Clone(tag.GetStatus().GetSnapshot()).(*ateapipb.ExternalSnapshot)
		}
	}
	return &ateapipb.ExternalSnapshot{
		SnapshotUri:      fmt.Sprintf("fargate-shim://%s/%s/%d", rec.actor.GetMetadata().GetAtespace(), rec.actor.GetMetadata().GetUid(), rec.snapshotN),
		ContentScope:     scope,
		ActorTemplateUid: templateUID,
	}
}

func (s *server) viewLocked(ctx context.Context, rec *actorRecord) *ateapipb.Actor {
	view := proto.Clone(rec.actor).(*ateapipb.Actor)
	key := ref{rec.actor.GetMetadata().GetAtespace(), rec.actor.GetMetadata().GetName()}
	state := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	view.Status = &ateapipb.ActorStatus{}
	switch rec.wanted {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		pod, err := s.pods.State(ctx, key.atespace, rec.pod())
		switch {
		case err == nil && pod.Ready:
			state = ateapipb.ActorState_ACTOR_STATE_RUNNING
		case err == nil && pod.Exists:
			state = ateapipb.ActorState_ACTOR_STATE_RESUMING
		default:
			// A lost pod (node maintenance, OOM, deletion) is not a crash kagent can act on: it only
			// resumes SUSPENDED actors. Report SUSPENDED, so the next request starts a new pod
			// that restores the data of the last suspend.
			state = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			view.Status.ExternalSnapshot = s.snapshotLocked(rec)
		}
		view.Status.AssignedNode = pod.Node
	case ateapipb.ActorState_ACTOR_STATE_PAUSED:
		state = ateapipb.ActorState_ACTOR_STATE_PAUSED
	default:
		view.Status.ExternalSnapshot = s.snapshotLocked(rec)
	}
	view.Status.State = state
	return view
}

// reap deletes pods of suspended actors after the grace period and pods without an actor.
// Warm pods belong to the pool, which manages them.
func (s *server) reap(ctx context.Context) {
	s.mu.Lock()
	var expired []ref
	inUse := map[ref]bool{}
	for key, rec := range s.actors {
		pod := ref{key.atespace, rec.pod()}
		inUse[pod] = true
		if rec.wanted == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && s.now().Sub(rec.suspendedAt) >= s.cfg.SuspendGrace {
			expired = append(expired, pod)
		}
	}
	s.mu.Unlock()
	managed, err := s.pods.List(ctx)
	if err != nil {
		slog.Warn("list managed pods", "error", err)
		return
	}
	for _, m := range managed {
		if m.warmKey == "" && !inUse[m.ref] {
			expired = append(expired, m.ref)
		}
	}
	for _, pod := range expired {
		if state, err := s.pods.State(ctx, pod.atespace, pod.name); err == nil && state.Exists {
			slog.Info("deleting idle pod", "pod", pod.String())
			if err := s.pods.Delete(ctx, pod.atespace, pod.name); err != nil {
				slog.Warn("delete idle pod", "pod", pod.String(), "error", err)
			}
		}
	}
}

func (s *server) runReaper(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reap(ctx)
		}
	}
}

// hintExisting re-registers the templates of known actors with the warm pool after a restart.
func (s *server) hintExisting() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.actors {
		if template := s.templates[refOf(rec.actor.GetActorTemplate())]; template != nil {
			s.pods.Hint(template)
		}
	}
}

func (s *server) runMaintenance(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pods.Maintain(ctx)
		}
	}
}

type persisted struct {
	Atespaces []string                `json:"atespaces"`
	Templates map[string][]byte       `json:"templates"`
	Actors    map[string]persistedRec `json:"actors"`
	Tags      map[string][]byte       `json:"tags,omitempty"`
}

type persistedRec struct {
	Actor       []byte    `json:"actor"`
	Policy      []byte    `json:"policy,omitempty"`
	PodName     string    `json:"podName,omitempty"`
	Wanted      int32     `json:"wanted"`
	SnapshotN   int       `json:"snapshotN"`
	SuspendedAt time.Time `json:"suspendedAt"`
}

func (s *server) saveLocked() error {
	if s.persist == nil {
		return nil
	}
	state := persisted{Templates: map[string][]byte{}, Actors: map[string]persistedRec{}, Tags: map[string][]byte{}}
	for key, tag := range s.tags {
		raw, err := proto.Marshal(tag)
		if err != nil {
			return status.Errorf(codes.Internal, "marshal tag: %v", err)
		}
		state.Tags[key.String()] = raw
	}
	for name := range s.atespaces {
		state.Atespaces = append(state.Atespaces, name)
	}
	for key, t := range s.templates {
		raw, err := proto.Marshal(t)
		if err != nil {
			return status.Errorf(codes.Internal, "marshal template: %v", err)
		}
		state.Templates[key.String()] = raw
	}
	for key, rec := range s.actors {
		raw, err := proto.Marshal(rec.actor)
		if err != nil {
			return status.Errorf(codes.Internal, "marshal actor: %v", err)
		}
		out := persistedRec{Actor: raw, PodName: rec.podName, Wanted: int32(rec.wanted), SnapshotN: rec.snapshotN, SuspendedAt: rec.suspendedAt}
		if rec.policy != nil {
			if out.Policy, err = proto.Marshal(rec.policy); err != nil {
				return status.Errorf(codes.Internal, "marshal policy: %v", err)
			}
		}
		state.Actors[key.String()] = out
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal state: %v", err)
	}
	if err := s.persist(raw); err != nil {
		return status.Errorf(codes.Internal, "persist state: %v", err)
	}
	return nil
}

func (s *server) load(raw []byte) error {
	var state persisted
	if err := json.Unmarshal(raw, &state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range state.Atespaces {
		s.atespaces[name] = &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
	}
	for _, raw := range state.Templates {
		t := &ateapipb.ActorTemplate{}
		if err := proto.Unmarshal(raw, t); err != nil {
			return err
		}
		s.templates[ref{t.GetMetadata().GetAtespace(), t.GetMetadata().GetName()}] = t
	}
	for _, raw := range state.Tags {
		tag := &ateapipb.Tag{}
		if err := proto.Unmarshal(raw, tag); err != nil {
			return err
		}
		s.tags[ref{tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()}] = tag
	}
	for _, p := range state.Actors {
		actor := &ateapipb.Actor{}
		if err := proto.Unmarshal(p.Actor, actor); err != nil {
			return err
		}
		rec := &actorRecord{actor: actor, podName: p.PodName, wanted: ateapipb.ActorState(p.Wanted), snapshotN: p.SnapshotN, suspendedAt: p.SuspendedAt}
		if len(p.Policy) > 0 {
			rec.policy = &ateapipb.EgressPolicy{}
			if err := proto.Unmarshal(p.Policy, rec.policy); err != nil {
				return err
			}
		}
		s.actors[ref{actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName()}] = rec
	}
	return nil
}
