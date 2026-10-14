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
	Ensure(ctx context.Context, actor *ateapipb.Actor, template *ateapipb.ActorTemplate) error
	State(ctx context.Context, atespace, name string) (podState, error)
	Delete(ctx context.Context, atespace, name string) error
	ListManaged(ctx context.Context) ([]ref, error)
}

type config struct {
	SuspendGrace time.Duration
	ResumeWait   time.Duration
	RouterWait   time.Duration
}

type actorRecord struct {
	actor       *ateapipb.Actor
	policy      *ateapipb.EgressPolicy
	wanted      ateapipb.ActorState
	snapshotN   int
	suspendedAt time.Time
}

// server implements the subset of the ate-api Control service that kagent uses.
// Every Actor is a Fargate pod; suspend keeps the pod for a grace period.
type server struct {
	ateapipb.UnimplementedControlServer
	mu        sync.Mutex
	atespaces map[string]*ateapipb.Atespace
	templates map[ref]*ateapipb.ActorTemplate
	actors    map[ref]*actorRecord
	pods      podManager
	persist   func([]byte) error
	cfg       config
	now       func() time.Time
}

func newServer(pods podManager, persist func([]byte) error, cfg config) *server {
	return &server{
		atespaces: map[string]*ateapipb.Atespace{},
		templates: map[ref]*ateapipb.ActorTemplate{},
		actors:    map[ref]*actorRecord{},
		pods:      pods, persist: persist, cfg: cfg, now: time.Now,
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

func (s *server) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	actor := req.GetActor()
	key := ref{actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName()}
	if key.atespace == "" || key.name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor needs an atespace and a name")
	}
	if actor.GetSourceTag() != nil {
		return nil, status.Error(codes.Unimplemented, "restoring an actor from a tag needs snapshots, which Fargate does not provide")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.atespaces[key.atespace]; !ok {
		return nil, status.Error(codes.FailedPrecondition, "atespace does not exist")
	}
	template, ok := s.templates[refOf(actor.GetActorTemplate())]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor template not found")
	}
	if _, ok := s.actors[key]; ok {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	stored := &ateapipb.Actor{
		Metadata:       meta(key),
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: actor.GetActorTemplate().GetAtespace(), Name: actor.GetActorTemplate().GetName()},
		WorkerSelector: template.GetWorkerSelector(),
	}
	rec := &actorRecord{actor: stored, wanted: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, suspendedAt: s.now()}
	s.actors[key] = rec
	if err := s.saveLocked(); err != nil {
		return nil, err
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
	return &ateapipb.ResumeActorResponse{Actor: s.viewLocked(ctx, rec), Resumed: true}, nil
}

func (s *server) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[refOf(req.GetActor())]
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
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.actors[refOf(req.GetActor())]
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
	delete(s.actors, key)
	err := s.saveLocked()
	s.mu.Unlock()
	if perr := s.pods.Delete(ctx, key.atespace, key.name); perr != nil {
		return nil, status.Errorf(codes.Internal, "delete pod: %v", perr)
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

// ensureRunning starts the actor's pod when needed and waits until it is ready or
// the timeout passes. The router calls it to resume suspended actors on demand.
func (s *server) ensureRunning(ctx context.Context, key ref, wait time.Duration) (podState, error) {
	s.mu.Lock()
	rec, ok := s.actors[key]
	if !ok {
		s.mu.Unlock()
		return podState{}, status.Error(codes.NotFound, "actor not found")
	}
	template, ok := s.templates[refOf(rec.actor.GetActorTemplate())]
	if !ok {
		s.mu.Unlock()
		return podState{}, status.Error(codes.FailedPrecondition, "actor template is gone")
	}
	actor := proto.Clone(rec.actor).(*ateapipb.Actor)
	template = proto.Clone(template).(*ateapipb.ActorTemplate)
	if rec.wanted != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		rec.wanted = ateapipb.ActorState_ACTOR_STATE_RUNNING
		if err := s.saveLocked(); err != nil {
			s.mu.Unlock()
			return podState{}, err
		}
	}
	s.mu.Unlock()

	if err := s.pods.Ensure(ctx, actor, template); err != nil {
		return podState{}, status.Errorf(codes.Unavailable, "start pod: %v", err)
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		state, err := s.pods.State(ctx, key.atespace, key.name)
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

func (s *server) viewLocked(ctx context.Context, rec *actorRecord) *ateapipb.Actor {
	view := proto.Clone(rec.actor).(*ateapipb.Actor)
	key := ref{rec.actor.GetMetadata().GetAtespace(), rec.actor.GetMetadata().GetName()}
	state := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	view.Status = &ateapipb.ActorStatus{}
	switch rec.wanted {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		pod, err := s.pods.State(ctx, key.atespace, key.name)
		switch {
		case err == nil && pod.Ready:
			state = ateapipb.ActorState_ACTOR_STATE_RUNNING
		case err == nil && pod.Exists:
			state = ateapipb.ActorState_ACTOR_STATE_RESUMING
		default:
			state = ateapipb.ActorState_ACTOR_STATE_CRASHED
			view.Status.Crash = &ateapipb.ActorCrash{Message: "pod is gone", CrashTime: timestamppb.New(s.now())}
		}
		view.Status.AssignedNode = pod.Node
	case ateapipb.ActorState_ACTOR_STATE_PAUSED:
		state = ateapipb.ActorState_ACTOR_STATE_PAUSED
	default:
		// The snapshot is only a marker: the pod may linger for the grace period and a new
		// pod starts fresh afterwards, so process memory and /data are not preserved.
		view.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
			SnapshotUri:      fmt.Sprintf("fargate-shim://%s/%s/%d", key.atespace, rec.actor.GetMetadata().GetUid(), rec.snapshotN),
			ContentScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			ActorTemplateUid: s.templates[refOf(rec.actor.GetActorTemplate())].GetMetadata().GetUid(),
		}
	}
	view.Status.State = state
	return view
}

// reap deletes pods of suspended actors after the grace period and pods without an actor.
func (s *server) reap(ctx context.Context) {
	s.mu.Lock()
	var expired []ref
	known := map[ref]bool{}
	for key, rec := range s.actors {
		known[key] = true
		if rec.wanted == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && s.now().Sub(rec.suspendedAt) >= s.cfg.SuspendGrace {
			expired = append(expired, key)
		}
	}
	s.mu.Unlock()
	managed, err := s.pods.ListManaged(ctx)
	if err != nil {
		slog.Warn("list managed pods", "error", err)
		return
	}
	for _, key := range managed {
		if !known[key] {
			expired = append(expired, key)
		}
	}
	for _, key := range expired {
		if state, err := s.pods.State(ctx, key.atespace, key.name); err == nil && state.Exists {
			slog.Info("deleting idle pod", "actor", key.String())
			if err := s.pods.Delete(ctx, key.atespace, key.name); err != nil {
				slog.Warn("delete idle pod", "actor", key.String(), "error", err)
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

type persisted struct {
	Atespaces []string                `json:"atespaces"`
	Templates map[string][]byte       `json:"templates"`
	Actors    map[string]persistedRec `json:"actors"`
}

type persistedRec struct {
	Actor       []byte    `json:"actor"`
	Policy      []byte    `json:"policy,omitempty"`
	Wanted      int32     `json:"wanted"`
	SnapshotN   int       `json:"snapshotN"`
	SuspendedAt time.Time `json:"suspendedAt"`
}

func (s *server) saveLocked() error {
	if s.persist == nil {
		return nil
	}
	state := persisted{Templates: map[string][]byte{}, Actors: map[string]persistedRec{}}
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
		out := persistedRec{Actor: raw, Wanted: int32(rec.wanted), SnapshotN: rec.snapshotN, SuspendedAt: rec.suspendedAt}
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
	for _, p := range state.Actors {
		actor := &ateapipb.Actor{}
		if err := proto.Unmarshal(p.Actor, actor); err != nil {
			return err
		}
		rec := &actorRecord{actor: actor, wanted: ateapipb.ActorState(p.Wanted), snapshotN: p.SnapshotN, suspendedAt: p.SuspendedAt}
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
