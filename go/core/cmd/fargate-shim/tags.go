package main

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CreateTag retains a copy of a suspended actor's saved /data. kagent turns the tag into a
// checkpoint and later forks new actors from it. Only DATA snapshots can be tagged, because a
// new pod cannot restore process memory.
func (s *server) CreateTag(ctx context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error) {
	in := req.GetTag()
	key := ref{in.GetMetadata().GetAtespace(), in.GetMetadata().GetName()}
	source := refOf(in.GetSourceActor())
	if key.atespace == "" || key.name == "" || source.name == "" || in.GetScope() != ateapipb.TagScope_TAG_SCOPE_ATESPACE {
		return nil, status.Error(codes.InvalidArgument, "a tag needs a name, an atespace-scoped source actor")
	}
	if !s.cfg.Snapshots.enabled() {
		return nil, status.Error(codes.Unimplemented, "tags need the snapshot store")
	}
	s.mu.Lock()
	if _, ok := s.tags[key]; ok {
		s.mu.Unlock()
		return nil, status.Error(codes.AlreadyExists, "tag exists")
	}
	rec, ok := s.actors[source]
	if !ok {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "source actor not found")
	}
	view := s.viewLocked(ctx, rec)
	snapshot := view.GetStatus().GetExternalSnapshot()
	if view.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || snapshot.GetContentScope() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "only a suspended actor with a DATA snapshot can be tagged")
	}
	sourceUID := rec.actor.GetMetadata().GetUid()
	s.mu.Unlock()

	tagMeta := meta(key)
	if err := s.cfg.Snapshots.copy(ctx, sourceUID, tagMeta.GetUid()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "copy the actor's data: %v", err)
	}
	tag := &ateapipb.Tag{
		Metadata:    tagMeta,
		SourceActor: &ateapipb.ObjectRef{Atespace: source.atespace, Name: source.name},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.TagStatus{
			ActorTemplateUid: snapshot.GetActorTemplateUid(),
			Snapshot: &ateapipb.ExternalSnapshot{
				SnapshotUri:      fmt.Sprintf("fargate-shim-tag://%s/%s", key.atespace, tagMeta.GetUid()),
				ContentScope:     snapshot.GetContentScope(),
				ActorTemplateUid: snapshot.GetActorTemplateUid(),
			},
		},
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tags[key]; ok {
		return nil, status.Error(codes.AlreadyExists, "tag exists")
	}
	s.tags[key] = tag
	return proto.Clone(tag).(*ateapipb.Tag), s.saveLocked()
}

func (s *server) GetTag(_ context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tag, ok := s.tags[refOf(req.GetTag())]
	if !ok {
		return nil, status.Error(codes.NotFound, "tag not found")
	}
	return proto.Clone(tag).(*ateapipb.Tag), nil
}

func (s *server) DeleteTag(ctx context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error) {
	key := refOf(req.GetTag())
	s.mu.Lock()
	tag, ok := s.tags[key]
	if !ok {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "tag not found")
	}
	delete(s.tags, key)
	err := s.saveLocked()
	s.mu.Unlock()
	if derr := s.cfg.Snapshots.delete(ctx, tag.GetMetadata().GetUid()); derr != nil {
		return nil, status.Errorf(codes.Unavailable, "delete the tag's data: %v", derr)
	}
	return tag, err
}
