package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	"github.com/google/uuid"
	adka2a "github.com/kagent-dev/kagent/go/adk/pkg/a2a"
	kagenta2a "github.com/kagent-dev/kagent/go/api/a2a"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// perfConfig selects the agents and the repetitions of the perf scenarios. The same binary
// runs against a native Substrate install and against the Fargate shim, so both sides are
// measured by identical client code from inside their clusters.
type perfConfig struct {
	agent, hitlAgent, mcpAgent, sharedAgent string
	n                                       int
	concurrency                             []int
	settle                                  time.Duration
	scenarios                               map[string]bool
}

type recorder struct {
	mu      sync.Mutex
	samples map[string][]time.Duration
	notes   map[string]string
}

func (r *recorder) add(name string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples[name] = append(r.samples[name], d)
}

func (r *recorder) note(name, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notes[name] = text
}

func timed(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Since(start)
}

func (c *client) perf(cfg perfConfig) {
	rec := &recorder{samples: map[string][]time.Duration{}, notes: map[string]string{}}
	if cfg.scenarios["session"] {
		c.perfSession(cfg, rec)
	}
	if cfg.scenarios["stream"] {
		c.perfStream(cfg, rec)
	}
	if cfg.scenarios["hitl"] && cfg.hitlAgent != "" {
		c.perfHITL(cfg, rec)
	}
	if cfg.scenarios["fork"] {
		c.perfFork(cfg, rec)
	}
	if cfg.scenarios["mcp"] && cfg.mcpAgent != "" {
		c.perfPrompt(cfg, rec, "mcp", cfg.mcpAgent, "Please add 3 and 5 using the configured MCP server.")
	}
	if cfg.scenarios["shared"] && cfg.sharedAgent != "" {
		c.perfPrompt(cfg, rec, "subagent", cfg.sharedAgent, "Ask the specialist")
	}
	if cfg.scenarios["concurrency"] {
		for _, n := range cfg.concurrency {
			c.perfConcurrency(cfg, rec, n)
		}
	}
	rec.print()
}

func (c *client) perfSession(cfg perfConfig, rec *recorder) {
	for i := 0; i < cfg.n; i++ {
		var s session
		rec.add("session.create", timed(func() { s = c.create(cfg.agent) }))
		rec.add("session.first_message", timed(func() { c.send(s, fmt.Sprintf("perf %d first", i)) }))
		rec.add("session.warm_message", timed(func() { c.send(s, fmt.Sprintf("perf %d warm", i)) }))
		time.Sleep(cfg.settle)
		rec.add("session.after_quiesce", timed(func() { c.send(s, fmt.Sprintf("perf %d after %s", i, cfg.settle)) }))
	}
}

func (c *client) perfStream(cfg perfConfig, rec *recorder) {
	s := c.create(cfg.agent)
	c.send(s, "warm up the stream session")
	for i := 0; i < cfg.n; i++ {
		start := time.Now()
		stream, err := c.a2a.SendStreamingMessage(c.ctx, c.request(s, fmt.Sprintf("stream %d", i)))
		check(err)
		first := time.Duration(0)
		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}
			if first == 0 {
				first = time.Since(start)
			}
			if _, err := pbconv.FromProtoStreamResponse(resp); err != nil {
				break
			}
		}
		rec.add("stream.first_event", first)
		rec.add("stream.total", time.Since(start))
		time.Sleep(time.Second)
	}
}

func (c *client) perfHITL(cfg perfConfig, rec *recorder) {
	h := *c
	h.ctx = metadata.AppendToOutgoingContext(c.ctx, strings.ToLower(a2atype.SvcParamExtensions), adka2a.HITLExtensionURI)
	for i := 0; i < cfg.n; i++ {
		s := h.create(cfg.hitlAgent)
		var waiting *a2atype.Task
		rec.add("hitl.question", timed(func() { waiting = h.send(s, "Which database should we use for storage?") }))
		request := adka2a.GetAskUserRequest(waiting.Status.Message)
		if waiting.Status.State != a2atype.TaskStateInputRequired || request == nil {
			rec.note("hitl", "the task did not pause with an ask_user request")
			return
		}
		// Long enough for the runtime to quiesce, so the answer exercises the resume path.
		time.Sleep(cfg.settle)
		reply := adka2a.AttachHitlExtension(a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("PostgreSQL")), &kagenta2a.AskUserResponse{
			Type: adka2a.HITLTypeAskUserResponse, ID: request.ID, Answers: []kagenta2a.AskUserAnswer{{Answer: []string{"PostgreSQL"}}},
		})
		reply.TaskID, reply.ContextID = waiting.ID, waiting.ContextID
		var resp a2atype.SendMessageResult
		var err error
		rec.add("hitl.resume_after_answer", timed(func() {
			resp, err = a2agrpc.NewGRPCTransportFromClient(h.a2a).SendMessage(h.ctx, nil, &a2atype.SendMessageRequest{Tenant: s.tenant, Message: reply})
		}))
		if done, ok := resp.(*a2atype.Task); err != nil || !ok || done.Status.State != a2atype.TaskStateCompleted {
			rec.note("hitl", fmt.Sprintf("resume failed: %v", err))
		}
	}
}

func (c *client) perfFork(cfg perfConfig, rec *recorder) {
	for i := 0; i < cfg.n; i++ {
		s := c.create(cfg.agent)
		first := c.send(s, "Remember the word ZEBRA-4711.")
		var checkpoint *apiv1alpha1.Checkpoint
		rec.add("fork.checkpoint", timed(func() {
			for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); {
				resp, err := c.checkpoints.CreateCheckpoint(c.ctx, &apiv1alpha1.CreateCheckpointRequest{SessionId: s.id, RequestId: uuid.NewString(), ExpectedHeadTaskId: string(first.ID)})
				if pending(err) {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				check(err)
				checkpoint = resp.GetCheckpoint()
				return
			}
		}))
		var forked *apiv1alpha1.ForkSessionResponse
		var err error
		d := timed(func() {
			forked, err = c.checkpoints.ForkSession(c.ctx, &apiv1alpha1.ForkSessionRequest{CheckpointId: checkpoint.GetId(), RequestId: uuid.NewString()})
		})
		if status.Code(err) == codes.FailedPrecondition {
			rec.note("fork", "rejected: "+status.Convert(err).Message())
			continue
		}
		check(err)
		rec.add("fork.create", d)
		f := session{id: forked.GetSession().GetId(), tenant: s.tenant}
		rec.add("fork.first_message", timed(func() { c.send(f, "What do you remember?") }))
	}
}

func (c *client) perfPrompt(cfg perfConfig, rec *recorder, name, agent, prompt string) {
	for i := 0; i < cfg.n; i++ {
		s := c.create(agent)
		rec.add(name+".first_message", timed(func() { c.send(s, prompt) }))
		rec.add(name+".warm_message", timed(func() { c.send(s, prompt) }))
	}
}

func (c *client) perfConcurrency(cfg perfConfig, rec *recorder, n int) {
	var wg sync.WaitGroup
	wall := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			s := c.create(cfg.agent)
			c.send(s, fmt.Sprintf("concurrent %d of %d", i, n))
			rec.add(fmt.Sprintf("concurrency.x%d.create_and_first_message", n), time.Since(start))
		}(i)
	}
	wg.Wait()
	rec.add(fmt.Sprintf("concurrency.x%d.wall_clock", n), time.Since(wall))
}

type stats struct {
	N                          int
	Min, P50, P90, Max, MeanMs float64
	SamplesMs                  []float64
}

func summarize(samples []time.Duration) stats {
	ms := make([]float64, len(samples))
	sum := 0.0
	for i, d := range samples {
		ms[i] = float64(d.Microseconds()) / 1000
		sum += ms[i]
	}
	sorted := append([]float64(nil), ms...)
	sort.Float64s(sorted)
	pick := func(q float64) float64 { return sorted[int(math.Ceil(q*float64(len(sorted))))-1] }
	return stats{N: len(ms), Min: sorted[0], P50: pick(0.5), P90: pick(0.9), Max: sorted[len(sorted)-1], MeanMs: sum / float64(len(ms)), SamplesMs: ms}
}

func (r *recorder) print() {
	names := make([]string, 0, len(r.samples))
	for name := range r.samples {
		names = append(names, name)
	}
	sort.Strings(names)
	out := map[string]any{}
	for _, name := range names {
		s := summarize(r.samples[name])
		out[name] = s
		fmt.Printf("PERF %-45s n=%-3d min=%9.1fms p50=%9.1fms p90=%9.1fms max=%9.1fms\n", name, s.N, s.Min, s.P50, s.P90, s.Max)
	}
	for name, text := range r.notes {
		fmt.Printf("NOTE %s: %s\n", name, text)
		out["note."+name] = text
	}
	raw, _ := json.Marshal(out)
	fmt.Printf("PERFJSON %s\n", raw)
}
