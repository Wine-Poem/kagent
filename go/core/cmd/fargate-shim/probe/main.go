// Command probe drives kagent's public gRPC and A2A APIs against a deployment, to check the
// behaviour of the Fargate shim: streaming, human in the loop, checkpoints and forks.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	"github.com/google/uuid"
	adka2a "github.com/kagent-dev/kagent/go/adk/pkg/a2a"
	kagenta2a "github.com/kagent-dev/kagent/go/api/a2a"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type client struct {
	ctx         context.Context
	a2a         a2apb.A2AServiceClient
	sessions    apiv1alpha1.SessionServiceClient
	checkpoints apiv1alpha1.CheckpointServiceClient
}

type session struct{ id, tenant string }

func main() {
	addr := flag.String("addr", "localhost:8083", "controller address")
	agent := flag.String("agent", "echo-agent", "agent name")
	n := flag.Int("n", 5, "bench: sessions in the sequential phase")
	conc := flag.Int("c", 8, "bench: sessions in the concurrent phase")
	settle := flag.Duration("settle", 8*time.Second, "bench: wait after the first message so the runtime can quiesce")
	flag.Parse()
	if flag.NArg() == 0 {
		fail("usage: probe [-addr host:port] [-agent name] stream|hitl|fork|isolation")
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "x-user-id", "probe"), 10*time.Minute)
	defer cancel()
	c := &client{
		ctx: ctx, a2a: a2apb.NewA2AServiceClient(conn), sessions: apiv1alpha1.NewSessionServiceClient(conn),
		checkpoints: apiv1alpha1.NewCheckpointServiceClient(conn),
	}
	switch flag.Arg(0) {
	case "stream":
		c.stream(*agent)
	case "hitl":
		c.hitl(*agent)
	case "fork":
		c.fork(*agent)
	case "isolation":
		c.isolation(*agent)
	case "cleanup":
		c.cleanup(*agent)
	case "bench":
		c.bench(*agent, *n, *conc, *settle)
	default:
		fail("unknown scenario " + flag.Arg(0))
	}
}

func (c *client) create(agent string) session {
	req := &apiv1alpha1.CreateSessionRequest{Agent: &apiv1alpha1.ResourceReference{Namespace: "kagent", Name: agent}, RequestId: uuid.NewString()}
	for {
		resp, err := c.sessions.CreateSession(c.ctx, req)
		if status.Code(err) == codes.FailedPrecondition {
			time.Sleep(time.Second)
			continue
		}
		check(err)
		s := resp.GetSession()
		return session{id: s.GetId(), tenant: s.GetAgent().GetNamespace() + "/" + s.GetAgent().GetName()}
	}
}

func (c *client) request(s session, text string) *a2apb.SendMessageRequest {
	message := a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart(text))
	message.ContextID = s.id
	req, err := pbconv.ToProtoSendMessageRequest(&a2atype.SendMessageRequest{Message: message})
	check(err)
	req.Tenant, req.Message.ContextId = s.tenant, s.id
	return req
}

// send follows the gateway's contract: retry only when it says the input was never accepted.
func (c *client) send(s session, text string) *a2atype.Task {
	req := c.request(s, text)
	for deadline := time.Now().Add(45 * time.Second); ; {
		resp, err := c.a2a.SendMessage(c.ctx, req)
		if err != nil && notAccepted(err) && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		check(err)
		result, err := pbconv.FromProtoSendMessageResponse(resp)
		check(err)
		task, ok := result.(*a2atype.Task)
		if !ok {
			fail(fmt.Sprintf("response is %T, want a task", result))
		}
		return task
	}
}

func notAccepted(err error) bool {
	if status.Code(err) != codes.FailedPrecondition {
		return false
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Metadata["reason"] == "KAGENT_SEND_NOT_ACCEPTED" {
			return true
		}
	}
	return false
}

func text(task *a2atype.Task) string {
	var parts []string
	if task.Status.Message != nil {
		for _, p := range task.Status.Message.Parts {
			parts = append(parts, p.Text())
		}
	}
	for _, a := range task.Artifacts {
		for _, p := range a.Parts {
			parts = append(parts, p.Text())
		}
	}
	return strings.Join(parts, " ")
}

func (c *client) stream(agent string) {
	s := c.create(agent)
	fmt.Println("session", s.id)
	start := time.Now()
	stream, err := c.a2a.SendStreamingMessage(c.ctx, c.request(s, "Stream this answer"))
	check(err)
	events, first := 0, time.Duration(0)
	var last string
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if events == 0 {
			first = time.Since(start)
		}
		events++
		event, err := pbconv.FromProtoStreamResponse(resp)
		check(err)
		last = fmt.Sprintf("%T", event)
		if u, ok := event.(*a2atype.TaskStatusUpdateEvent); ok {
			last = "status-update " + string(u.Status.State)
		}
		fmt.Printf("  event %d at %s: %s\n", events, time.Since(start).Round(time.Millisecond), last)
	}
	fmt.Printf("RESULT stream: %d events, first after %s, last %s, total %s\n", events, first.Round(time.Millisecond), last, time.Since(start).Round(time.Millisecond))
	if events < 2 {
		os.Exit(1)
	}
}

func (c *client) hitl(agent string) {
	ctx := metadata.AppendToOutgoingContext(c.ctx, strings.ToLower(a2atype.SvcParamExtensions), adka2a.HITLExtensionURI)
	c.ctx = ctx
	s := c.create(agent)
	fmt.Println("session", s.id)
	waiting := c.send(s, "Which database should we use for storage?")
	fmt.Println("state after the question:", waiting.Status.State)
	request := adka2a.GetAskUserRequest(waiting.Status.Message)
	if waiting.Status.State != a2atype.TaskStateInputRequired || request == nil {
		fail("RESULT hitl: the task did not pause with an ask_user request")
	}
	fmt.Println("paused with an ask_user request:", request.ID)
	time.Sleep(10 * time.Second)
	reply := adka2a.AttachHitlExtension(a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("PostgreSQL")), &kagenta2a.AskUserResponse{
		Type: adka2a.HITLTypeAskUserResponse, ID: request.ID, Answers: []kagenta2a.AskUserAnswer{{Answer: []string{"PostgreSQL"}}},
	})
	reply.TaskID, reply.ContextID = waiting.ID, waiting.ContextID
	start := time.Now()
	resp, err := a2agrpc.NewGRPCTransportFromClient(c.a2a).SendMessage(c.ctx, nil, &a2atype.SendMessageRequest{Tenant: s.tenant, Message: reply})
	check(err)
	done, ok := resp.(*a2atype.Task)
	if !ok || done.Status.State != a2atype.TaskStateCompleted || !strings.Contains(text(done), "Using PostgreSQL") {
		fail(fmt.Sprintf("RESULT hitl: resumed task = %#v", resp))
	}
	fmt.Printf("RESULT hitl: paused, resumed after the human answer in %s, completed with %q\n", time.Since(start).Round(time.Millisecond), text(done))
}

func (c *client) fork(agent string) {
	s := c.create(agent)
	fmt.Println("source session", s.id)
	first := c.send(s, "Remember the word ZEBRA-4711. First turn.")
	fmt.Println("turn 1:", text(first))
	var checkpoint *apiv1alpha1.Checkpoint
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); {
		resp, err := c.checkpoints.CreateCheckpoint(c.ctx, &apiv1alpha1.CreateCheckpointRequest{SessionId: s.id, RequestId: uuid.NewString(), ExpectedHeadTaskId: string(first.ID)})
		if pending(err) {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		check(err)
		checkpoint = resp.GetCheckpoint()
		break
	}
	if checkpoint == nil || checkpoint.GetState() != apiv1alpha1.CheckpointState_CHECKPOINT_STATE_READY {
		fail(fmt.Sprintf("RESULT fork: checkpoint = %v", checkpoint))
	}
	fmt.Println("checkpoint", checkpoint.GetId(), checkpoint.GetState())
	second := c.send(s, "Second turn, only in the source.")
	fmt.Println("source turn 2:", text(second))
	start := time.Now()
	forked, err := c.checkpoints.ForkSession(c.ctx, &apiv1alpha1.ForkSessionRequest{CheckpointId: checkpoint.GetId(), RequestId: uuid.NewString()})
	check(err)
	f := session{id: forked.GetSession().GetId(), tenant: s.tenant}
	fmt.Printf("fork session %s created in %s\n", f.id, time.Since(start).Round(time.Millisecond))
	got := c.send(f, "What do you remember? (fork)")
	fmt.Println("fork turn:", text(got))
	fmt.Println("RESULT fork: source turn 2 reply =", text(second), "| fork reply =", text(got))
	// The fork holds the history up to the checkpoint plus its own turn, exactly what the source
	// saw for its second turn; the source's second turn must not leak into the fork.
	if !strings.Contains(text(got), "ZEBRA-4711=true") || strings.TrimSpace(text(got)) != strings.TrimSpace(text(second)) {
		os.Exit(1)
	}
}

func pending(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "KAGENT_CHECKPOINT_SNAPSHOT_PENDING" {
			return true
		}
	}
	return false
}

func (c *client) isolation(agent string) {
	a, b := c.create(agent), c.create(agent)
	fmt.Println("sessions", a.id, b.id)
	fmt.Println("a:", text(c.send(a, "Remember the word ZEBRA-4711.")))
	fmt.Println("b:", text(c.send(b, "What do you remember?")))
	fmt.Println("a again:", text(c.send(a, "What do you remember?")))
}

func check(err error) {
	if err != nil {
		fail(err.Error())
	}
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

func report(name string, durations []time.Duration) {
	if len(durations) == 0 {
		return
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	fmt.Printf("BENCH %-34s n=%-2d min=%-8s median=%-8s max=%s\n", name, len(durations),
		durations[0].Round(time.Millisecond), durations[len(durations)/2].Round(time.Millisecond), durations[len(durations)-1].Round(time.Millisecond))
}

// bench measures the latencies a user feels: creating a session, the first message right after it,
// a message after the runtime had time to quiesce, and many sessions starting at once.
func (c *client) bench(agent string, n, conc int, settle time.Duration) {
	var create, first, later []time.Duration
	for i := 0; i < n; i++ {
		start := time.Now()
		s := c.create(agent)
		create = append(create, time.Since(start))
		start = time.Now()
		c.send(s, fmt.Sprintf("bench %d first message", i))
		first = append(first, time.Since(start))
		time.Sleep(settle)
		start = time.Now()
		c.send(s, fmt.Sprintf("bench %d message after %s", i, settle))
		later = append(later, time.Since(start))
	}
	report("create session", create)
	report("first message right after create", first)
	report("message after the runtime quiesced", later)

	type result struct{ total, firstMsg time.Duration }
	results := make([]result, conc)
	var wg sync.WaitGroup
	wall := time.Now()
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			s := c.create(agent)
			mid := time.Now()
			c.send(s, fmt.Sprintf("concurrent %d", i))
			results[i] = result{total: time.Since(start), firstMsg: time.Since(mid)}
		}(i)
	}
	wg.Wait()
	var totals, firsts []time.Duration
	for _, r := range results {
		totals = append(totals, r.total)
		firsts = append(firsts, r.firstMsg)
	}
	report(fmt.Sprintf("concurrent x%d create+first message", conc), totals)
	report(fmt.Sprintf("concurrent x%d first message only", conc), firsts)
	fmt.Printf("BENCH concurrent x%d wall clock until all answered: %s\n", conc, time.Since(wall).Round(time.Millisecond))
}

// cleanup deletes the probe's own sessions of one agent, so a test leaves no actors behind.
func (c *client) cleanup(agent string) {
	deleted := 0
	for {
		resp, err := c.sessions.ListSessions(c.ctx, &apiv1alpha1.ListSessionsRequest{
			Agent: &apiv1alpha1.ResourceReference{Namespace: "kagent", Name: agent}, Page: &apiv1alpha1.PageRequest{Limit: 100},
		})
		check(err)
		if len(resp.GetSessions()) == 0 {
			break
		}
		progress := false
		for _, session := range resp.GetSessions() {
			for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); {
				_, err := c.sessions.DeleteSession(c.ctx, &apiv1alpha1.DeleteSessionRequest{SessionId: session.GetId()})
				if status.Code(err) == codes.FailedPrecondition {
					time.Sleep(time.Second)
					continue
				}
				if err == nil || status.Code(err) == codes.NotFound {
					deleted++
					progress = true
				}
				break
			}
		}
		if !progress {
			break
		}
	}
	fmt.Printf("deleted %d sessions of agent %s\n", deleted, agent)
}
