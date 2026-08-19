// Command orionctl is Orion's command-line interface.
//
// Two rules shape the output. Human output is a table sized to the terminal
// with no decoration that a pipe would have to strip. Machine output is
// -o json, which is the exact API response — not a reformatted subset — so a
// script never has to work around the CLI's opinion about which fields matter.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/sujalbistaa/orion/internal/version"
	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/client"
	"github.com/sujalbistaa/orion/pkg/store"
)

const usage = `orionctl - control an Orion cluster

Usage:
  orionctl [global flags] <command> [args]

Commands:
  cluster status                     cluster health, consensus state and capacity
  node list                          list nodes
  node describe NAME                 node detail, its workloads and recent events
  node cordon NAME                   stop scheduling new workloads onto a node
  node uncordon NAME                 allow scheduling again
  node drain NAME                    move every workload off a node
  node delete NAME                   remove a node from the cluster

  workload list                      list workloads
  workload describe NAME             workload detail, placement decision and events
  workload create FILE               create a workload from a JSON file ("-" for stdin)
  workload delete NAME               delete a workload
  workload logs NAME                 stream a workload's container logs

  deployment list                    list deployments
  deployment describe NAME           deployment detail, replicas and rollout history
  deployment create FILE             create a deployment from a JSON file
  deployment apply FILE              create or update a deployment
  deployment scale NAME N            change the replica count
  deployment rollback NAME [REV]     roll back to a previous revision
  deployment status NAME             watch a rollout until it completes
  deployment delete NAME             delete a deployment and its replicas

  service list                       list services and endpoint health
  service create FILE                create a service from a JSON file
  service delete NAME                delete a service

  events                             recent operational events
  fault list                         available fault injection experiments
  fault inject KIND [k=v ...]        run an experiment and report the result
  fault runs                         past experiment runs

Global flags:
  -server URL     Orion API address (default http://127.0.0.1:7070, or $ORION_ADDR)
  -token TOKEN    API token (or $ORION_API_TOKEN)
  -o FORMAT       output format: table (default) or json
  -timeout D      request timeout (default 30s)
`

type globals struct {
	server  string
	token   string
	output  string
	timeout time.Duration
}

func main() {
	var g globals
	flag.StringVar(&g.server, "server", env("ORION_ADDR", "http://127.0.0.1:7070"), "Orion API address")
	flag.StringVar(&g.token, "token", os.Getenv("ORION_API_TOKEN"), "API token")
	flag.StringVar(&g.output, "o", "table", "output format: table or json")
	flag.DurationVar(&g.timeout, "timeout", 30*time.Second, "request timeout")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Get().String())
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c := client.New(g.server, client.WithToken(g.token), client.WithTimeout(g.timeout))

	if err := dispatch(ctx, c, g, args); err != nil {
		fmt.Fprintln(os.Stderr, "error: "+humanError(err))
		os.Exit(1)
	}
}

// humanError turns an API failure into something an operator can act on. A
// bare "409 Conflict" tells nobody anything; the server's message explains what
// to do instead, and the leader hint is worth surfacing verbatim.
func humanError(err error) string {
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		return err.Error()
	}
	msg := apiErr.Error()
	switch {
	case apiErr.Status == 401:
		return msg + "\n\nSet ORION_API_TOKEN or pass -token."
	case apiErr.LeaderAddress != "":
		return msg + "\n\nThe leader is at " + apiErr.LeaderAddress + "."
	}
	return msg
}

func dispatch(ctx context.Context, c *client.Client, g globals, args []string) error {
	switch args[0] {
	case "cluster":
		return clusterCmd(ctx, c, g, args[1:])
	case "node":
		return nodeCmd(ctx, c, g, args[1:])
	case "workload":
		return workloadCmd(ctx, c, g, args[1:])
	case "deployment", "deploy":
		return deploymentCmd(ctx, c, g, args[1:])
	case "service", "svc":
		return serviceCmd(ctx, c, g, args[1:])
	case "events":
		return eventsCmd(ctx, c, g, args[1:])
	case "fault":
		return faultCmd(ctx, c, g, args[1:])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q; run `orionctl help`", args[0])
	}
}

// ---------------------------------------------------------------------------
// cluster
// ---------------------------------------------------------------------------

func clusterCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	if len(args) == 0 || args[0] != "status" {
		return errors.New("usage: orionctl cluster status")
	}
	resp, err := c.Cluster(ctx)
	if err != nil {
		return err
	}
	if g.output == "json" {
		return printJSON(resp)
	}

	cl, s := resp.Cluster, resp.Summary
	health := "healthy"
	switch {
	case !cl.QuorumHealthy:
		health = "DEGRADED — the control plane has no quorum"
	case s.Nodes.Unreachable > 0:
		health = fmt.Sprintf("degraded — %d node(s) unreachable", s.Nodes.Unreachable)
	case s.Deployments.Degraded > 0:
		health = fmt.Sprintf("degraded — %d deployment(s) not converging", s.Deployments.Degraded)
	}

	fmt.Printf("Cluster        %s\n", cl.ID)
	fmt.Printf("Health         %s\n", health)
	fmt.Printf("Leader         %s (term %d)\n", cl.LeaderID, cl.RaftTerm)
	fmt.Printf("Consensus      commit %d, applied %d, quorum %d of %d\n",
		cl.CommitIndex, cl.AppliedIndex, cl.Quorum, len(cl.ControlPlane))
	fmt.Println()

	fmt.Println("Nodes")
	printCounts(map[string]int{
		"Ready": s.Nodes.Ready, "Not ready": s.Nodes.NotReady,
		"Unreachable": s.Nodes.Unreachable, "Cordoned": s.Nodes.Cordoned,
	}, []string{"Ready", "Not ready", "Unreachable", "Cordoned"})

	fmt.Println("\nWorkloads")
	printCounts(map[string]int{
		"Running": s.Workloads.Running, "Starting": s.Workloads.Starting,
		"Pending": s.Workloads.Pending, "Failed": s.Workloads.Failed,
		"Unhealthy": s.Workloads.Unhealthy, "Restarts": s.Workloads.Restarts,
	}, []string{"Running", "Starting", "Pending", "Failed", "Unhealthy", "Restarts"})

	fmt.Println("\nResources")
	fmt.Printf("  %-14s %s / %s allocated (%s in use)\n", "CPU",
		s.Capacity.CPUAllocated, s.Capacity.CPUAllocatable, s.Capacity.CPUUsed)
	fmt.Printf("  %-14s %s / %s allocated (%s in use)\n", "Memory",
		s.Capacity.MemAllocated, s.Capacity.MemAllocatable, s.Capacity.MemUsed)

	if len(cl.ControlPlane) > 1 {
		fmt.Println("\nControl plane")
		for _, m := range cl.ControlPlane {
			reach := "reachable"
			if !m.Reachable {
				reach = "UNREACHABLE"
			}
			fmt.Printf("  %-14s %-10s %-12s %s\n", m.ID, m.Role, reach, m.Address)
		}
	}
	return nil
}

func printCounts(counts map[string]int, order []string) {
	for _, k := range order {
		fmt.Printf("  %-14s %d\n", k, counts[k])
	}
}

// ---------------------------------------------------------------------------
// node
// ---------------------------------------------------------------------------

func nodeCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: orionctl node <list|describe|cordon|uncordon|drain|delete>")
	}
	switch args[0] {
	case "list", "ls":
		nodes, err := c.ListNodes(ctx)
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(nodes)
		}
		if len(nodes) == 0 {
			fmt.Println("No nodes registered.")
			fmt.Println("\nStart one with: orion-agent --server 127.0.0.1:7071")
			return nil
		}
		w := table("NAME", "STATUS", "CPU", "MEMORY", "WORKLOADS", "LAST HEARTBEAT", "RUNTIME")
		for _, n := range nodes {
			status := string(n.Status.Phase)
			if n.Spec.Unschedulable {
				status += " (cordoned)"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
				n.Name, status,
				utilization(int64(n.Status.Allocated.CPU), int64(n.Status.Allocatable.CPU)),
				utilization(int64(n.Status.Allocated.Memory), int64(n.Status.Allocatable.Memory)),
				n.Status.WorkloadCount,
				ago(n.Status.LastHeartbeat),
				n.Status.Runtime.Name+" "+n.Status.Runtime.Version)
		}
		return w.Flush()

	case "describe":
		if len(args) < 2 {
			return errors.New("usage: orionctl node describe NAME")
		}
		detail, err := c.GetNode(ctx, args[1])
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(detail)
		}
		n := detail.Node
		fmt.Printf("Name           %s\n", n.Name)
		fmt.Printf("UID            %s\n", n.UID)
		fmt.Printf("Status         %s\n", n.Status.Phase)
		fmt.Printf("Address        %s\n", n.Spec.Address)
		fmt.Printf("Schedulable    %t\n", n.Schedulable())
		fmt.Printf("Runtime        %s %s (%s/%s)\n",
			n.Status.Runtime.Name, n.Status.Runtime.Version, n.Status.Runtime.OS, n.Status.Runtime.Arch)
		fmt.Printf("Last heartbeat %s\n", ago(n.Status.LastHeartbeat))
		fmt.Println()
		fmt.Printf("CPU            %s allocated of %s allocatable (%s capacity), %s in use\n",
			n.Status.Allocated.CPU, n.Status.Allocatable.CPU, n.Status.Capacity.CPU, n.Status.Usage.CPU)
		fmt.Printf("Memory         %s allocated of %s allocatable (%s capacity), %s in use\n",
			n.Status.Allocated.Memory, n.Status.Allocatable.Memory, n.Status.Capacity.Memory, n.Status.Usage.Memory)

		if len(n.Labels) > 0 {
			fmt.Println("\nLabels")
			for _, k := range sortedKeys(n.Labels) {
				fmt.Printf("  %s=%s\n", k, n.Labels[k])
			}
		}
		if len(n.Status.Conditions) > 0 {
			fmt.Println("\nConditions")
			for _, cond := range n.Status.Conditions {
				fmt.Printf("  %-16s %-6t %s %s\n", cond.Type, cond.Status, cond.Reason, cond.Message)
			}
		}

		fmt.Printf("\nWorkloads (%d)\n", len(detail.Workloads))
		if len(detail.Workloads) > 0 {
			w := table("  NAME", "PHASE", "HEALTH", "CPU", "MEMORY", "RESTARTS")
			for _, wl := range detail.Workloads {
				fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%d\n",
					wl.Name, wl.Status.Phase, wl.Status.Health,
					wl.Spec.Resources.Request.CPU, wl.Spec.Resources.Request.Memory, wl.Status.RestartCount)
			}
			w.Flush()
		}
		printEvents(detail.Events, 10)
		return nil

	case "cordon":
		if len(args) < 2 {
			return errors.New("usage: orionctl node cordon NAME")
		}
		if err := c.CordonNode(ctx, args[1], true); err != nil {
			return err
		}
		fmt.Printf("Node %s cordoned; no new workloads will be scheduled onto it.\n", args[1])
		return nil

	case "uncordon":
		if len(args) < 2 {
			return errors.New("usage: orionctl node uncordon NAME")
		}
		if err := c.CordonNode(ctx, args[1], false); err != nil {
			return err
		}
		fmt.Printf("Node %s uncordoned.\n", args[1])
		return nil

	case "drain":
		fs := flag.NewFlagSet("drain", flag.ContinueOnError)
		force := fs.Bool("force", false, "drain even if it would leave workloads nowhere to go")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return errors.New("usage: orionctl node drain NAME [-force]")
		}
		name := fs.Arg(0)
		if err := c.DrainNode(ctx, name, *force); err != nil {
			return err
		}
		fmt.Printf("Node %s is draining; its workloads will be rescheduled.\n", name)
		return nil

	case "delete", "rm":
		fs := flag.NewFlagSet("delete", flag.ContinueOnError)
		force := fs.Bool("force", false, "remove even if the node is Ready and running workloads")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return errors.New("usage: orionctl node delete NAME [-force]")
		}
		if err := c.DeleteNode(ctx, fs.Arg(0), *force); err != nil {
			return err
		}
		fmt.Printf("Node %s removed.\n", fs.Arg(0))
		return nil

	default:
		return fmt.Errorf("unknown node subcommand %q", args[0])
	}
}

// ---------------------------------------------------------------------------
// workload
// ---------------------------------------------------------------------------

func workloadCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: orionctl workload <list|describe|create|delete|logs>")
	}
	switch args[0] {
	case "list", "ls":
		fs := flag.NewFlagSet("list", flag.ContinueOnError)
		node := fs.String("node", "", "only workloads on this node")
		deployment := fs.String("deployment", "", "only workloads owned by this deployment")
		phase := fs.String("phase", "", "only workloads in this phase")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		workloads, err := c.ListWorkloads(ctx, client.WorkloadFilter{
			Node: *node, Deployment: *deployment, Phase: *phase,
		})
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(workloads)
		}
		if len(workloads) == 0 {
			fmt.Println("No workloads.")
			return nil
		}
		w := table("NAME", "PHASE", "HEALTH", "NODE", "CPU", "MEMORY", "RESTARTS", "AGE")
		for _, wl := range workloads {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
				wl.Name, wl.Status.Phase, wl.Status.Health, orDash(wl.Status.NodeName),
				wl.Spec.Resources.Request.CPU, wl.Spec.Resources.Request.Memory,
				wl.Status.RestartCount, ago(wl.CreatedAt))
		}
		return w.Flush()

	case "describe":
		if len(args) < 2 {
			return errors.New("usage: orionctl workload describe NAME")
		}
		detail, err := c.GetWorkload(ctx, args[1])
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(detail)
		}
		return describeWorkload(detail)

	case "create":
		if len(args) < 2 {
			return errors.New("usage: orionctl workload create FILE")
		}
		var wl v1.Workload
		if err := readSpec(args[1], &wl); err != nil {
			return err
		}
		created, err := c.CreateWorkload(ctx, &wl)
		if err != nil {
			return err
		}
		fmt.Printf("Workload %s created (uid %s). It will be scheduled shortly.\n", created.Name, created.UID)
		return nil

	case "delete", "rm":
		fs := flag.NewFlagSet("delete", flag.ContinueOnError)
		force := fs.Bool("force", false, "delete even if a deployment owns it (it will be recreated)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return errors.New("usage: orionctl workload delete NAME [-force]")
		}
		if err := c.DeleteWorkload(ctx, fs.Arg(0), *force); err != nil {
			return err
		}
		fmt.Printf("Workload %s deleted.\n", fs.Arg(0))
		return nil

	case "logs":
		fs := flag.NewFlagSet("logs", flag.ContinueOnError)
		tail := fs.Int("tail", 200, "lines to show from the end of the log")
		follow := fs.Bool("f", false, "stream new output as it arrives")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return errors.New("usage: orionctl workload logs NAME [-tail N] [-f]")
		}
		stream, err := c.WorkloadLogs(ctx, fs.Arg(0), *tail, *follow)
		if err != nil {
			return err
		}
		defer stream.Close()
		_, err = io.Copy(os.Stdout, stream)
		// A cancelled follow is the user pressing Ctrl-C, not a failure.
		if err != nil && ctx.Err() != nil {
			return nil
		}
		return err

	default:
		return fmt.Errorf("unknown workload subcommand %q", args[0])
	}
}

func describeWorkload(d *apiserver.WorkloadDetail) error {
	wl := d.Workload
	fmt.Printf("Name           %s\n", wl.Name)
	fmt.Printf("UID            %s\n", wl.UID)
	fmt.Printf("Phase          %s\n", wl.Status.Phase)
	fmt.Printf("Health         %s\n", wl.Status.Health)
	fmt.Printf("Node           %s\n", orDash(wl.Status.NodeName))
	fmt.Printf("Image          %s\n", wl.Spec.Image)
	fmt.Printf("Resources      request %s", wl.Spec.Resources.Request)
	if limit := wl.Spec.Resources.EffectiveLimit(); limit != wl.Spec.Resources.Request {
		fmt.Printf(", limit %s", limit)
	}
	fmt.Println()
	fmt.Printf("Restarts       %d\n", wl.Status.RestartCount)
	fmt.Printf("Created        %s\n", ago(wl.CreatedAt))
	if wl.Status.ContainerID != "" {
		fmt.Printf("Container      %s\n", shortID(wl.Status.ContainerID))
	}
	if wl.Status.ExitCode != nil {
		fmt.Printf("Exit code      %d\n", *wl.Status.ExitCode)
	}
	if wl.Status.Reason != "" {
		fmt.Printf("Reason         %s\n", wl.Status.Reason)
	}
	if wl.Status.Message != "" {
		fmt.Printf("Message        %s\n", wl.Status.Message)
	}
	if wl.OwnerRef != nil {
		fmt.Printf("Owner          %s/%s\n", wl.OwnerRef.Kind, wl.OwnerRef.Name)
	}

	if len(wl.Status.HostPorts) > 0 {
		fmt.Println("\nPorts")
		for _, container := range sortedInt32Keys(wl.Status.HostPorts) {
			fmt.Printf("  container %d -> host %d\n", container, wl.Status.HostPorts[container])
		}
	}

	// The placement decision is the answer to "why is this here?", which is the
	// question people actually ask about a scheduler.
	if p := wl.Status.Placement; p != nil {
		fmt.Println("\nPlacement")
		fmt.Printf("  %s\n", p.Reason)
		if p.LatencyMicros > 0 {
			fmt.Printf("  decided in %dµs\n", p.LatencyMicros)
		}
		if len(p.Candidates) > 0 {
			fmt.Println("\n  Candidates")
			for _, cand := range p.Candidates {
				parts := make([]string, 0, len(cand.Breakdown))
				for _, k := range sortedKeysInt32(cand.Breakdown) {
					parts = append(parts, fmt.Sprintf("%s=%d", k, cand.Breakdown[k]))
				}
				fmt.Printf("    %-16s score %-5d %s\n", cand.NodeName, cand.Score, strings.Join(parts, " "))
			}
		}
		if len(p.Rejections) > 0 {
			fmt.Println("\n  Rejected")
			for _, r := range p.Rejections {
				fmt.Printf("    %-16s %-14s %s\n", r.NodeName, r.Filter, r.Reason)
			}
		}
	}
	printEvents(d.Events, 15)
	return nil
}

// ---------------------------------------------------------------------------
// deployment
// ---------------------------------------------------------------------------

func deploymentCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: orionctl deployment <list|describe|create|apply|scale|rollback|status|delete>")
	}
	switch args[0] {
	case "list", "ls":
		deployments, err := c.ListDeployments(ctx)
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(deployments)
		}
		if len(deployments) == 0 {
			fmt.Println("No deployments.")
			return nil
		}
		w := table("NAME", "STATUS", "READY", "UPDATED", "IMAGE", "REV", "AGE")
		for _, d := range deployments {
			fmt.Fprintf(w, "%s\t%s\t%d/%d\t%d\t%s\t%d\t%s\n",
				d.Name, d.Status.Phase, d.Status.AvailableReplicas, d.Status.DesiredReplicas,
				d.Status.UpdatedReplicas, d.Spec.Template.Image, d.Status.Revision, ago(d.CreatedAt))
		}
		return w.Flush()

	case "describe":
		if len(args) < 2 {
			return errors.New("usage: orionctl deployment describe NAME")
		}
		detail, err := c.GetDeployment(ctx, args[1])
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(detail)
		}
		d := detail.Deployment
		fmt.Printf("Name           %s\n", d.Name)
		fmt.Printf("Status         %s\n", d.Status.Phase)
		fmt.Printf("Replicas       %d available, %d updated, %d current, %d desired\n",
			d.Status.AvailableReplicas, d.Status.UpdatedReplicas,
			d.Status.CurrentReplicas, d.Status.DesiredReplicas)
		if d.Status.UnschedulableReplicas > 0 {
			fmt.Printf("Unschedulable  %d replica(s) could not be placed\n", d.Status.UnschedulableReplicas)
		}
		fmt.Printf("Image          %s\n", d.Spec.Template.Image)
		fmt.Printf("Strategy       %s (maxSurge %d, maxUnavailable %d)\n",
			d.Spec.Strategy.Kind, d.Spec.Strategy.MaxSurge, d.Spec.Strategy.MaxUnavailable)
		fmt.Printf("Revision       %d\n", d.Status.Revision)

		for _, cond := range d.Status.Conditions {
			fmt.Printf("\n%-14s %s\n", cond.Type, cond.Message)
		}

		fmt.Printf("\nReplicas (%d)\n", len(detail.Workloads))
		if len(detail.Workloads) > 0 {
			w := table("  NAME", "PHASE", "HEALTH", "NODE", "RESTARTS", "AGE")
			for _, wl := range detail.Workloads {
				fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%d\t%s\n",
					wl.Name, wl.Status.Phase, wl.Status.Health, orDash(wl.Status.NodeName),
					wl.Status.RestartCount, ago(wl.CreatedAt))
			}
			w.Flush()
		}

		if len(detail.Revisions) > 0 {
			fmt.Println("\nRollout history")
			w := table("  REVISION", "IMAGE", "REPLICAS", "CREATED")
			for _, r := range detail.Revisions {
				fmt.Fprintf(w, "  %d\t%s\t%d\t%s\n", r.Revision, r.Template.Image, r.Replicas, ago(r.CreatedAt))
			}
			w.Flush()
		}
		printEvents(detail.Events, 15)
		return nil

	case "create", "apply":
		if len(args) < 2 {
			return fmt.Errorf("usage: orionctl deployment %s FILE", args[0])
		}
		var d v1.Deployment
		if err := readSpec(args[1], &d); err != nil {
			return err
		}
		created, err := c.CreateDeployment(ctx, &d)
		if err != nil {
			var apiErr *client.APIError
			// `apply` means create-or-update; `create` should fail loudly on a
			// name that already exists.
			if args[0] == "apply" && errors.As(err, &apiErr) && apiErr.Code == "already_exists" {
				updated, uerr := c.UpdateDeployment(ctx, &d)
				if uerr != nil {
					return uerr
				}
				fmt.Printf("Deployment %s updated (revision %d).\n", updated.Name, updated.Status.Revision)
				return nil
			}
			return err
		}
		fmt.Printf("Deployment %s created with %d replicas.\n", created.Name, created.Spec.Replicas)
		return nil

	case "scale":
		if len(args) < 3 {
			return errors.New("usage: orionctl deployment scale NAME REPLICAS")
		}
		n, err := strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("replicas must be a number, got %q", args[2])
		}
		if err := c.ScaleDeployment(ctx, args[1], int32(n)); err != nil {
			return err
		}
		fmt.Printf("Deployment %s scaled to %d replicas.\n", args[1], n)
		return nil

	case "rollback":
		if len(args) < 2 {
			return errors.New("usage: orionctl deployment rollback NAME [REVISION]")
		}
		var revision int64
		if len(args) > 2 {
			v, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("revision must be a number, got %q", args[2])
			}
			revision = v
		}
		d, err := c.RollbackDeployment(ctx, args[1], revision)
		if err != nil {
			return err
		}
		fmt.Printf("Deployment %s rolling back to %s (revision %d).\n",
			d.Name, d.Spec.Template.Image, d.Status.Revision)
		return nil

	case "status":
		if len(args) < 2 {
			return errors.New("usage: orionctl deployment status NAME")
		}
		return watchRollout(ctx, c, args[1])

	case "delete", "rm":
		if len(args) < 2 {
			return errors.New("usage: orionctl deployment delete NAME")
		}
		if err := c.DeleteDeployment(ctx, args[1]); err != nil {
			return err
		}
		fmt.Printf("Deployment %s deleted; its replicas are terminating.\n", args[1])
		return nil

	default:
		return fmt.Errorf("unknown deployment subcommand %q", args[0])
	}
}

// watchRollout follows a deployment until it settles, so `deployment status`
// can be used in a script as a gate rather than requiring a sleep.
func watchRollout(ctx context.Context, c *client.Client, name string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	last := ""
	for {
		detail, err := c.GetDeployment(ctx, name)
		if err != nil {
			return err
		}
		d := detail.Deployment
		line := fmt.Sprintf("%s: %d/%d replicas available, %d updated (%s)",
			d.Name, d.Status.AvailableReplicas, d.Status.DesiredReplicas,
			d.Status.UpdatedReplicas, d.Status.Phase)
		if line != last {
			fmt.Println(line)
			last = line
		}

		switch d.Status.Phase {
		case v1.DeploymentAvailable:
			fmt.Println("Rollout complete.")
			return nil
		case v1.DeploymentDegraded:
			if d.Status.UnschedulableReplicas > 0 {
				return fmt.Errorf("rollout stalled: %d replica(s) cannot be scheduled. "+
					"Run `orionctl workload list -deployment %s` and describe a Pending one to see why",
					d.Status.UnschedulableReplicas, name)
			}
			return errors.New("rollout is degraded; see `orionctl deployment describe " + name + "`")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// service, events, faults
// ---------------------------------------------------------------------------

func serviceCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: orionctl service <list|create|delete>")
	}
	switch args[0] {
	case "list", "ls":
		services, err := c.ListServices(ctx)
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(services)
		}
		if len(services) == 0 {
			fmt.Println("No services.")
			return nil
		}
		w := table("NAME", "PORT", "TARGET", "STRATEGY", "ENDPOINTS", "SELECTOR")
		for _, s := range services {
			endpoints := fmt.Sprintf("%d/%d healthy", s.Status.HealthyEndpoints, s.Status.TotalEndpoints)
			if s.Status.TotalEndpoints == 0 {
				endpoints = "none"
			}
			fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%s\n",
				s.Name, s.Spec.Port, s.Spec.TargetPort, s.Spec.Strategy, endpoints, formatSelector(s.Spec.Selector))
		}
		return w.Flush()

	case "create":
		if len(args) < 2 {
			return errors.New("usage: orionctl service create FILE")
		}
		var svc v1.Service
		if err := readSpec(args[1], &svc); err != nil {
			return err
		}
		created, err := c.CreateService(ctx, &svc)
		if err != nil {
			return err
		}
		fmt.Printf("Service %s created on port %d.\n", created.Name, created.Spec.Port)
		return nil

	case "delete", "rm":
		if len(args) < 2 {
			return errors.New("usage: orionctl service delete NAME")
		}
		if err := c.DeleteService(ctx, args[1]); err != nil {
			return err
		}
		fmt.Printf("Service %s deleted.\n", args[1])
		return nil

	default:
		return fmt.Errorf("unknown service subcommand %q", args[0])
	}
}

func eventsCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	kind := fs.String("kind", "", "filter by object kind: Node, Workload, Deployment, Service")
	name := fs.String("name", "", "filter by object name")
	severity := fs.String("severity", "", "filter by severity: Info, Warning, Critical")
	limit := fs.Int("limit", 50, "maximum events to show")
	if err := fs.Parse(args); err != nil {
		return err
	}

	events, err := c.ListEvents(ctx, store.EventQuery{
		Kind: *kind, Name: *name, Severity: v1.EventSeverity(*severity), Limit: *limit,
	})
	if err != nil {
		return err
	}
	if g.output == "json" {
		return printJSON(events)
	}
	if len(events) == 0 {
		fmt.Println("No events.")
		return nil
	}
	w := table("TIME", "SEVERITY", "REASON", "OBJECT", "MESSAGE")
	for _, e := range events {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s/%s\t%s\n",
			ago(e.Timestamp), e.Severity, e.Reason, e.Kind, e.Name, e.Message)
	}
	return w.Flush()
}

func faultCmd(ctx context.Context, c *client.Client, g globals, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: orionctl fault <list|inject|runs>")
	}
	switch args[0] {
	case "list", "ls":
		experiments, err := c.ListExperiments(ctx)
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(experiments)
		}
		for _, e := range experiments {
			fmt.Printf("%s (%s)\n", e.Name, e.Kind)
			fmt.Printf("  %s\n", e.Description)
			fmt.Printf("  Hypothesis: %s\n", e.Hypothesis)
			if len(e.Parameters) > 0 {
				fmt.Println("  Parameters:")
				for _, p := range e.Parameters {
					req := ""
					if p.Required {
						req = " (required)"
					}
					fmt.Printf("    %s%s  %s\n", p.Name, req, p.Help)
				}
			}
			fmt.Println()
		}
		return nil

	case "inject":
		if len(args) < 2 {
			return errors.New("usage: orionctl fault inject KIND [key=value ...]")
		}
		params := map[string]string{}
		for _, kv := range args[2:] {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("parameter %q must be in key=value form", kv)
			}
			params[k] = v
		}
		run, err := c.StartExperiment(ctx, apiserver.ExperimentRequest{
			Kind: apiserver.ExperimentKind(args[1]), Params: params,
		})
		if err != nil {
			return err
		}
		fmt.Printf("Experiment %s started.\n\n", run.ID)
		return followRun(ctx, c, run.ID, g)

	case "runs":
		runs, err := c.ListRuns(ctx)
		if err != nil {
			return err
		}
		if g.output == "json" {
			return printJSON(runs)
		}
		if len(runs) == 0 {
			fmt.Println("No experiment runs.")
			return nil
		}
		w := table("ID", "KIND", "STATE", "RECOVERY", "STARTED", "ACTOR")
		for _, r := range runs {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				r.ID, r.Kind, r.State, orDash(r.RecoveryDuration), ago(r.StartedAt), orDash(r.Actor))
		}
		return w.Flush()

	default:
		return fmt.Errorf("unknown fault subcommand %q", args[0])
	}
}

// followRun prints an experiment's timeline as it happens, then its verdict.
func followRun(ctx context.Context, c *client.Client, id string, g globals) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	printed := 0
	for {
		run, err := c.GetRun(ctx, id)
		if err != nil {
			return err
		}
		for _, entry := range run.Timeline[printed:] {
			fmt.Printf("%8s  %-11s %s\n", entry.Elapsed, entry.Phase, entry.Message)
		}
		printed = len(run.Timeline)

		switch run.State {
		case apiserver.RunSucceeded, apiserver.RunFailed, apiserver.RunAborted:
			if g.output == "json" {
				return printJSON(run)
			}
			fmt.Println()
			fmt.Printf("Result: %s", run.State)
			if run.RecoveryDuration != "" {
				fmt.Printf(" (recovered in %s)", run.RecoveryDuration)
			}
			fmt.Println()
			if run.Error != "" {
				fmt.Printf("Error: %s\n", run.Error)
			}
			if len(run.Invariants) > 0 {
				fmt.Println("\nInvariants")
				for _, inv := range run.Invariants {
					mark := "held"
					if !inv.Held {
						mark = fmt.Sprintf("VIOLATED (%d times)", inv.Violations)
					}
					fmt.Printf("  %-32s %s\n", inv.Name, mark)
					if !inv.Held {
						fmt.Printf("    %s\n", inv.Detail)
					}
				}
			}
			if run.State == apiserver.RunFailed {
				return errors.New("the experiment failed; see the invariants above")
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

func table(headers ...string) *tabwriter.Writer {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 3, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	return w
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printEvents(events []v1.Event, limit int) {
	if len(events) == 0 {
		return
	}
	if len(events) > limit {
		events = events[:limit]
	}
	fmt.Println("\nEvents")
	w := table("  TIME", "SEVERITY", "REASON", "MESSAGE")
	for _, e := range events {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", ago(e.Timestamp), e.Severity, e.Reason, e.Message)
	}
	w.Flush()
}

func readSpec(path string, out any) error {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	// Match the server: a misspelled field must fail here rather than being
	// silently dropped and leaving the user wondering why nothing happened.
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}
	return nil
}

// ago renders a timestamp as an age, which is what an operator scanning a table
// actually wants.
func ago(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	switch {
	case d < 0:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func utilization(used, total int64) string {
	if total <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d%%", used*100/total)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func formatSelector(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(m))
	for _, k := range sortedKeys(m) {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysInt32(m map[string]int32) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedInt32Keys(m map[int32]int32) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
