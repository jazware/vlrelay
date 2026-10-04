// gocheck: indigo's firehose consumer (events.HandleRepoStream) against a
// relay, with an optional sync 1.1 verifier on top built from indigo's
// atproto/repo: signature, MST inversion, and per-DID chain continuity
// (prevData equals the last commit's data, rev increases).
//
//	gocheck --url ws://127.0.0.1:3480 [--cursor N] --secs 30 [--verify --plc http://127.0.0.1:3482]
//
// Prints one JSON report: frames by kind, #info and error frames, seq order,
// how the stream ended, and verifier failures by reason with samples.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
)

type chain struct {
	rev  string
	data string
}

type report struct {
	URL         string            `json:"url"`
	Cursor      *int64            `json:"cursor,omitempty"`
	Frames      map[string]int    `json:"frames"`
	Infos       []map[string]any  `json:"infos"`
	Errors      []map[string]any  `json:"errorFrames"`
	FirstSeq    int64             `json:"firstSeq"`
	LastSeq     int64             `json:"lastSeq"`
	SeqBackward int               `json:"seqBackward"`
	Ended       string            `json:"ended"`
	Verify      map[string]int    `json:"verify,omitempty"`
	Samples     map[string]string `json:"verifySamples,omitempty"`
	Identity    map[string]int    `json:"identity"`
}

func main() {
	u := flag.String("url", "ws://127.0.0.1:3480", "relay origin (ws/wss/http/https)")
	cursor := flag.Int64("cursor", -1, "cursor (-1 = none)")
	secs := flag.Int("secs", 30, "seconds to run")
	verify := flag.Bool("verify", false, "sync 1.1 checks on every #commit and #sync")
	plc := flag.String("plc", "http://127.0.0.1:3482", "PLC directory for signing keys")
	seqsOut := flag.String("seqs-out", "", "write every event's seq, one per line")
	acctOut := flag.String("accounts-out", "", "write every #account and #identity frame as JSON lines")
	flag.Parse()

	r := &report{URL: *u, Frames: map[string]int{}, Infos: []map[string]any{}, Errors: []map[string]any{}, Identity: map[string]int{}, FirstSeq: -1}
	if *cursor >= 0 {
		r.Cursor = cursor
	}
	var mu sync.Mutex
	chains := map[string]chain{}
	vfail := func(reason, sample string) {
		if r.Verify == nil {
			r.Verify = map[string]int{}
			r.Samples = map[string]string{}
		}
		r.Verify[reason]++
		if _, ok := r.Samples[reason]; !ok {
			r.Samples[reason] = sample
		}
	}
	dir := identity.BaseDirectory{PLCURL: *plc, SkipHandleVerification: true}
	var seqs []int64
	var acctLines []any
	seen := func(kind string, seq int64) {
		seqs = append(seqs, seq)
		r.Frames[kind]++
		if r.FirstSeq < 0 {
			r.FirstSeq = seq
		}
		if seq <= r.LastSeq {
			r.SeqBackward++
		}
		r.LastSeq = seq
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*secs)*time.Second)
	defer cancel()

	cbs := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			mu.Lock()
			defer mu.Unlock()
			seen("#commit", evt.Seq)
			if !*verify {
				return nil
			}
			tag := fmt.Sprintf("%s rev %s seq %d", evt.Repo, evt.Rev, evt.Seq)
			commit, _, err := repo.LoadCommitFromCAR(ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				vfail("car", tag+": "+err.Error())
				return nil
			}
			if err := commit.VerifyStructure(); err != nil {
				vfail("structure", tag+": "+err.Error())
			}
			if commit.DID != evt.Repo || commit.Rev != evt.Rev {
				vfail("did_or_rev_mismatch", tag)
			}
			if evt.Commit.String() == "" {
				vfail("no_commit_cid", tag)
			}
			if did, err := syntax.ParseDID(evt.Repo); err == nil {
				if id, err := dir.LookupDID(ctx, did); err != nil {
					vfail("resolve", tag+": "+err.Error())
				} else if key, err := id.PublicKey(); err != nil {
					vfail("key", tag+": "+err.Error())
				} else if err := commit.VerifySignature(key); err != nil {
					vfail("signature", tag+": "+err.Error())
				}
			}
			if evt.PrevData == nil {
				vfail("no_prevData", tag)
			} else if _, err := repo.VerifyCommitMessage(ctx, evt); err != nil {
				vfail("inversion", tag+": "+err.Error())
			}
			if c, ok := chains[evt.Repo]; ok {
				if evt.Rev <= c.rev {
					vfail("rev_not_increasing", tag+" after "+c.rev)
				}
				if evt.PrevData != nil && evt.PrevData.String() != c.data {
					vfail("chain_break_prevData", tag)
				}
				if evt.Since != nil && *evt.Since != c.rev {
					vfail("since_mismatch", tag)
				}
			}
			chains[evt.Repo] = chain{rev: evt.Rev, data: commit.Data.String()}
			return nil
		},
		RepoSync: func(evt *comatproto.SyncSubscribeRepos_Sync) error {
			mu.Lock()
			defer mu.Unlock()
			seen("#sync", evt.Seq)
			if !*verify {
				return nil
			}
			tag := fmt.Sprintf("%s rev %s seq %d", evt.Did, evt.Rev, evt.Seq)
			commit, _, err := repo.LoadCommitFromCAR(ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				vfail("sync_car", tag+": "+err.Error())
				return nil
			}
			if commit.DID != evt.Did || commit.Rev != evt.Rev {
				vfail("sync_did_or_rev_mismatch", tag)
			}
			if did, err := syntax.ParseDID(evt.Did); err == nil {
				if id, err := dir.LookupDID(ctx, did); err == nil {
					if key, err := id.PublicKey(); err == nil {
						if err := commit.VerifySignature(key); err != nil {
							vfail("sync_signature", tag+": "+err.Error())
						}
					}
				}
			}
			chains[evt.Did] = chain{rev: evt.Rev, data: commit.Data.String()}
			return nil
		},
		RepoIdentity: func(evt *comatproto.SyncSubscribeRepos_Identity) error {
			mu.Lock()
			defer mu.Unlock()
			seen("#identity", evt.Seq)
			acctLines = append(acctLines, map[string]any{"t": "#identity", "evt": evt})
			if evt.Handle != nil {
				r.Identity["with_handle"]++
			} else {
				r.Identity["no_handle"]++
			}
			return nil
		},
		RepoAccount: func(evt *comatproto.SyncSubscribeRepos_Account) error {
			mu.Lock()
			defer mu.Unlock()
			seen("#account", evt.Seq)
			acctLines = append(acctLines, map[string]any{"t": "#account", "evt": evt})
			return nil
		},
		RepoInfo: func(evt *comatproto.SyncSubscribeRepos_Info) error {
			mu.Lock()
			defer mu.Unlock()
			r.Frames["#info"]++
			m := map[string]any{"name": evt.Name}
			if evt.Message != nil {
				m["message"] = *evt.Message
			}
			r.Infos = append(r.Infos, m)
			return nil
		},
		Error: func(evt *events.ErrorFrame) error {
			mu.Lock()
			defer mu.Unlock()
			r.Frames["error"]++
			r.Errors = append(r.Errors, map[string]any{"error": evt.Error, "message": evt.Message})
			return nil
		},
	}

	pu, err := url.Parse(*u)
	if err != nil {
		panic(err)
	}
	switch pu.Scheme {
	case "http":
		pu.Scheme = "ws"
	case "https":
		pu.Scheme = "wss"
	}
	pu.Path = "/xrpc/com.atproto.sync.subscribeRepos"
	if *cursor >= 0 {
		pu.RawQuery = fmt.Sprintf("cursor=%d", *cursor)
	}
	con, resp, err := websocket.DefaultDialer.DialContext(ctx, pu.String(), http.Header{"User-Agent": []string{"vlrelay-compat-gocheck"}})
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		r.Ended = fmt.Sprintf("dial failed (HTTP %d): %v", code, err)
	} else {
		sched := sequential.NewScheduler("gocheck", cbs.EventHandler)
		err = events.HandleRepoStream(ctx, con, sched, nil)
		switch {
		case ctx.Err() != nil:
			r.Ended = "timeout (still open)"
		case err == nil:
			r.Ended = "closed by server"
		default:
			r.Ended = "error: " + err.Error()
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if *seqsOut != "" {
		var b bytes.Buffer
		for _, s := range seqs {
			fmt.Fprintf(&b, "%d\n", s)
		}
		if err := os.WriteFile(*seqsOut, b.Bytes(), 0o644); err != nil {
			panic(err)
		}
	}
	if *acctOut != "" {
		var b bytes.Buffer
		for _, l := range acctLines {
			j, _ := json.Marshal(l)
			b.Write(append(j, '\n'))
		}
		if err := os.WriteFile(*acctOut, b.Bytes(), 0o644); err != nil {
			panic(err)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(r)
}
