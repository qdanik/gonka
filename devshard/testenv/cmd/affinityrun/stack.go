package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// probeClient bounds every poll and scrape, so a process that accepts a connection and never answers cannot hang a run.
var probeClient = &http.Client{Timeout: 10 * time.Second}

const oneShotLimit = time.Minute

// stack owns every process one run starts and stops them all, newest first.
type stack struct {
	dir     string
	procs   []*proc
	closers []func()
}

type proc struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
	log  *os.File
}

func (s *stack) start(name, bin string, env []string, args ...string) error {
	logFile, err := os.Create(filepath.Join(s.dir, name+".log"))
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Dir = s.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, done: make(chan struct{}), log: logFile}
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	s.procs = append(s.procs, p)
	return nil
}

// runToEnd runs a one-shot command and fails with its log name when it exits non-zero.
func (s *stack) runToEnd(ctx context.Context, name, bin string, args ...string) error {
	if err := s.start(name, bin, nil, args...); err != nil {
		return err
	}
	p := s.procs[len(s.procs)-1]
	s.procs = s.procs[:len(s.procs)-1]
	select {
	case <-p.done:
	case <-ctx.Done():
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		<-p.done
		_ = p.log.Close()
		return fmt.Errorf("%s cancelled", name)
	case <-time.After(oneShotLimit):
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		<-p.done
		_ = p.log.Close()
		return fmt.Errorf("%s did not finish within %s, see %s.log", name, oneShotLimit, name)
	}
	_ = p.log.Close()
	if code := p.cmd.ProcessState.ExitCode(); code != 0 {
		return fmt.Errorf("%s exited with %d, see %s.log", name, code, name)
	}
	return nil
}

// stop sends SIGTERM to each process group and kills what is still up after the grace.
func (s *stack) stop(grace time.Duration) {
	for _, closeFn := range s.closers {
		closeFn()
	}
	s.closers = nil
	for i := len(s.procs) - 1; i >= 0; i-- {
		p := s.procs[i]
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(grace):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
		_ = p.log.Close()
	}
	s.procs = nil
}

// exited names the first process that is no longer running, for a failed readiness wait.
func (s *stack) exited() string {
	for _, p := range s.procs {
		select {
		case <-p.done:
			return p.name
		default:
		}
	}
	return ""
}

// waitHTTP polls url until it answers 200; the wait is bounded and the error names what it waited for.
func (s *stack) waitHTTP(ctx context.Context, what, url string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if name := s.exited(); name != "" {
			return fmt.Errorf("waiting for %s: process %s exited, see %s.log", what, name, name)
		}
		if resp, err := probeClient.Get(url); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return fmt.Errorf("%s not ready at %s after %s", what, url, limit)
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
