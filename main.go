package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	mu       sync.Mutex
	messages []string
	waiters  []chan string
}

func (q *Queue) Put(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w <- msg
		return
	}
	q.messages = append(q.messages, msg)
}

func (q *Queue) GetWOWait(w http.ResponseWriter) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) > 0 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(q.messages[0]))
		q.messages = q.messages[1:]
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (q *Queue) GetWait(w http.ResponseWriter, ctx context.Context) {
	q.mu.Lock()
	if len(q.messages) > 0 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(q.messages[0]))
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return
	}
	ch := make(chan string)
	q.waiters = append(q.waiters, ch)
	q.mu.Unlock()

	defer close(ch)
	select {
	case msg := <-ch:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))
	case <-ctx.Done():
		q.mu.Lock()
		defer q.mu.Unlock()
		select {
		case msg := <-ch:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(msg))
		default:
			for i, waiter := range q.waiters {
				if waiter == ch {
					q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
					break
				}
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

var (
	queues = make(map[string]*Queue)
	mu     sync.RWMutex
)

func getQueue(name string) *Queue {
	mu.RLock()
	q, ok := queues[name]
	if ok {
		mu.RUnlock()
		return q
	}
	mu.RUnlock()
	mu.Lock()
	defer mu.Unlock()
	q, ok = queues[name]
	if ok {
		return q
	}
	q = &Queue{messages: make([]string, 0), waiters: make([]chan string, 0)}
	queues[name] = q
	return q
}

var port = flag.Int("port", 80, "port to listen on")

func main() {
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /{queue...}", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if !q.Has("v") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		getQueue(r.PathValue("queue")).Put(q.Get("v"))
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /{queue...}", func(w http.ResponseWriter, r *http.Request) {
		q := getQueue(r.PathValue("queue"))
		tStr := r.URL.Query().Get("timeout")
		if tStr == "" {
			q.GetWOWait(w)
			return
		}
		t, _ := strconv.Atoi(tStr)
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(t)*time.Second)
		defer cancel()
		q.GetWait(w, ctx)
	})

	http.ListenAndServe(fmt.Sprintf(":%d", *port), mux)
}
