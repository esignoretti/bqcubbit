package webui

import (
	"embed"
	"encoding/json"
	"html/template"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/esignoretti/bqcubbit/internal/state"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

type LogBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
	subs  map[chan string]struct{}
}

func NewLogBuffer(cap int) *LogBuffer {
	return &LogBuffer{
		lines: make([]string, 0, cap),
		cap:   cap,
		subs:  make(map[chan string]struct{}),
	}
}

func (lb *LogBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	line := string(p)
	lb.lines = append(lb.lines, line)
	if len(lb.lines) > lb.cap {
		lb.lines = lb.lines[1:]
	}
	for ch := range lb.subs {
		select {
		case ch <- line:
		default:
		}
	}
	return len(p), nil
}

func (lb *LogBuffer) Subscribe() chan string {
	ch := make(chan string, 64)
	lb.mu.Lock()
	lb.subs[ch] = struct{}{}
	lb.mu.Unlock()
	return ch
}

func (lb *LogBuffer) Unsubscribe(ch chan string) {
	lb.mu.Lock()
	delete(lb.subs, ch)
	lb.mu.Unlock()
}

type Handler struct {
	stateStore state.StateStore
	templates  *template.Template
	logBuf     *LogBuffer
}

type TableSummary struct {
	Dataset        string
	Name           string
	SchemaVersion  int
	LastSync       string
	PartitionCount int
}

type DashboardData struct {
	Title  string
	Page   string
	Tables []TableSummary
}

type TableDetailData struct {
	Title          string
	Page           string
	Dataset        string
	Table          string
	SchemaVersions []SchemaVersionRow
	Partitions     []PartitionRow
}

type SchemaVersionRow struct {
	Version    int
	ChangeType string
	ValidFrom  string
	ValidUntil string
}

type PartitionRow struct {
	PartitionID    string
	SchemaVersion  int
	LastSync       string
	RowCount       int64
	BytesInCubbit  int64
}

func NewHandler(stateStore state.StateStore) (*Handler, error) {
	tmpl, err := template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Handler{
		stateStore: stateStore,
		templates:  tmpl,
		logBuf:     NewLogBuffer(1000),
	}, nil
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", h.dashboard)
	mux.HandleFunc("/table/", h.tableDetail)
	mux.HandleFunc("/api/logs", h.sseLogs)
	mux.HandleFunc("/api/status", h.apiStatus)
	if _, err := staticFS.Open("static"); err == nil {
		mux.Handle("/static/", http.FileServer(http.FS(staticFS)))
	}
}

func (h *Handler) dashboard(w http.ResponseWriter, r *http.Request) {
	summaries, err := h.stateStore.GetDashboardSummary(r.Context())
	tables := []TableSummary{}
	if err == nil {
		for _, s := range summaries {
			lastSync := ""
			if s.LastSyncTime != nil {
				lastSync = s.LastSyncTime.Format(time.RFC3339)
			}
			tables = append(tables, TableSummary{
				Dataset:        s.Dataset,
				Name:           s.TableName,
				SchemaVersion:  s.SchemaVersion,
				LastSync:       lastSync,
				PartitionCount: s.PartitionCount,
			})
		}
	}
	data := DashboardData{
		Title:  "bqcubbit Dashboard",
		Page:   "dashboard",
		Tables: tables,
	}
	h.templates.ExecuteTemplate(w, "layout.html", data)
}

func (h *Handler) tableDetail(w http.ResponseWriter, r *http.Request) {
	dataset := r.PathValue("dataset")
	table := r.PathValue("table")

	data := TableDetailData{
		Title:   dataset + "." + table,
		Page:    "table",
		Dataset: dataset,
		Table:   table,
	}
	h.templates.ExecuteTemplate(w, "layout.html", data)
}

func (h *Handler) sseLogs(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := h.logBuf.Subscribe()
	defer h.logBuf.Unsubscribe(ch)

	for {
		select {
		case <-r.Context().Done():
			return
		case line := <-ch:
			data, _ := json.Marshal(line)
			io.WriteString(w, "data: ")
			w.Write(data)
			io.WriteString(w, "\n\n")
			flusher.Flush()
		}
	}
}

func (h *Handler) apiStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "running"})
}
