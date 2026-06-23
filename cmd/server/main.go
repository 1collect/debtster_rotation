package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
)

var rotationFixedStatuses = map[string]bool{
	"оплата пв":                 true,
	"оплата по соглашению":      true,
	"оплата по первому условию": true,
	"оплата по второму условию": true,
	"полное погашение":          true,
	"полное погошение":          true,
}

var alignmentFixedStatuses = map[string]bool{
	"оплата пв":                 true,
	"оплата по соглашению":      true,
	"оплата по первому условию": true,
	"оплата по второму условию": true,
	"полное погашение":          true,
	"полное погошение":          true,
	"должник обещает оплату":    true,
}

var rotationScoreWeights = scoreWeights{
	amount:        decimal.NewFromInt(1),
	materialCount: decimal.NewFromInt(1),
	iinCount:      decimal.NewFromInt(1),
}
var alignmentScoreWeights = scoreWeights{
	amount:        decimal.NewFromInt(1),
	materialCount: decimal.Zero,
	iinCount:      decimal.NewFromInt(1),
}

func init() {
	decimal.DivisionPrecision = 80
}

type server struct {
	hub          *progressHub
	jobs         map[string]*jobResult
	integrations *appIntegrations
	serviceToken string
	mu           sync.Mutex
}

type jobResult struct {
	id          string
	process     string
	state       string
	percent     int
	message     string
	resultData  []byte
	filename    string
	err         string
	createdAt   time.Time
	startedAt   time.Time
	completedAt time.Time
	cancel      context.CancelFunc
	log         []progressRecord
}

type progressRecord struct {
	At      time.Time `json:"at"`
	Percent int       `json:"percent"`
	Message string    `json:"message"`
}

type jobView struct {
	ID          string           `json:"id"`
	Process     string           `json:"process"`
	State       string           `json:"state"`
	Percent     int              `json:"percent"`
	Message     string           `json:"message"`
	LastComment string           `json:"last_comment,omitempty"`
	Filename    string           `json:"filename,omitempty"`
	Error       string           `json:"error,omitempty"`
	DownloadURL string           `json:"download_url,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
	StartedAt   time.Time        `json:"started_at,omitempty"`
	CompletedAt time.Time        `json:"completed_at,omitempty"`
	Log         []progressRecord `json:"log,omitempty"`
}

type payload map[string]any

type progressHub struct {
	mu      sync.Mutex
	clients map[string]map[chan payload]bool
}

type columns struct {
	rp, iin, detach, attach, status, amount, sourceRP int
}

type loginKey struct {
	rp    string
	login string
}

type iinGroup struct {
	rp           string
	sourceRP     string
	iin          string
	rows         []int
	amount       decimal.Decimal
	pinnedLogin  string
	currentLogin string
}

type load struct {
	count    int
	amount   decimal.Decimal
	iinCount int
}

type scoreWeights struct {
	amount        decimal.Decimal
	materialCount decimal.Decimal
	iinCount      decimal.Decimal
}

type workbookConfig struct {
	fixedStatuses map[string]bool
	sourceColumn  string
	strategy      string
	processName   string
	summaryTitle  string
}

type workbookDiagnostics struct {
	rows           int
	repeatedHeader int
	missingRP      int
	missingIIN     int
	missingLogin   int
	fixedRows      int
	rotationRows   int
	distinctLogins int
}

type progressFunc func(percent int, message string)

type progressCounter struct {
	mu           sync.Mutex
	progress     progressFunc
	startPercent int
	endPercent   int
	done         int
	total        int
	lastPercent  int
	label        string
}

type rpBalanceResult struct {
	rp          string
	assignments map[string]loginKey
	loads       map[loginKey]*load
	loginIINs   map[loginKey]map[string]bool
	err         error
}

type partialMoveCandidate struct {
	group      *iinGroup
	login      loginKey
	score      decimal.Decimal
	groupIndex int
	loginIndex int
	ok         bool
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	app := &server{
		hub:  &progressHub{clients: make(map[string]map[chan payload]bool)},
		jobs: make(map[string]*jobResult),
	}
	app.integrations = initIntegrations(context.Background())
	app.serviceToken = os.Getenv("SERVICE_TOKEN")
	if app.serviceToken == "" {
		app.serviceToken = "debtster-rotation"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/upload", app.uploadAndStart)
	mux.HandleFunc("/api/jobs", app.requireAuth(app.jobsList))
	mux.HandleFunc("/api/jobs/", app.requireAuth(app.jobAction))
	mux.HandleFunc("/api/history/clear", app.requireAuth(app.clearHistory))
	mux.HandleFunc("/rotate-parallel/", app.requireAuth(app.rotateParallelFile))
	mux.HandleFunc("/rotate-between-rp/", app.requireAuth(app.rotateBetweenRPFile))
	mux.HandleFunc("/balance/", app.requireAuth(app.balanceFile))
	mux.HandleFunc("/download/", app.requireAuth(app.downloadFile))
	mux.HandleFunc("/ws/rotation/", app.requireAuth(app.websocket))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8040"
	}
	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, recoverPanic(withCORS(logRequests(mux)))); err != nil {
		log.Fatal(err)
	}
}

func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("http panic method=%s path=%s panic=%v stack=%s", r.Method, r.URL.Path, recovered, debug.Stack())
				writeCORS(w)
				writeJSONError(w, "Внутренняя ошибка сервера.", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		log.Printf("http request method=%s path=%s remote=%s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
		log.Printf("http done method=%s path=%s elapsed=%s", r.Method, r.URL.Path, time.Since(started).Round(time.Millisecond))
	})
}

func (s *server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.validServiceToken(r) {
			if _, ok := s.authUserID(r); !ok {
				writeJSONError(w, "Требуется авторизация.", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *server) validServiceToken(r *http.Request) bool {
	if s.serviceToken == "" {
		return false
	}

	token := r.Header.Get("X-Rotation-Token")
	if token == "" {
		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		}
	}
	if token == "" {
		token = r.URL.Query().Get("token")
	}

	return subtle.ConstantTimeCompare([]byte(token), []byte(s.serviceToken)) == 1
}

func (s *server) rotateParallelFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	file, uploadedFilename, jobID, err := validateUpload(r)
	if err != nil {
		log.Printf("upload rejected process=rotation_parallel job=%s error=%q", jobID, err.Error())
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("upload read failed process=rotation_parallel job=%s file=%q error=%v", jobID, uploadedFilename, err)
		writeJSONError(w, "Не удалось прочитать файл.", http.StatusBadRequest)
		return
	}
	log.Printf("upload accepted process=rotation_parallel job=%s file=%q bytes=%d", jobID, uploadedFilename, len(content))

	if !s.startJob(content, jobID, "rotation_parallel", "rotation_parallel_result.xlsx", workbookConfig{
		fixedStatuses: rotationFixedStatuses,
		sourceColumn:  "detach",
		strategy:      "full_parallel",
		processName:   "параллельной ротации",
		summaryTitle:  "Итоги параллельной ротации",
	}) {
		log.Printf("job rejected process=rotation_parallel job=%s reason=running_job_exists", jobID)
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "state": "running"})
}

func (s *server) rotateBetweenRPFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	file, uploadedFilename, jobID, err := validateUpload(r)
	if err != nil {
		log.Printf("upload rejected process=rotation_between_rp job=%s error=%q", jobID, err.Error())
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("upload read failed process=rotation_between_rp job=%s file=%q error=%v", jobID, uploadedFilename, err)
		writeJSONError(w, "Не удалось прочитать файл.", http.StatusBadRequest)
		return
	}
	log.Printf("upload accepted process=rotation_between_rp job=%s file=%q bytes=%d", jobID, uploadedFilename, len(content))

	if !s.startJob(content, jobID, "rotation_between_rp", "rotation_between_rp_result.xlsx", workbookConfig{
		fixedStatuses: rotationFixedStatuses,
		sourceColumn:  "detach",
		strategy:      "cross_rp",
		processName:   "ротации между РП",
		summaryTitle:  "Итоги ротации между РП",
	}) {
		log.Printf("job rejected process=rotation_between_rp job=%s reason=running_job_exists", jobID)
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "state": "running"})
}

func (s *server) balanceFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	file, uploadedFilename, jobID, err := validateUpload(r)
	if err != nil {
		log.Printf("upload rejected process=alignment job=%s error=%q", jobID, err.Error())
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("upload read failed process=alignment job=%s file=%q error=%v", jobID, uploadedFilename, err)
		writeJSONError(w, "Не удалось прочитать файл.", http.StatusBadRequest)
		return
	}
	log.Printf("upload accepted process=alignment job=%s file=%q bytes=%d", jobID, uploadedFilename, len(content))

	if !s.startJob(content, jobID, "alignment", "alignment_result.xlsx", workbookConfig{
		fixedStatuses: alignmentFixedStatuses,
		sourceColumn:  "attach",
		strategy:      "partial",
		processName:   "выравнивания",
		summaryTitle:  "Итоги выравнивания",
	}) {
		log.Printf("job rejected process=alignment job=%s reason=running_job_exists", jobID)
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "state": "running"})
}

func (s *server) startJob(content []byte, jobID, process, filename string, cfg workbookConfig) bool {
	ctx, cancel := context.WithCancel(context.Background())
	job := &jobResult{
		id:        jobID,
		process:   process,
		state:     "running",
		percent:   0,
		message:   "Задача создана",
		filename:  filename,
		createdAt: time.Now(),
		startedAt: time.Now(),
		cancel:    cancel,
	}
	s.mu.Lock()
	if s.hasRunningJobLocked() {
		s.mu.Unlock()
		cancel()
		return false
	}
	s.jobs[jobID] = job
	s.mu.Unlock()

	log.Printf("job started process=%s job=%s bytes=%d strategy=%s source_column=%s", process, jobID, len(content), cfg.strategy, cfg.sourceColumn)
	s.sendProgress(jobID, 1, fmt.Sprintf("Задача %s создана, запускаю обработку", cfg.processName))
	go s.runJob(ctx, content, jobID, filename, cfg)
	return true
}

func (s *server) hasRunningJobLocked() bool {
	for _, job := range s.jobs {
		if job.state == "running" || job.state == "canceling" {
			return true
		}
	}
	return false
}

func (s *server) runJob(ctx context.Context, content []byte, jobID, filename string, cfg workbookConfig) {
	started := time.Now()
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("job panic job=%s process=%s panic=%v", jobID, cfg.processName, recovered)
			s.storeJobError(jobID, fmt.Sprintf("Внутренняя ошибка обработки: %v", recovered))
		}
	}()
	log.Printf("job worker begin job=%s process=%s result=%s", jobID, cfg.processName, filename)
	output, err := redistributeWorkbook(ctx, bytes.NewReader(content), jobID, s, cfg)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Printf("job canceled job=%s process=%s elapsed=%s", jobID, cfg.processName, time.Since(started).Round(time.Millisecond))
			s.storeJobCanceled(jobID)
			return
		}
		log.Printf("job failed job=%s process=%s elapsed=%s error=%v", jobID, cfg.processName, time.Since(started).Round(time.Millisecond), err)
		s.storeJobError(jobID, err.Error())
		return
	}
	if err := ctx.Err(); err != nil {
		log.Printf("job canceled after workbook job=%s process=%s elapsed=%s", jobID, cfg.processName, time.Since(started).Round(time.Millisecond))
		s.storeJobCanceled(jobID)
		return
	}

	resultPath, err := s.saveRotationResult(ctx, jobID, filename, output)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Printf("job canceled during result save job=%s process=%s elapsed=%s", jobID, cfg.processName, time.Since(started).Round(time.Millisecond))
			s.storeJobCanceled(jobID)
			return
		}
		log.Printf("job result s3 save failed job=%s process=%s error=%v", jobID, cfg.processName, err)
		s.storeJobError(jobID, "Не удалось сохранить результат в MinIO.")
		return
	}

	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		if job.state == "canceling" || job.state == "canceled" {
			s.mu.Unlock()
			log.Printf("job canceled before ready store job=%s process=%s elapsed=%s", jobID, cfg.processName, time.Since(started).Round(time.Millisecond))
			s.storeJobCanceled(jobID)
			return
		}
		job.state = "ready"
		job.resultData = append([]byte(nil), output...)
		job.filename = filename
		job.percent = 100
		job.message = "Файл готов."
		job.completedAt = time.Now()
		job.cancel = nil
	}
	s.mu.Unlock()
	_ = s.updateRotationRecord(context.Background(), jobID, bson.M{
		"status":       "completed",
		"progress":     100,
		"message":      "Файл готов.",
		"completed_at": time.Now(),
	})
	s.hub.send(jobID, payload{
		"type":         "job_ready",
		"message":      "Файл готов.",
		"last_comment": "Файл готов.",
		"download_url": "/download/" + jobID + "/",
		"result_path":  resultPath,
	})
	log.Printf("job ready job=%s process=%s result=%s bytes=%d elapsed=%s", jobID, cfg.processName, resultPath, len(output), time.Since(started).Round(time.Millisecond))
}

func (s *server) storeJobError(jobID, message string) {
	log.Printf("job error stored job=%s message=%q", jobID, message)
	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		if job.state == "canceled" {
			s.mu.Unlock()
			return
		}
		job.state = "error"
		job.err = message
		job.message = message
		job.completedAt = time.Now()
		job.cancel = nil
	}
	s.mu.Unlock()
	_ = s.updateRotationRecord(context.Background(), jobID, bson.M{
		"status":       "failed",
		"message":      message,
		"completed_at": time.Now(),
	})
	s.hub.send(jobID, payload{"type": "job_error", "message": message})
}

func (s *server) storeJobCanceled(jobID string) {
	log.Printf("job canceled stored job=%s", jobID)
	now := time.Now()
	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		job.state = "canceled"
		job.message = "Задача отменена."
		job.completedAt = now
		job.cancel = nil
	}
	s.mu.Unlock()
	_ = s.updateRotationRecordDetached(jobID, bson.M{
		"status":       "canceled",
		"message":      "Задача отменена.",
		"completed_at": now,
	})
	s.hub.send(jobID, payload{"type": "job_canceled", "message": "Задача отменена."})
}

func (s *server) downloadFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	jobID := strings.TrimPrefix(r.URL.Path, "/download/")
	jobID = strings.TrimSuffix(jobID, "/")
	if jobID == "" {
		http.NotFound(w, r)
		return
	}

	s.mu.Lock()
	result, ok := s.jobs[jobID]
	s.mu.Unlock()
	if !ok {
		writeJSONError(w, "Результат не найден.", http.StatusNotFound)
		return
	}
	if result.state == "running" {
		writeJSONError(w, "Файл еще обрабатывается.", http.StatusConflict)
		return
	}
	if result.state == "canceling" {
		writeJSONError(w, "Задача отменяется.", http.StatusConflict)
		return
	}
	if result.state == "error" {
		writeJSONError(w, result.err, http.StatusBadRequest)
		return
	}
	if result.state == "canceled" {
		writeJSONError(w, "Задача отменена.", http.StatusBadRequest)
		return
	}
	if len(result.resultData) == 0 {
		writeJSONError(w, "Файл результата не найден.", http.StatusNotFound)
		return
	}
	writeXLSX(w, result.resultData, result.filename)
}

func (s *server) jobsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	views := make([]jobView, 0, len(s.jobs))
	for _, job := range s.jobs {
		views = append(views, s.jobViewLocked(job, false))
	}
	s.mu.Unlock()
	sort.Slice(views, func(i, j int) bool {
		return views[i].CreatedAt.After(views[j].CreatedAt)
	})
	_ = json.NewEncoder(w).Encode(views)
}

func (s *server) jobAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(path, "/")
	jobID := parts[0]
	if len(parts) == 2 && parts[1] == "cancel" {
		s.cancelJob(w, r, jobID)
		return
	}
	if len(parts) != 1 || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	s.mu.Lock()
	job := s.jobs[jobID]
	if job == nil {
		s.mu.Unlock()
		_ = s.cancelInterruptedRotationRecord(jobID, "Задача отменена после перезапуска сервиса.", time.Now())
		_ = json.NewEncoder(w).Encode(map[string]string{
			"state":   "canceled",
			"message": "Задача отменена после перезапуска сервиса.",
		})
		return
	}
	view := s.jobViewLocked(job, true)
	s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(view)
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request, jobID string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	now := time.Now()
	s.mu.Lock()
	job := s.jobs[jobID]
	if job == nil {
		s.mu.Unlock()
		_ = s.cancelInterruptedRotationRecord(jobID, "Задача отменена.", now)
		_ = json.NewEncoder(w).Encode(map[string]string{"state": "canceled"})
		return
	}
	if job.state != "running" || job.cancel == nil {
		view := s.jobViewLocked(job, true)
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(view)
		return
	}
	cancel := job.cancel
	job.state = "canceled"
	job.message = "Задача отменена."
	job.completedAt = now
	job.cancel = nil
	job.log = append(job.log, progressRecord{At: time.Now(), Percent: job.percent, Message: job.message})
	percent := job.percent
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	_ = s.updateRotationRecordDetached(jobID, bson.M{
		"status":       "canceled",
		"progress":     percent,
		"message":      "Задача отменена.",
		"completed_at": now,
	})
	s.hub.send(jobID, payload{
		"type":         "job_canceled",
		"percent":      percent,
		"message":      "Задача отменена.",
		"last_comment": "Задача отменена.",
		"state":        "canceled",
	})
	_ = json.NewEncoder(w).Encode(map[string]string{"state": "canceled"})
}

func (s *server) jobViewLocked(job *jobResult, includeLog bool) jobView {
	view := jobView{
		ID:          job.id,
		Process:     job.process,
		State:       job.state,
		Percent:     job.percent,
		Message:     job.message,
		LastComment: job.message,
		Filename:    job.filename,
		Error:       job.err,
		CreatedAt:   job.createdAt,
		StartedAt:   job.startedAt,
		CompletedAt: job.completedAt,
	}
	if job.state == "ready" && len(job.resultData) > 0 {
		view.DownloadURL = "/download/" + job.id + "/"
	}
	if includeLog {
		view.Log = append([]progressRecord(nil), job.log...)
	}
	return view
}

func (s *server) clearHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var cancels []context.CancelFunc
	s.mu.Lock()
	for _, job := range s.jobs {
		if job.cancel != nil {
			cancels = append(cancels, job.cancel)
		}
	}
	s.jobs = make(map[string]*jobResult)
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	_ = json.NewEncoder(w).Encode(map[string]bool{"cleared": true})
}

func (s *server) websocket(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimPrefix(r.URL.Path, "/ws/rotation/")
	jobID = strings.TrimSuffix(jobID, "/")
	if jobID == "" {
		http.NotFound(w, r)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ch := s.hub.add(jobID)
	defer s.hub.remove(jobID, ch)

	s.mu.Lock()
	job := s.jobs[jobID]
	if job != nil {
		_ = conn.WriteJSON(payload{
			"type":         "progress",
			"percent":      job.percent,
			"message":      job.message,
			"last_comment": job.message,
			"state":        job.state,
		})
		if job.state == "ready" {
			_ = conn.WriteJSON(payload{"type": "job_ready", "message": job.message, "last_comment": job.message, "download_url": "/download/" + jobID + "/"})
		}
	} else {
		message := "Задача остановлена после перезапуска сервиса."
		_ = s.cancelInterruptedRotationRecord(jobID, message, time.Now())
		_ = conn.WriteJSON(payload{
			"type":         "job_canceled",
			"percent":      0,
			"message":      message,
			"last_comment": message,
			"state":        "canceled",
		})
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case msg := <-ch:
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (h *progressHub) add(jobID string) chan payload {
	ch := make(chan payload, 16)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[jobID] == nil {
		h.clients[jobID] = make(map[chan payload]bool)
	}
	h.clients[jobID][ch] = true
	return ch
}

func (h *progressHub) remove(jobID string, ch chan payload) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients[jobID], ch)
	close(ch)
	if len(h.clients[jobID]) == 0 {
		delete(h.clients, jobID)
	}
}

func (h *progressHub) send(jobID string, msg payload) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients[jobID] {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (s *server) sendProgress(jobID string, percent int, message string) {
	record := progressRecord{At: time.Now(), Percent: percent, Message: message}
	log.Printf("job progress job=%s percent=%d message=%q", jobID, percent, message)
	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		if job.state != "running" {
			s.mu.Unlock()
			return
		}
		job.percent = percent
		job.message = message
		job.log = append(job.log, record)
		if len(job.log) > 200 {
			job.log = job.log[len(job.log)-200:]
		}
	}
	s.mu.Unlock()
	_ = s.updateRotationRecord(context.Background(), jobID, bson.M{
		"status":   "running",
		"progress": percent,
		"message":  message,
	})
	s.hub.send(jobID, payload{
		"type":         "progress",
		"percent":      percent,
		"message":      message,
		"last_comment": message,
		"state":        "running",
	})
}

func reportProgress(progress progressFunc, percent int, message string) {
	if progress != nil {
		progress(percent, message)
	}
}

func reportRangeProgress(progress progressFunc, startPercent, endPercent, done, total, lastPercent int, label string) int {
	if progress == nil || total <= 0 {
		return lastPercent
	}
	percent := startPercent + ((endPercent - startPercent) * done / total)
	if percent <= lastPercent {
		return lastPercent
	}
	if percent > endPercent {
		percent = endPercent
	}
	progress(percent, fmt.Sprintf("%s: %d из %d", label, done, total))
	return percent
}

func newProgressCounter(progress progressFunc, startPercent, endPercent, total int, label string) *progressCounter {
	return &progressCounter{
		progress:     progress,
		startPercent: startPercent,
		endPercent:   endPercent,
		total:        max(total, 1),
		lastPercent:  startPercent,
		label:        label,
	}
}

func (counter *progressCounter) add(done int) {
	if counter == nil || counter.progress == nil || done <= 0 {
		return
	}
	counter.mu.Lock()
	defer counter.mu.Unlock()
	counter.done += done
	counter.lastPercent = reportRangeProgress(
		counter.progress,
		counter.startPercent,
		counter.endPercent,
		counter.done,
		counter.total,
		counter.lastPercent,
		counter.label,
	)
}

func validateUpload(r *http.Request) (io.ReadCloser, string, string, error) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		return nil, "", "", errors.New("Загрузите XLSX файл.")
	}
	jobID := r.FormValue("job_id")
	if jobID == "" {
		jobID = newJobID()
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		return nil, "", jobID, errors.New("Загрузите XLSX файл.")
	}
	if !strings.HasSuffix(strings.ToLower(header.Filename), ".xlsx") {
		_ = file.Close()
		return nil, "", jobID, errors.New("Поддерживается только формат .xlsx.")
	}
	return file, header.Filename, jobID, nil
}

func newJobID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

func writeJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func writeXLSX(w http.ResponseWriter, content []byte, filename string) {
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	_, _ = w.Write(content)
}

func redistributeWorkbook(ctx context.Context, input io.Reader, jobID string, app *server, cfg workbookConfig) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 5, "Файл загружен, читаю Excel")

	workbook, err := excelize.OpenReader(input)
	if err != nil {
		return nil, errors.New("Не удалось прочитать Excel файл.")
	}
	defer workbook.Close()

	sheet := workbook.GetSheetName(0)
	if sheet == "" {
		return nil, errors.New("Не найден первый лист Excel.")
	}

	rows, err := workbook.GetRows(sheet)
	if err != nil {
		return nil, errors.New("Не удалось прочитать строки Excel.")
	}
	header, err := readHeaderFromRows(rows)
	if err != nil {
		return nil, err
	}
	cols, err := findColumns(header)
	if err != nil {
		return nil, err
	}

	rowCount := max(len(rows)-1, 0)
	if rowCount == 0 {
		log.Printf("workbook rejected job=%s process=%s reason=no_data_rows sheet=%q", jobID, cfg.processName, sheet)
		return nil, fmt.Errorf("В файле нет строк для %s.", cfg.processName)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	diagnostics := diagnoseRows(rows, cols, cfg.fixedStatuses, cfg.sourceColumn)
	log.Printf(
		"workbook diagnostics job=%s process=%s sheet=%q rows=%d repeated_headers=%d missing_rp=%d missing_iin=%d missing_login=%d fixed_rows=%d rotation_rows=%d distinct_logins=%d source_column=%s",
		jobID,
		cfg.processName,
		sheet,
		diagnostics.rows,
		diagnostics.repeatedHeader,
		diagnostics.missingRP,
		diagnostics.missingIIN,
		diagnostics.missingLogin,
		diagnostics.fixedRows,
		diagnostics.rotationRows,
		diagnostics.distinctLogins,
		cfg.sourceColumn,
	)
	app.sendProgress(jobID, 12, fmt.Sprintf(
		"Найдено строк: %d. В ротацию: %d, фиксированных: %d, повторных заголовков: %d, без ИИН: %d, без логина: %d",
		diagnostics.rows,
		diagnostics.rotationRows,
		diagnostics.fixedRows,
		diagnostics.repeatedHeader,
		diagnostics.missingIIN,
		diagnostics.missingLogin,
	))
	groupsByKey, groups, loads, loginIINs, fixedCount, fixedIINCount := collectGroupsFromRows(workbook, sheet, rows, cols, cfg.fixedStatuses, cfg.sourceColumn)
	if len(groupsByKey) == 0 {
		log.Printf("workbook rejected job=%s process=%s reason=no_groups fixed_rows=%d missing_iin=%d missing_login=%d missing_rp=%d", jobID, cfg.processName, diagnostics.fixedRows, diagnostics.missingIIN, diagnostics.missingLogin, diagnostics.missingRP)
		return nil, fmt.Errorf("Нет строк для %s после фиксации материалов по статусам.", cfg.processName)
	}

	loginKeys := readLoginKeysFromRows(rows, cols, cfg.sourceColumn)
	if len(loginKeys) == 0 {
		log.Printf("workbook rejected job=%s process=%s reason=no_logins source_column=%s missing_login=%d", jobID, cfg.processName, cfg.sourceColumn, diagnostics.missingLogin)
		return nil, errors.New("Не найдены логины для распределения.")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 32, fmt.Sprintf("Логинов: %d, ИИН в ротации: %d, строк в ротации: %d, зафиксировано строк: %d, зафиксировано ИИН: %d", len(loginKeys), len(groupsByKey), diagnostics.rotationRows, fixedCount, fixedIINCount))
	ensureLoadsForAllLogins(loads, loginIINs, loginKeys)

	app.sendProgress(jobID, 48, "Считаю нагрузку по логинам и подбираю распределение по РП")
	var assignments map[string]loginKey
	progress := func(percent int, message string) {
		app.sendProgress(jobID, percent, message)
	}
	if cfg.strategy == "partial" {
		assignments, err = partiallyBalanceGroups(ctx, groups, loginKeys, loads, loginIINs, progress)
	} else if cfg.strategy == "cross_rp" {
		assignments, err = balanceGroupsAcrossRP(ctx, groups, loginKeys, loads, loginIINs, progress)
	} else if cfg.strategy == "full_parallel" {
		assignments, err = balanceGroupsParallel(ctx, groups, loginKeys, loads, loginIINs, progress)
	} else {
		err = fmt.Errorf("Неизвестная стратегия обработки: %s.", cfg.strategy)
	}
	if err != nil {
		log.Printf("assignment failed job=%s process=%s strategy=%s groups=%d logins=%d error=%v", jobID, cfg.processName, cfg.strategy, len(groups), len(loginKeys), err)
		return nil, err
	}

	rotationRowCount := 0
	for _, group := range groupsByKey {
		rotationRowCount += len(group.rows)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.strategy == "cross_rp" {
		sourceRPCol, err := ensureSourceRPColumn(workbook, sheet, rows, cols)
		if err != nil {
			return nil, fmt.Errorf("Не удалось создать колонку Исходное рп: %w", err)
		}
		cols.sourceRP = sourceRPCol
		fillSourceRPColumn(workbook, sheet, rows, cols)
	}
	app.sendProgress(jobID, 70, fmt.Sprintf("Распределено ИИН: %d. Заполняю колонку \"Закрепить\" для %d строк", len(assignments), rotationRowCount))
	for key, group := range groupsByKey {
		login := assignments[key]
		for _, rowNumber := range group.rows {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.strategy == "cross_rp" {
				_ = setCell(workbook, sheet, rowNumber, cols.rp, login.rp)
			}
			_ = setCell(workbook, sheet, rowNumber, cols.attach, login.login)
		}
	}

	_ = styleAttachColumn(workbook, sheet, cols.attach)
	if cfg.strategy == "cross_rp" {
		_ = styleSourceRPColumn(workbook, sheet, cols.sourceRP)
	}

	app.sendProgress(jobID, 84, "Формирую лист с итогами")
	summaryLoads, _ := collectFinalLoads(workbook, sheet, cols)
	if err := replaceSummarySheet(workbook, summaryLoads, fixedCount, fixedIINCount, cfg.summaryTitle); err != nil {
		return nil, fmt.Errorf("Не удалось сформировать лист итогов: %w", err)
	}
	if cfg.strategy == "cross_rp" {
		if err := appendCrossRPExchangeSummary(workbook, sheet, cols, cfg.summaryTitle); err != nil {
			return nil, fmt.Errorf("Не удалось сформировать итоги обмена между РП: %w", err)
		}
	}

	var output bytes.Buffer
	if err := workbook.Write(&output); err != nil {
		return nil, errors.New("Не удалось сохранить Excel файл.")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 100, fmt.Sprintf("Процесс %s завершен", cfg.processName))
	return output.Bytes(), nil
}

func readHeader(workbook *excelize.File, sheet string) (map[string]int, error) {
	rows, err := workbook.GetRows(sheet)
	if err != nil || len(rows) == 0 {
		return nil, errors.New("Не удалось прочитать заголовок Excel.")
	}
	return readHeaderFromRows(rows)
}

func readHeaderFromRows(rows [][]string) (map[string]int, error) {
	if len(rows) == 0 {
		return nil, errors.New("Не удалось прочитать заголовок Excel.")
	}
	header := make(map[string]int)
	for i, value := range rows[0] {
		if strings.TrimSpace(value) == "" {
			continue
		}
		header[normalizeHeader(value)] = i + 1
	}
	return header, nil
}

func findColumns(header map[string]int) (columns, error) {
	cols := columns{
		rp:       header["рп"],
		iin:      header["иин"],
		detach:   header["открепить"],
		attach:   header["закрепить"],
		status:   header["статус"],
		sourceRP: header["исходное рп"],
	}
	for name, col := range header {
		if strings.HasPrefix(name, "общая задолженность") {
			cols.amount = col
			break
		}
	}

	var missing []string
	checks := []struct {
		col   int
		title string
	}{
		{cols.rp, "РП"},
		{cols.iin, "ИИН"},
		{cols.amount, "Общая задолженность"},
		{cols.detach, "Открепить"},
		{cols.attach, "Закрепить"},
		{cols.status, "Статус"},
	}
	for _, item := range checks {
		if item.col == 0 {
			missing = append(missing, item.title)
		}
	}
	if len(missing) > 0 {
		return cols, errors.New("Не найдены колонки: " + strings.Join(missing, ", "))
	}
	return cols, nil
}

func collectGroups(workbook *excelize.File, sheet string, maxRow int, cols columns, fixedStatuses map[string]bool, sourceColumn string) (map[string]*iinGroup, []*iinGroup, map[loginKey]*load, map[loginKey]map[string]bool, int, int) {
	rows, err := workbook.GetRows(sheet)
	if err != nil {
		rows = nil
	}
	if maxRow > 0 && maxRow < len(rows) {
		rows = rows[:maxRow]
	}
	return collectGroupsFromRows(workbook, sheet, rows, cols, fixedStatuses, sourceColumn)
}

func diagnoseRows(rows [][]string, cols columns, fixedStatuses map[string]bool, sourceColumn string) workbookDiagnostics {
	diagnostics := workbookDiagnostics{}
	seenLogins := make(map[loginKey]bool)
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		diagnostics.rows++
		row := rows[rowIndex]
		if isRepeatedHeaderRow(row, cols) {
			diagnostics.repeatedHeader++
			continue
		}

		rp := normalizeRP(getRowCell(row, cols.rp))
		iin := normalizeIIN(getRowCell(row, cols.iin))
		currentLogin := readSourceLoginFromRow(row, cols, sourceColumn)
		if rp == "" {
			diagnostics.missingRP++
		}
		if iin == "" {
			diagnostics.missingIIN++
		}
		if currentLogin == "" {
			diagnostics.missingLogin++
		}
		if rp == "" || iin == "" || currentLogin == "" {
			continue
		}

		login := loginKey{rp: rp, login: currentLogin}
		if !seenLogins[login] {
			seenLogins[login] = true
			diagnostics.distinctLogins++
		}
		status := normalizeStatus(getRowCell(row, cols.status))
		if fixedStatuses[status] {
			diagnostics.fixedRows++
			continue
		}
		diagnostics.rotationRows++
	}
	return diagnostics
}

func collectGroupsFromRows(workbook *excelize.File, sheet string, rows [][]string, cols columns, fixedStatuses map[string]bool, sourceColumn string) (map[string]*iinGroup, []*iinGroup, map[loginKey]*load, map[loginKey]map[string]bool, int, int) {
	groups := make(map[string]*iinGroup)
	groupOrder := make([]*iinGroup, 0)
	loads := make(map[loginKey]*load)
	loginIINs := make(map[loginKey]map[string]bool)
	pinnedIINs := make(map[string]map[string]bool)
	fixedIINs := make(map[string]bool)
	fixedCount := 0

	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		rowNumber := rowIndex + 1
		row := rows[rowIndex]
		if isRepeatedHeaderRow(row, cols) {
			continue
		}
		rp := normalizeRP(getRowCell(row, cols.rp))
		iin := normalizeIIN(getRowCell(row, cols.iin))
		currentLogin := readSourceLoginFromRow(row, cols, sourceColumn)
		if rp == "" || iin == "" || currentLogin == "" {
			continue
		}

		groupKey := makeGroupKey(rp, iin)
		login := loginKey{rp: rp, login: currentLogin}
		amount := toDecimal(getRowCell(row, cols.amount))
		status := normalizeStatus(getRowCell(row, cols.status))
		if fixedStatuses[status] {
			addLoad(loads, loginIINs, login, iin, amount, 1)
			_ = setCell(workbook, sheet, rowNumber, cols.attach, currentLogin)
			if pinnedIINs[groupKey] == nil {
				pinnedIINs[groupKey] = make(map[string]bool)
			}
			pinnedIINs[groupKey][currentLogin] = true
			fixedIINs[groupKey] = true
			fixedCount++
			continue
		}

		if groups[groupKey] == nil {
			groups[groupKey] = &iinGroup{rp: rp, iin: iin, currentLogin: currentLogin}
			groupOrder = append(groupOrder, groups[groupKey])
		}
		groups[groupKey].rows = append(groups[groupKey].rows, rowNumber)
		groups[groupKey].amount = pyAdd(groups[groupKey].amount, amount)
	}

	for groupKey, logins := range pinnedIINs {
		if group, ok := groups[groupKey]; ok && len(logins) == 1 {
			for login := range logins {
				group.pinnedLogin = login
			}
		}
	}
	return groups, groupOrder, loads, loginIINs, fixedCount, len(fixedIINs)
}

func readLoginKeys(workbook *excelize.File, sheet string, maxRow int, cols columns, sourceColumn string) []loginKey {
	rows, err := workbook.GetRows(sheet)
	if err != nil {
		return nil
	}
	if maxRow > 0 && maxRow < len(rows) {
		rows = rows[:maxRow]
	}
	return readLoginKeysFromRows(rows, cols, sourceColumn)
}

func readLoginKeysFromRows(rows [][]string, cols columns, sourceColumn string) []loginKey {
	var keys []loginKey
	seen := make(map[loginKey]bool)
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		row := rows[rowIndex]
		if isRepeatedHeaderRow(row, cols) {
			continue
		}
		rp := normalizeRP(getRowCell(row, cols.rp))
		login := readSourceLoginFromRow(row, cols, sourceColumn)
		key := loginKey{rp: rp, login: login}
		if rp != "" && login != "" && !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].rp == keys[j].rp {
			return keys[i].login < keys[j].login
		}
		return keys[i].rp < keys[j].rp
	})
	return keys
}

func readSourceLogin(workbook *excelize.File, sheet string, rowNumber int, cols columns, sourceColumn string) string {
	if sourceColumn == "attach" {
		attachedLogin := normalizeLogin(getCell(workbook, sheet, rowNumber, cols.attach))
		if attachedLogin != "" && !isHeaderLikeLogin(attachedLogin) {
			return attachedLogin
		}
	}
	return normalizeSourceLogin(getCell(workbook, sheet, rowNumber, cols.detach))
}

func readSourceLoginFromRow(row []string, cols columns, sourceColumn string) string {
	if sourceColumn == "attach" {
		attachedLogin := normalizeLogin(getRowCell(row, cols.attach))
		if attachedLogin != "" && !isHeaderLikeLogin(attachedLogin) {
			return attachedLogin
		}
	}
	return normalizeSourceLogin(getRowCell(row, cols.detach))
}

func ensureLoadsForAllLogins(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeys []loginKey) {
	for _, key := range loginKeys {
		if loads[key] == nil {
			loads[key] = &load{}
		}
		if loginIINs[key] == nil {
			loginIINs[key] = make(map[string]bool)
		}
	}
}

func addLoad(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, key loginKey, iin string, amount decimal.Decimal, materialCount int) {
	if loads[key] == nil {
		loads[key] = &load{}
	}
	if loginIINs[key] == nil {
		loginIINs[key] = make(map[string]bool)
	}
	loads[key].count += materialCount
	loads[key].amount = pyAdd(loads[key].amount, amount)
	if !loginIINs[key][iin] {
		loginIINs[key][iin] = true
		loads[key].iinCount++
	}
}

func collectFinalLoads(workbook *excelize.File, sheet string, cols columns) (map[loginKey]*load, map[loginKey]map[string]bool) {
	rows, err := workbook.GetRows(sheet)
	if err != nil {
		rows = nil
	}
	loads := make(map[loginKey]*load)
	loginIINs := make(map[loginKey]map[string]bool)
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		row := rows[rowIndex]
		if isRepeatedHeaderRow(row, cols) {
			continue
		}
		rp := normalizeRP(getRowCell(row, cols.rp))
		iin := normalizeIIN(getRowCell(row, cols.iin))
		login := normalizeLogin(getRowCell(row, cols.attach))
		if rp == "" || iin == "" || login == "" {
			continue
		}
		key := loginKey{rp: rp, login: login}
		amount := toDecimal(getRowCell(row, cols.amount))
		addLoad(loads, loginIINs, key, iin, amount, 1)
	}
	return loads, loginIINs
}

func ensureSourceRPColumn(workbook *excelize.File, sheet string, rows [][]string, cols columns) (int, error) {
	if cols.sourceRP > 0 {
		return cols.sourceRP, nil
	}

	column := max(len(rows[0])+1, maxColumn(cols)+1)
	if err := setCell(workbook, sheet, 1, column, "Исходное рп"); err != nil {
		return 0, err
	}
	return column, nil
}

func maxColumn(cols columns) int {
	return max(cols.rp, max(cols.iin, max(cols.detach, max(cols.attach, max(cols.status, max(cols.amount, cols.sourceRP))))))
}

func fillSourceRPColumn(workbook *excelize.File, sheet string, rows [][]string, cols columns) {
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		row := rows[rowIndex]
		if isRepeatedHeaderRow(row, cols) {
			continue
		}
		rp := normalizeRP(getRowCell(row, cols.rp))
		if rp == "" {
			continue
		}
		_ = setCell(workbook, sheet, rowIndex+1, cols.sourceRP, rp)
	}
}

func normalizeSourceLogin(value string) string {
	login := normalizeLogin(value)
	if isHeaderLikeLogin(login) {
		return ""
	}
	return login
}

func isHeaderLikeLogin(login string) bool {
	switch normalizeHeader(login) {
	case "закрепить", "открепить", "логин", "рп", "иин", "статус":
		return true
	default:
		return false
	}
}

func isRepeatedHeaderRow(row []string, cols columns) bool {
	matches := 0
	checks := []struct {
		col  int
		name string
	}{
		{cols.rp, "рп"},
		{cols.iin, "иин"},
		{cols.detach, "открепить"},
		{cols.attach, "закрепить"},
		{cols.status, "статус"},
	}
	for _, check := range checks {
		if normalizeHeader(getRowCell(row, check.col)) == check.name {
			matches++
		}
	}
	return matches >= 3
}

func removeLoad(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, key loginKey, iin string, amount decimal.Decimal, materialCount int) {
	loads[key].count -= materialCount
	loads[key].amount = pySub(loads[key].amount, amount)
	if loginIINs[key][iin] {
		delete(loginIINs[key], iin)
		loads[key].iinCount--
	}
}

func balanceGroupsParallel(ctx context.Context, groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, progress progressFunc) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	loginKeysByRP := groupLoginKeysByRP(loginKeys)
	groupsByRP := groupGroupsByRP(groups)
	targetsByRP := make(map[string][3]decimal.Decimal)
	for rp, rpLoginKeys := range loginKeysByRP {
		targetsByRP[rp] = targetsForRP(groupsByRP[rp], rpLoginKeys, loads, loginIINs)
	}
	reportProgress(progress, 50, "Целевая нагрузка по РП рассчитана")

	for rp, rpGroups := range groupsByRP {
		targets := targetsByRP[rp]
		sort.SliceStable(rpGroups, func(i, j int) bool {
			return groupWeight(rpGroups[i], targets).GreaterThan(groupWeight(rpGroups[j], targets))
		})
	}
	reportProgress(progress, 52, fmt.Sprintf("Группы отсортированы по весу: %d ИИН", len(groups)))

	results := make(chan rpBalanceResult, len(groupsByRP))
	counter := newProgressCounter(progress, 52, 62, len(groups), "Параллельное первичное распределение ИИН")
	for rp, rpGroups := range groupsByRP {
		rp := rp
		rpGroups := rpGroups
		rpLoginKeys := loginKeysByRP[rp]
		targets := targetsByRP[rp]
		go func() {
			if len(rpLoginKeys) == 0 {
				results <- rpBalanceResult{rp: rp, err: fmt.Errorf("Для РП %q нет логинов для распределения.", rp)}
				return
			}
			localLoads, localLoginIINs := clonePortfolioForLogins(loads, loginIINs, rpLoginKeys)
			localAssignments, err := balanceGroupsForRP(ctx, rpGroups, rpLoginKeys, localLoads, localLoginIINs, targets, counter)
			results <- rpBalanceResult{
				rp:          rp,
				assignments: localAssignments,
				loads:       localLoads,
				loginIINs:   localLoginIINs,
				err:         err,
			}
		}()
	}

	for completed := 0; completed < len(groupsByRP); completed++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-results:
			if result.err != nil {
				return nil, result.err
			}
			mergeAssignments(assignments, result.assignments)
			mergePortfolio(loads, loginIINs, result.loads, result.loginIINs)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := improveAssignmentsParallel(ctx, groups, assignments, loads, loginIINs, loginKeysByRP, targetsByRP, rotationScoreWeights, progress); err != nil {
		return nil, err
	}
	return assignments, nil
}

func balanceGroupsForRP(ctx context.Context, groups []*iinGroup, rpLoginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, targets [3]decimal.Decimal, counter *progressCounter) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		groupKey := groupAssignmentKey(group)
		if len(rpLoginKeys) == 0 {
			return nil, fmt.Errorf("Для РП %q нет логинов для распределения.", group.rp)
		}
		var selected loginKey
		if group.pinnedLogin != "" {
			selected = loginKey{rp: group.rp, login: group.pinnedLogin}
		} else {
			baseScores := loginScoresForLogins(loads, rpLoginKeys, targets, rotationScoreWeights)
			selected, _ = bestCandidateAfterAdd(loads, loginIINs, rpLoginKeys, baseScores, group, targets, rotationScoreWeights)
		}
		assignments[groupKey] = selected
		addLoad(loads, loginIINs, selected, group.iin, group.amount, len(group.rows))
		counter.add(1)
	}
	return assignments, nil
}

func partiallyBalanceGroups(ctx context.Context, groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, progress progressFunc) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	loginKeysByRP := groupLoginKeysByRP(loginKeys)
	targetsByRP := make(map[string][3]decimal.Decimal)
	for rp, rpLoginKeys := range loginKeysByRP {
		targetsByRP[rp] = targetsForRP(groupsForRP(groups, rp), rpLoginKeys, loads, loginIINs)
	}
	reportProgress(progress, 50, "Целевая нагрузка по РП рассчитана")

	counter := newProgressCounter(progress, 50, 58, len(groups), "Читаю текущее закрепление ИИН")
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		groupKey := groupAssignmentKey(group)
		login := loginKey{rp: group.rp, login: firstNonEmpty(group.pinnedLogin, group.currentLogin)}
		if !containsLoginKey(loginKeysByRP[group.rp], login) {
			return nil, fmt.Errorf("Для ИИН %s не найден текущий логин в РП %q.", group.iin, group.rp)
		}
		assignments[groupKey] = login
		addLoad(loads, loginIINs, login, group.iin, group.amount, len(group.rows))
		counter.add(1)
	}

	if err := improvePartialAssignments(ctx, groups, assignments, loads, loginIINs, loginKeysByRP, targetsByRP, alignmentScoreWeights, alignmentMoveLimits(groups), progress, nil); err != nil {
		return nil, err
	}
	return assignments, nil
}

func balanceGroupsAcrossRP(ctx context.Context, groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, progress progressFunc) (map[string]loginKey, error) {
	if len(loginKeys) == 0 {
		return nil, errors.New("Не найдены логины для распределения между РП.")
	}
	loginKeysByRP := groupLoginKeysByRP(loginKeys)
	for rp := range groupGroupsByRP(groups) {
		if len(loginKeysByRP[rp]) == 0 {
			return nil, fmt.Errorf("Для РП %q нет логинов для распределения.", rp)
		}
	}

	reportProgress(progress, 50, "Сначала рассчитываю полноценную ротацию внутри каждого РП")
	targetRPByGroup, err := exchangeGroupsBetweenRP(ctx, groups, loginKeysByRP, loads, progress)
	if err != nil {
		return nil, err
	}
	targetedGroups := groupsWithTargetRP(groups, targetRPByGroup)
	reportProgress(progress, 58, "Обмен между РП подобран: сколько материалов пришло, столько же ушло")
	assignments, err := balanceGroupsParallel(ctx, targetedGroups, loginKeys, loads, loginIINs, progress)
	if err != nil {
		return nil, err
	}
	reportProgress(progress, 68, "Финальная ротация внутри целевых РП завершена")
	return assignments, nil
}

func exchangeGroupsBetweenRP(ctx context.Context, groups []*iinGroup, loginKeysByRP map[string][]loginKey, fixedLoads map[loginKey]*load, progress progressFunc) (map[string]string, error) {
	targetRPByGroup := make(map[string]string, len(groups))
	for _, group := range groups {
		targetRPByGroup[groupAssignmentKey(group)] = group.rp
	}

	movableByRPAndCount := movableGroupsByRPAndCount(groups)
	maxSwaps := len(groups)
	lastPercent := 50
	for swaps := 0; swaps < maxSwaps; swaps++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stats := rpAverageStats(groups, targetRPByGroup, loginKeysByRP, fixedLoads)
		highRP, lowRP, ok := widestRPAverageGap(stats)
		if !ok {
			break
		}
		highGroup, lowGroup, ok := bestBalancedExchange(movableByRPAndCount, targetRPByGroup, highRP, lowRP)
		if !ok {
			break
		}
		highKey := groupAssignmentKey(highGroup)
		lowKey := groupAssignmentKey(lowGroup)
		targetRPByGroup[highKey] = lowRP
		targetRPByGroup[lowKey] = highRP
		lastPercent = reportRangeProgress(progress, 50, 58, swaps+1, maxSwaps, lastPercent, "Подбираю равноценный обмен материалами между РП")
	}
	return targetRPByGroup, nil
}

func movableGroupsByRPAndCount(groups []*iinGroup) map[string]map[int][]*iinGroup {
	out := make(map[string]map[int][]*iinGroup)
	for _, group := range groups {
		if group.pinnedLogin != "" {
			continue
		}
		if out[group.rp] == nil {
			out[group.rp] = make(map[int][]*iinGroup)
		}
		out[group.rp][len(group.rows)] = append(out[group.rp][len(group.rows)], group)
	}
	for rp, byCount := range out {
		for count := range byCount {
			sort.SliceStable(byCount[count], func(i, j int) bool {
				return byCount[count][i].amount.GreaterThan(byCount[count][j].amount)
			})
		}
		out[rp] = byCount
	}
	return out
}

type rpAverageStat struct {
	rp     string
	amount decimal.Decimal
	logins int
	avg    decimal.Decimal
}

func rpAverageStats(groups []*iinGroup, targetRPByGroup map[string]string, loginKeysByRP map[string][]loginKey, fixedLoads map[loginKey]*load) map[string]rpAverageStat {
	stats := make(map[string]rpAverageStat)
	for rp, logins := range loginKeysByRP {
		stat := stats[rp]
		stat.rp = rp
		stat.logins = len(logins)
		for _, login := range logins {
			if fixedLoads[login] != nil {
				stat.amount = pyAdd(stat.amount, fixedLoads[login].amount)
			}
		}
		stats[rp] = stat
	}
	for _, group := range groups {
		targetRP := targetRPByGroup[groupAssignmentKey(group)]
		stat := stats[targetRP]
		stat.rp = targetRP
		stat.amount = pyAdd(stat.amount, group.amount)
		stats[targetRP] = stat
	}
	for rp, stat := range stats {
		if stat.logins > 0 {
			stat.avg = pyDiv(stat.amount, decimal.NewFromInt(int64(stat.logins)))
		}
		stats[rp] = stat
	}
	return stats
}

func widestRPAverageGap(stats map[string]rpAverageStat) (string, string, bool) {
	var high rpAverageStat
	var low rpAverageStat
	hasHigh := false
	hasLow := false
	for _, stat := range stats {
		if stat.logins == 0 {
			continue
		}
		if !hasHigh || stat.avg.GreaterThan(high.avg) {
			high = stat
			hasHigh = true
		}
		if !hasLow || stat.avg.LessThan(low.avg) {
			low = stat
			hasLow = true
		}
	}
	if !hasHigh || !hasLow || high.rp == low.rp || high.avg.LessThanOrEqual(low.avg) {
		return "", "", false
	}
	return high.rp, low.rp, true
}

func bestBalancedExchange(groupsByRPAndCount map[string]map[int][]*iinGroup, targetRPByGroup map[string]string, highRP, lowRP string) (*iinGroup, *iinGroup, bool) {
	var bestHigh *iinGroup
	var bestLow *iinGroup
	bestImprovement := decimal.Zero
	for count, highGroups := range groupsByRPAndCount[highRP] {
		lowGroups := groupsByRPAndCount[lowRP][count]
		for _, highGroup := range highGroups {
			if targetRPByGroup[groupAssignmentKey(highGroup)] != highRP {
				continue
			}
			for lowIndex := len(lowGroups) - 1; lowIndex >= 0; lowIndex-- {
				lowGroup := lowGroups[lowIndex]
				if targetRPByGroup[groupAssignmentKey(lowGroup)] != lowRP {
					continue
				}
				improvement := pySub(highGroup.amount, lowGroup.amount)
				if improvement.GreaterThan(bestImprovement) {
					bestImprovement = improvement
					bestHigh = highGroup
					bestLow = lowGroup
				}
				break
			}
		}
	}
	if bestHigh == nil || bestLow == nil || !bestImprovement.GreaterThan(decimal.Zero) {
		return nil, nil, false
	}
	return bestHigh, bestLow, true
}

func groupsWithTargetRP(groups []*iinGroup, targetRPByGroup map[string]string) []*iinGroup {
	out := make([]*iinGroup, 0, len(groups))
	for _, group := range groups {
		targetRP := targetRPByGroup[groupAssignmentKey(group)]
		if targetRP == "" {
			targetRP = group.rp
		}
		cloned := *group
		cloned.sourceRP = firstNonEmpty(group.sourceRP, group.rp)
		cloned.rp = targetRP
		if cloned.rp != cloned.sourceRP {
			cloned.pinnedLogin = ""
		}
		out = append(out, &cloned)
	}
	return out
}

func groupLoginKeysByRP(loginKeys []loginKey) map[string][]loginKey {
	out := make(map[string][]loginKey)
	for _, key := range loginKeys {
		out[key.rp] = append(out[key.rp], key)
	}
	return out
}

func rowsBySourceRP(groups []*iinGroup) map[string]int {
	rows := make(map[string]int)
	for _, group := range groups {
		rows[group.rp] += len(group.rows)
	}
	return rows
}

func amountBySourceRP(groups []*iinGroup) map[string]decimal.Decimal {
	amounts := make(map[string]decimal.Decimal)
	for _, group := range groups {
		amounts[group.rp] = pyAdd(amounts[group.rp], group.amount)
	}
	return amounts
}

func loginKeysWithCapacity(loginKeys []loginKey, remainingRowsByRP map[string]int, remainingAmountByRP map[string]decimal.Decimal, group *iinGroup) []loginKey {
	var candidates []loginKey
	for _, key := range loginKeys {
		if remainingRowsByRP[key.rp] >= len(group.rows) && remainingAmountByRP[key.rp].GreaterThanOrEqual(group.amount) {
			candidates = append(candidates, key)
		}
	}
	return candidates
}

func fallbackLoginKeys(loginKeys []loginKey, remainingRowsByRP map[string]int, sourceRP string) []loginKey {
	var candidates []loginKey
	for _, key := range loginKeys {
		if remainingRowsByRP[key.rp] > 0 || key.rp == sourceRP {
			candidates = append(candidates, key)
		}
	}
	return candidates
}

func groupGroupsByRP(groups []*iinGroup) map[string][]*iinGroup {
	out := make(map[string][]*iinGroup)
	for _, group := range groups {
		out[group.rp] = append(out[group.rp], group)
	}
	return out
}

func clonePortfolioForLogins(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeys []loginKey) (map[loginKey]*load, map[loginKey]map[string]bool) {
	clonedLoads := make(map[loginKey]*load, len(loginKeys))
	clonedLoginIINs := make(map[loginKey]map[string]bool, len(loginKeys))
	for _, key := range loginKeys {
		sourceLoad := loads[key]
		if sourceLoad == nil {
			sourceLoad = &load{}
		}
		clonedLoads[key] = &load{
			count:    sourceLoad.count,
			amount:   sourceLoad.amount,
			iinCount: sourceLoad.iinCount,
		}
		clonedLoginIINs[key] = cloneStringSet(loginIINs[key])
	}
	return clonedLoads, clonedLoginIINs
}

func cloneStringSet(source map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(source))
	for value := range source {
		cloned[value] = true
	}
	return cloned
}

func mergeAssignments(target map[string]loginKey, source map[string]loginKey) {
	for key, value := range source {
		target[key] = value
	}
}

func mergePortfolio(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, sourceLoads map[loginKey]*load, sourceLoginIINs map[loginKey]map[string]bool) {
	for key, sourceLoad := range sourceLoads {
		loads[key] = &load{
			count:    sourceLoad.count,
			amount:   sourceLoad.amount,
			iinCount: sourceLoad.iinCount,
		}
		loginIINs[key] = cloneStringSet(sourceLoginIINs[key])
	}
}

func filterAssignmentsForGroups(assignments map[string]loginKey, groups []*iinGroup) map[string]loginKey {
	filtered := make(map[string]loginKey, len(groups))
	for _, group := range groups {
		groupKey := groupAssignmentKey(group)
		filtered[groupKey] = assignments[groupKey]
	}
	return filtered
}

func targetsForRP(groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool) [3]decimal.Decimal {
	loginCount := decimal.NewFromInt(int64(len(loginKeys)))
	totalCount := 0
	totalIIN := 0
	totalAmount := decimal.Zero
	for _, key := range loginKeys {
		totalCount += loads[key].count
		totalIIN += loads[key].iinCount
		totalAmount = pyAdd(totalAmount, loads[key].amount)
	}
	for _, group := range groups {
		totalCount += len(group.rows)
		totalAmount = pyAdd(totalAmount, group.amount)
		if group.pinnedLogin == "" || !loginIINs[loginKey{rp: group.rp, login: group.pinnedLogin}][group.iin] {
			totalIIN++
		}
	}
	if loginCount.IsZero() {
		return [3]decimal.Decimal{decimal.Zero, decimal.NewFromInt(1), decimal.Zero}
	}
	targetCount := pyDiv(decimal.NewFromInt(int64(totalCount)), loginCount)
	targetIIN := pyDiv(decimal.NewFromInt(int64(totalIIN)), loginCount)
	targetAmount := pyDiv(totalAmount, loginCount)
	if targetAmount.IsZero() {
		targetAmount = decimal.NewFromInt(1)
	}
	return [3]decimal.Decimal{targetIIN, targetAmount, targetCount}
}

func targetsForLogins(groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool) [3]decimal.Decimal {
	loginCount := decimal.NewFromInt(int64(len(loginKeys)))
	totalCount := 0
	totalIIN := 0
	totalAmount := decimal.Zero
	for _, key := range loginKeys {
		totalCount += loads[key].count
		totalIIN += loads[key].iinCount
		totalAmount = pyAdd(totalAmount, loads[key].amount)
	}
	for _, group := range groups {
		totalCount += len(group.rows)
		totalAmount = pyAdd(totalAmount, group.amount)
		if group.pinnedLogin == "" || !loginIINs[loginKey{rp: group.rp, login: group.pinnedLogin}][group.iin] {
			totalIIN++
		}
	}
	if loginCount.IsZero() {
		return [3]decimal.Decimal{decimal.Zero, decimal.NewFromInt(1), decimal.Zero}
	}
	targetCount := pyDiv(decimal.NewFromInt(int64(totalCount)), loginCount)
	targetIIN := pyDiv(decimal.NewFromInt(int64(totalIIN)), loginCount)
	targetAmount := pyDiv(totalAmount, loginCount)
	if targetAmount.IsZero() {
		targetAmount = decimal.NewFromInt(1)
	}
	return [3]decimal.Decimal{targetIIN, targetAmount, targetCount}
}

func groupWeight(group *iinGroup, targets [3]decimal.Decimal) decimal.Decimal {
	targetAmount := targets[1]
	targetCount := targets[2]
	amountWeight := decimal.Zero
	countWeight := decimal.Zero
	if !targetAmount.IsZero() {
		amountWeight = pyDiv(group.amount, targetAmount)
	}
	if !targetCount.IsZero() {
		countWeight = pyDiv(decimal.NewFromInt(int64(len(group.rows))), targetCount)
	}
	return pyAdd(pyAdd(amountWeight, countWeight), decimal.NewFromInt(1))
}

func scoreAfterAdd(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, logins []loginKey, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	score := decimal.Zero
	for _, item := range logins {
		itemLoad := loads[item]
		amount := itemLoad.amount
		count := itemLoad.count
		iinCount := itemLoad.iinCount
		if item == login {
			amount = pyAdd(amount, group.amount)
			count += len(group.rows)
			if !loginIINs[item][group.iin] {
				iinCount++
			}
		}
		score = pyAdd(score, loadScore(iinCount, amount, count, targets, weights))
	}
	return score
}

func scoreAfterAddFromScores(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, baseScore decimal.Decimal, baseScores map[loginKey]decimal.Decimal, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	amount := pyAdd(item.amount, group.amount)
	count := item.count + len(group.rows)
	iinCount := item.iinCount
	if !loginIINs[login][group.iin] {
		iinCount++
	}
	return pyAdd(pySub(baseScore, baseScores[login]), loadScore(iinCount, amount, count, targets, weights))
}

func bestCandidateAfterAdd(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, candidates []loginKey, baseScores map[loginKey]decimal.Decimal, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) (loginKey, decimal.Decimal) {
	return bestCandidateAfterAddWithBaseline(loads, loginIINs, candidates, baseScores, group, targets, weights, candidates[0], decimal.Zero, false)
}

func bestCandidateAfterAddWithBaseline(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, candidates []loginKey, baseScores map[loginKey]decimal.Decimal, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights, defaultLogin loginKey, defaultScore decimal.Decimal, hasDefault bool) (loginKey, decimal.Decimal) {
	baseScore := scoreFromScores(candidates, baseScores)
	if len(candidates) < 12 {
		bestLogin := defaultLogin
		bestScore := defaultScore
		hasBest := hasDefault
		for _, candidate := range candidates {
			score := scoreAfterAddFromScores(loads, loginIINs, baseScore, baseScores, candidate, group, targets, weights)
			if !hasBest || score.LessThan(bestScore) {
				bestScore = score
				bestLogin = candidate
				hasBest = true
			}
		}
		return bestLogin, bestScore
	}

	scores := make([]decimal.Decimal, len(candidates))
	workerCount := min(runtime.GOMAXPROCS(0), len(candidates))
	jobs := make(chan int, len(candidates))
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				candidate := candidates[index]
				scores[index] = scoreAfterAddFromScores(loads, loginIINs, baseScore, baseScores, candidate, group, targets, weights)
			}
		}()
	}
	for index := range candidates {
		jobs <- index
	}
	close(jobs)
	wg.Wait()

	bestLogin := defaultLogin
	bestScore := defaultScore
	hasBest := hasDefault
	for index, candidateScore := range scores {
		if !hasBest || candidateScore.LessThan(bestScore) {
			bestScore = candidateScore
			bestLogin = candidates[index]
			hasBest = true
		}
	}
	return bestLogin, bestScore
}

func loginScoresForLogins(loads map[loginKey]*load, logins []loginKey, targets [3]decimal.Decimal, weights scoreWeights) map[loginKey]decimal.Decimal {
	scores := make(map[loginKey]decimal.Decimal, len(logins))
	for _, login := range logins {
		scores[login] = loginScore(loads, login, targets, weights)
	}
	return scores
}

func cloneScoreMap(source map[loginKey]decimal.Decimal) map[loginKey]decimal.Decimal {
	cloned := make(map[loginKey]decimal.Decimal, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func scoreFromScores(logins []loginKey, scores map[loginKey]decimal.Decimal) decimal.Decimal {
	score := decimal.Zero
	for _, login := range logins {
		score = pyAdd(score, scores[login])
	}
	return score
}

func loginScoreAfterAdd(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	amount := pyAdd(item.amount, group.amount)
	count := item.count + len(group.rows)
	iinCount := item.iinCount
	if !loginIINs[login][group.iin] {
		iinCount++
	}
	return loadScore(iinCount, amount, count, targets, weights)
}

func loadScore(iinCount int, amount decimal.Decimal, materialCount int, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	targetIIN, targetAmount, targetCount := targets[0], targets[1], targets[2]
	amountDelta := decimal.Zero
	countDelta := decimal.Zero
	iinDelta := decimal.Zero
	if !targetAmount.IsZero() {
		amountDelta = pyDiv(pySub(amount, targetAmount), targetAmount)
	}
	if !targetCount.IsZero() {
		countDelta = pyDiv(pySub(decimal.NewFromInt(int64(materialCount)), targetCount), targetCount)
	}
	if !targetIIN.IsZero() {
		iinDelta = pyDiv(pySub(decimal.NewFromInt(int64(iinCount)), targetIIN), targetIIN)
	}
	amountScore := pyMul(pyMul(amountDelta, amountDelta), weights.amount)
	countScore := pyMul(pyMul(countDelta, countDelta), weights.materialCount)
	iinScore := pyMul(pyMul(iinDelta, iinDelta), weights.iinCount)
	return pyAdd(pyAdd(amountScore, countScore), iinScore)
}

func loginScore(loads map[loginKey]*load, login loginKey, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	return loadScore(item.iinCount, item.amount, item.count, targets, weights)
}

func portfolioScore(loads map[loginKey]*load, logins []loginKey, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	score := decimal.Zero
	for _, login := range logins {
		score = pyAdd(score, loginScore(loads, login, targets, weights))
	}
	return score
}

func improveAssignments(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, progress progressFunc, counter *progressCounter) error {
	var movableGroups []*iinGroup
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableGroups = append(movableGroups, group)
		}
	}
	reportProgress(progress, 63, fmt.Sprintf("Уточняю распределение: можно переместить %d ИИН", len(movableGroups)))

	for i := 0; i < 4; i++ {
		changed := false
		lastPercent := 63 + i
		for index, group := range movableGroups {
			if err := ctx.Err(); err != nil {
				return err
			}
			groupKey := groupAssignmentKey(group)
			rpLoginKeys := loginKeysByRP[group.rp]
			targets := targetsByRP[group.rp]
			currentScores := loginScoresForLogins(loads, rpLoginKeys, targets, weights)
			currentScore := scoreFromScores(rpLoginKeys, currentScores)
			currentLogin := assignments[groupKey]
			bestLogin := currentLogin
			bestScore := currentScore

			removeLoad(loads, loginIINs, currentLogin, group.iin, group.amount, len(group.rows))
			removedScores := cloneScoreMap(currentScores)
			removedScores[currentLogin] = loginScore(loads, currentLogin, targets, weights)
			bestLogin, bestScore = bestCandidateAfterAddWithBaseline(loads, loginIINs, rpLoginKeys, removedScores, group, targets, weights, bestLogin, bestScore, true)

			addLoad(loads, loginIINs, bestLogin, group.iin, group.amount, len(group.rows))
			if bestLogin != currentLogin {
				assignments[groupKey] = bestLogin
				changed = true
			}
			counter.add(1)
			lastPercent = reportRangeProgress(
				progress,
				63+i,
				64+i,
				index+1,
				len(movableGroups),
				lastPercent,
				fmt.Sprintf("Уточнение распределения, проход %d из 4", i+1),
			)
		}
		if !changed {
			reportProgress(progress, 68, "Уточнение завершено: улучшений больше нет")
			return nil
		}
	}
	return nil
}

func improveAssignmentsParallel(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, progress progressFunc) error {
	var movableCount int
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableCount++
		}
	}
	reportProgress(progress, 63, fmt.Sprintf("Уточняю распределение параллельно по РП: можно переместить %d ИИН", movableCount))

	groupsByRP := groupGroupsByRP(groups)
	results := make(chan rpBalanceResult, len(groupsByRP))
	counter := newProgressCounter(progress, 63, 68, movableCount*4, "Уточнение распределения")
	for rp, rpGroups := range groupsByRP {
		rp := rp
		rpGroups := rpGroups
		rpLoginKeys := loginKeysByRP[rp]
		localLoads, localLoginIINs := clonePortfolioForLogins(loads, loginIINs, rpLoginKeys)
		localAssignments := filterAssignmentsForGroups(assignments, rpGroups)
		go func() {
			err := improveAssignments(ctx, rpGroups, localAssignments, localLoads, localLoginIINs, map[string][]loginKey{rp: rpLoginKeys}, map[string][3]decimal.Decimal{rp: targetsByRP[rp]}, weights, nil, counter)
			results <- rpBalanceResult{
				rp:          rp,
				assignments: localAssignments,
				loads:       localLoads,
				loginIINs:   localLoginIINs,
				err:         err,
			}
		}()
	}

	completedGroups := 0
	lastPercent := 63
	for range groupsByRP {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if result.err != nil {
				return result.err
			}
			mergeAssignments(assignments, result.assignments)
			mergePortfolio(loads, loginIINs, result.loads, result.loginIINs)
			completedGroups += len(groupsByRP[result.rp])
			lastPercent = reportRangeProgress(
				progress,
				63,
				68,
				completedGroups,
				len(groups),
				lastPercent,
				"Уточнение распределения по РП",
			)
		}
	}
	reportProgress(progress, 68, "Уточнение завершено")
	return nil
}

func improvePartialAssignments(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, moveLimitsByRP map[string]int, progress progressFunc, counter *progressCounter) error {
	movableByRP := make(map[string][]*iinGroup)
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableByRP[group.rp] = append(movableByRP[group.rp], group)
		}
	}

	movedGroups := make(map[string]bool)
	totalLimit := 0
	for _, limit := range moveLimitsByRP {
		totalLimit += limit
	}
	checkedMoves := 0
	lastPercent := 58
	reportProgress(progress, 59, fmt.Sprintf("Ищу полезные перемещения ИИН, лимит перемещений: %d", totalLimit))
	for rp, groupsInRP := range movableByRP {
		rpLoginKeys := loginKeysByRP[rp]
		targets := targetsByRP[rp]
		moveLimit := moveLimitsByRP[rp]

		for i := 0; i < moveLimit; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			checkedMoves++
			baseScores := loginScoresForLogins(loads, rpLoginKeys, targets, weights)
			currentScore := scoreFromScores(rpLoginKeys, baseScores)
			bestGroup, bestLogin, ok := bestPartialMoveCandidate(ctx, groupsInRP, assignments, loads, loginIINs, rpLoginKeys, baseScores, targets, weights, movedGroups, currentScore)
			if counter != nil {
				counter.add(1)
			}
			if !ok {
				break
			}
			groupKey := groupAssignmentKey(bestGroup)
			currentLogin := assignments[groupKey]
			removeLoad(loads, loginIINs, currentLogin, bestGroup.iin, bestGroup.amount, len(bestGroup.rows))
			addLoad(loads, loginIINs, bestLogin, bestGroup.iin, bestGroup.amount, len(bestGroup.rows))
			assignments[groupKey] = bestLogin
			movedGroups[groupKey] = true
			if counter == nil {
				lastPercent = reportRangeProgress(
					progress,
					59,
					68,
					checkedMoves,
					max(totalLimit, 1),
					lastPercent,
					fmt.Sprintf("Выравнивание РП %s: перемещено ИИН %d", rp, len(movedGroups)),
				)
			}
		}
	}
	reportProgress(progress, 68, fmt.Sprintf("Выравнивание подобрано, перемещено ИИН: %d", len(movedGroups)))
	return nil
}

func bestPartialMoveCandidate(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, rpLoginKeys []loginKey, baseScores map[loginKey]decimal.Decimal, targets [3]decimal.Decimal, weights scoreWeights, movedGroups map[string]bool, currentScore decimal.Decimal) (*iinGroup, loginKey, bool) {
	if len(groups) > 1 && len(rpLoginKeys) > 0 && runtime.GOMAXPROCS(0) > 1 {
		candidate := bestPartialMoveCandidateParallel(ctx, groups, assignments, loads, loginIINs, rpLoginKeys, baseScores, targets, weights, movedGroups, currentScore)
		return candidate.group, candidate.login, candidate.ok
	}

	candidate := bestPartialMoveCandidateRange(ctx, groups, assignments, loads, loginIINs, rpLoginKeys, baseScores, targets, weights, movedGroups, currentScore, 0, len(groups))
	return candidate.group, candidate.login, candidate.ok
}

func bestPartialMoveCandidateParallel(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, rpLoginKeys []loginKey, baseScores map[loginKey]decimal.Decimal, targets [3]decimal.Decimal, weights scoreWeights, movedGroups map[string]bool, currentScore decimal.Decimal) partialMoveCandidate {
	workerCount := min(runtime.GOMAXPROCS(0), len(groups))
	if workerCount <= 1 {
		return bestPartialMoveCandidateRange(ctx, groups, assignments, loads, loginIINs, rpLoginKeys, baseScores, targets, weights, movedGroups, currentScore, 0, len(groups))
	}

	results := make(chan partialMoveCandidate, workerCount)
	chunkSize := (len(groups) + workerCount - 1) / workerCount
	for worker := 0; worker < workerCount; worker++ {
		start := worker * chunkSize
		end := min(start+chunkSize, len(groups))
		if start >= end {
			results <- partialMoveCandidate{}
			continue
		}
		go func(start int, end int) {
			results <- bestPartialMoveCandidateRange(ctx, groups, assignments, loads, loginIINs, rpLoginKeys, baseScores, targets, weights, movedGroups, currentScore, start, end)
		}(start, end)
	}

	best := partialMoveCandidate{score: currentScore}
	for i := 0; i < workerCount; i++ {
		select {
		case <-ctx.Done():
			return partialMoveCandidate{}
		case candidate := <-results:
			if betterPartialMoveCandidate(candidate, best) {
				best = candidate
			}
		}
	}
	return best
}

func bestPartialMoveCandidateRange(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, rpLoginKeys []loginKey, baseScores map[loginKey]decimal.Decimal, targets [3]decimal.Decimal, weights scoreWeights, movedGroups map[string]bool, currentScore decimal.Decimal, start int, end int) partialMoveCandidate {
	bestScore := currentScore
	best := partialMoveCandidate{score: bestScore}

	for groupIndex := start; groupIndex < end; groupIndex++ {
		if groupIndex%64 == 0 && ctx.Err() != nil {
			return partialMoveCandidate{}
		}
		group := groups[groupIndex]
		groupKey := groupAssignmentKey(group)
		if movedGroups[groupKey] {
			continue
		}
		currentLogin := assignments[groupKey]
		currentLoginScore := baseScores[currentLogin]
		currentRemovedScore := loginScoreAfterRemove(loads, loginIINs, currentLogin, group, targets, weights)
		for loginIndex, candidate := range rpLoginKeys {
			if candidate == currentLogin {
				continue
			}
			score := partialMoveScoreFromBase(loads, loginIINs, candidate, group, targets, weights, currentScore, currentLoginScore, currentRemovedScore, baseScores[candidate])
			next := partialMoveCandidate{
				group:      group,
				login:      candidate,
				score:      score,
				groupIndex: groupIndex,
				loginIndex: loginIndex,
				ok:         true,
			}
			if betterPartialMoveCandidate(next, best) {
				bestScore = score
				best = next
			}
		}
	}
	return best
}

func betterPartialMoveCandidate(candidate partialMoveCandidate, current partialMoveCandidate) bool {
	if !candidate.ok {
		return false
	}
	if !current.ok {
		return true
	}
	if candidate.score.LessThan(current.score) {
		return true
	}
	if !candidate.score.Equal(current.score) {
		return false
	}
	if candidate.groupIndex != current.groupIndex {
		return candidate.groupIndex < current.groupIndex
	}
	return candidate.loginIndex < current.loginIndex
}

func partialMoveScoreFromBase(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, candidate loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights, currentScore decimal.Decimal, currentLoginScore decimal.Decimal, currentRemovedScore decimal.Decimal, candidateScoreBefore decimal.Decimal) decimal.Decimal {
	candidateScoreAfter := loginScoreAfterAdd(loads, loginIINs, candidate, group, targets, weights)
	return pyAdd(
		pyAdd(
			pySub(
				pySub(currentScore, currentLoginScore),
				candidateScoreBefore,
			),
			currentRemovedScore,
		),
		candidateScoreAfter,
	)
}

func loginScoreAfterRemove(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	amount := pySub(item.amount, group.amount)
	count := item.count - len(group.rows)
	iinCount := item.iinCount
	if loginIINs[login][group.iin] {
		iinCount--
	}
	return loadScore(iinCount, amount, count, targets, weights)
}

func alignmentMoveLimits(groups []*iinGroup) map[string]int {
	counts := make(map[string]int)
	for _, group := range groups {
		if group.pinnedLogin == "" {
			counts[group.rp]++
		}
	}
	limits := make(map[string]int)
	for rp, count := range counts {
		limits[rp] = max(1, int(math.Ceil(float64(count)*0.10)))
	}
	return limits
}

func replaceSummarySheet(workbook *excelize.File, loads map[loginKey]*load, fixedCount int, fixedIINCount int, title string) error {
	if index, err := workbook.GetSheetIndex(title); err == nil && index != -1 {
		_ = workbook.DeleteSheet(title)
	}
	_, err := workbook.NewSheet(title)
	if err != nil {
		return err
	}
	headers := []any{"РП", "Логин", "Количество материалов", "Сумма задолженности", "Количество ИИН"}
	for i, value := range headers {
		if err := setCell(workbook, title, 1, i+1, value); err != nil {
			return err
		}
	}

	headerStyle, _ := workbook.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"E7EEF8"}, Pattern: 1},
	})
	_ = workbook.SetCellStyle(title, "A1", "E1", headerStyle)

	keys := make([]loginKey, 0, len(loads))
	for key := range loads {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].rp == keys[j].rp {
			return keys[i].login < keys[j].login
		}
		return keys[i].rp < keys[j].rp
	})

	row := 2
	for _, key := range keys {
		item := loads[key]
		values := []any{key.rp, key.login, item.count, nil, item.iinCount}
		for col, value := range values {
			if value == nil {
				continue
			}
			if err := setCell(workbook, title, row, col+1, value); err != nil {
				return err
			}
		}
		amountCell, err := excelize.CoordinatesToCellName(4, row)
		if err != nil {
			return err
		}
		amount, _ := item.amount.Float64()
		if err := workbook.SetCellFloat(title, amountCell, amount, -1, 64); err != nil {
			return err
		}
		row++
	}
	if row > 2 {
		amountStyle, _ := workbook.NewStyle(&excelize.Style{NumFmt: 4})
		_ = workbook.SetCellStyle(title, "D2", fmt.Sprintf("D%d", row-1), amountStyle)
	}
	row++
	_ = setCell(workbook, title, row, 1, "Зафиксировано строк со статусами оплаты")
	_ = setCell(workbook, title, row, 2, fixedCount)
	row++
	_ = setCell(workbook, title, row, 1, "Зафиксировано ИИН со статусами оплаты")
	_ = setCell(workbook, title, row, 2, fixedIINCount)

	for col := 1; col <= 5; col++ {
		name, _ := excelize.ColumnNumberToName(col)
		_ = workbook.SetColWidth(title, name, name, 28)
	}
	return nil
}

type rpExchangeStat struct {
	rp             string
	givenCount     int
	receivedCount  int
	givenAmount    decimal.Decimal
	receivedAmount decimal.Decimal
}

func appendCrossRPExchangeSummary(workbook *excelize.File, sourceSheet string, cols columns, summarySheet string) error {
	if cols.sourceRP == 0 {
		return nil
	}
	rows, err := workbook.GetRows(sourceSheet)
	if err != nil {
		return err
	}
	stats := make(map[string]*rpExchangeStat)
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		row := rows[rowIndex]
		if isRepeatedHeaderRow(row, cols) {
			continue
		}
		sourceRP := normalizeRP(getRowCell(row, cols.sourceRP))
		targetRP := normalizeRP(getRowCell(row, cols.rp))
		if sourceRP == "" || targetRP == "" || sourceRP == targetRP {
			continue
		}
		amount := toDecimal(getRowCell(row, cols.amount))
		given := ensureRPExchangeStat(stats, sourceRP)
		given.givenCount++
		given.givenAmount = pyAdd(given.givenAmount, amount)
		received := ensureRPExchangeStat(stats, targetRP)
		received.receivedCount++
		received.receivedAmount = pyAdd(received.receivedAmount, amount)
	}

	summaryRows, err := workbook.GetRows(summarySheet)
	if err != nil {
		return err
	}
	startRow := len(summaryRows) + 3
	if err := setCell(workbook, summarySheet, startRow, 1, "Обмен между РП"); err != nil {
		return err
	}

	headers := []any{"РП", "Отдал материалов", "Получил материалов", "Отдал сумма", "Получил сумма", "Разница материалов", "Разница суммы"}
	for col, value := range headers {
		if err := setCell(workbook, summarySheet, startRow+1, col+1, value); err != nil {
			return err
		}
	}
	headerStyle, _ := workbook.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"E7EEF8"}, Pattern: 1},
	})
	titleStyle, _ := workbook.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true}})
	_ = workbook.SetCellStyle(summarySheet, fmt.Sprintf("A%d", startRow), fmt.Sprintf("A%d", startRow), titleStyle)
	_ = workbook.SetCellStyle(summarySheet, fmt.Sprintf("A%d", startRow+1), fmt.Sprintf("G%d", startRow+1), headerStyle)

	keys := make([]string, 0, len(stats))
	for rp := range stats {
		keys = append(keys, rp)
	}
	sort.Strings(keys)
	rowNumber := startRow + 2
	for _, rp := range keys {
		stat := stats[rp]
		values := []any{
			stat.rp,
			stat.givenCount,
			stat.receivedCount,
			nil,
			nil,
			stat.receivedCount - stat.givenCount,
			nil,
		}
		for col, value := range values {
			if value == nil {
				continue
			}
			if err := setCell(workbook, summarySheet, rowNumber, col+1, value); err != nil {
				return err
			}
		}
		for _, item := range []struct {
			col    int
			amount decimal.Decimal
		}{
			{col: 4, amount: stat.givenAmount},
			{col: 5, amount: stat.receivedAmount},
			{col: 7, amount: pySub(stat.receivedAmount, stat.givenAmount)},
		} {
			cell, err := excelize.CoordinatesToCellName(item.col, rowNumber)
			if err != nil {
				return err
			}
			amount, _ := item.amount.Float64()
			if err := workbook.SetCellFloat(summarySheet, cell, amount, -1, 64); err != nil {
				return err
			}
		}
		rowNumber++
	}
	if rowNumber > startRow+2 {
		amountStyle, _ := workbook.NewStyle(&excelize.Style{NumFmt: 4})
		_ = workbook.SetCellStyle(summarySheet, fmt.Sprintf("D%d", startRow+2), fmt.Sprintf("E%d", rowNumber-1), amountStyle)
		_ = workbook.SetCellStyle(summarySheet, fmt.Sprintf("G%d", startRow+2), fmt.Sprintf("G%d", rowNumber-1), amountStyle)
	}
	for col := 1; col <= 7; col++ {
		name, _ := excelize.ColumnNumberToName(col)
		_ = workbook.SetColWidth(summarySheet, name, name, 28)
	}
	return nil
}

func ensureRPExchangeStat(stats map[string]*rpExchangeStat, rp string) *rpExchangeStat {
	if stats[rp] == nil {
		stats[rp] = &rpExchangeStat{rp: rp}
	}
	return stats[rp]
}

func styleAttachColumn(workbook *excelize.File, sheet string, column int) error {
	colName, err := excelize.ColumnNumberToName(column)
	if err != nil {
		return err
	}
	style, _ := workbook.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"DDEFD9"}, Pattern: 1},
	})
	_ = workbook.SetCellStyle(sheet, colName+"1", colName+"1", style)
	width, err := workbook.GetColWidth(sheet, colName)
	if err != nil || width < 18 {
		width = 18
	}
	return workbook.SetColWidth(sheet, colName, colName, width)
}

func styleSourceRPColumn(workbook *excelize.File, sheet string, column int) error {
	colName, err := excelize.ColumnNumberToName(column)
	if err != nil {
		return err
	}
	style, _ := workbook.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"FFF2CC"}, Pattern: 1},
	})
	_ = workbook.SetCellStyle(sheet, colName+"1", colName+"1", style)
	width, err := workbook.GetColWidth(sheet, colName)
	if err != nil || width < 16 {
		width = 16
	}
	return workbook.SetColWidth(sheet, colName, colName, width)
}

func getCell(workbook *excelize.File, sheet string, row, col int) string {
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return ""
	}
	value, err := workbook.GetCellValue(sheet, cell)
	if err != nil {
		return ""
	}
	return value
}

func getRowCell(row []string, col int) string {
	index := col - 1
	if index < 0 || index >= len(row) {
		return ""
	}
	return row[index]
}

func setCell(workbook *excelize.File, sheet string, row, col int, value any) error {
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return err
	}
	return workbook.SetCellValue(sheet, cell, value)
}

func normalizeHeader(value string) string {
	text := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "ё", "е")
	return strings.Join(strings.Fields(text), " ")
}

func normalizeLogin(value string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func normalizeRP(value string) string {
	return normalizeLogin(value)
}

func normalizeStatus(value string) string {
	return normalizeHeader(value)
}

func normalizeIIN(value string) string {
	text := strings.TrimSpace(value)
	if strings.HasSuffix(text, ".0") && onlyDigits(strings.TrimSuffix(text, ".0")) {
		text = strings.TrimSuffix(text, ".0")
	}
	if onlyDigits(text) {
		for len(text) < 12 {
			text = "0" + text
		}
	}
	return text
}

func onlyDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func toDecimal(value string) decimal.Decimal {
	if number, ok := parseCleanDecimal(value); ok {
		return number
	}

	text := normalizeDecimalText(value)
	if text == "" {
		return decimal.Zero
	}
	number, err := decimal.NewFromString(text)
	if err != nil {
		return decimal.Zero
	}
	return number
}

func parseCleanDecimal(value string) (decimal.Decimal, bool) {
	text := strings.TrimSpace(value)
	text = strings.ReplaceAll(text, " ", "")
	text = strings.ReplaceAll(text, "\u00a0", "")
	text = strings.ReplaceAll(text, "\u202f", "")
	if text == "" {
		return decimal.Zero, false
	}
	if strings.ContainsAny(text, "eE") {
		text = strings.ReplaceAll(text, ",", ".")
		number, err := decimal.NewFromString(text)
		return number, err == nil
	}
	if strings.Contains(text, ",") || isLikelyThousandsDot(text) {
		return decimal.Zero, false
	}
	number, err := decimal.NewFromString(text)
	return number, err == nil
}

func isLikelyThousandsDot(value string) bool {
	if strings.Count(value, ".") != 1 {
		return false
	}
	index := strings.Index(value, ".")
	return index > 0 && index <= 3 && len(value)-index-1 == 3
}

func normalizeDecimalText(value string) string {
	var builder strings.Builder
	negative := false
	for _, r := range strings.TrimSpace(value) {
		switch {
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == ',' || r == '.':
			builder.WriteRune(r)
		case r == '-' || r == '−':
			if builder.Len() == 0 {
				negative = true
			}
		}
	}

	text := builder.String()
	if text == "" {
		return ""
	}

	lastComma := strings.LastIndex(text, ",")
	lastDot := strings.LastIndex(text, ".")
	decimalIndex := -1
	if lastComma >= 0 && lastDot >= 0 {
		decimalIndex = max(lastComma, lastDot)
	} else if lastComma >= 0 {
		decimalIndex = decimalSeparatorIndex(text, ",", lastComma)
	} else if lastDot >= 0 {
		decimalIndex = decimalSeparatorIndex(text, ".", lastDot)
	}

	var normalized strings.Builder
	if negative {
		normalized.WriteByte('-')
	}
	for index, r := range text {
		if r >= '0' && r <= '9' {
			normalized.WriteRune(r)
			continue
		}
		if index == decimalIndex {
			normalized.WriteByte('.')
		}
	}
	return normalized.String()
}

func decimalSeparatorIndex(text, separator string, lastIndex int) int {
	if strings.Count(text, separator) > 1 {
		decimalDigits := len(text) - lastIndex - 1
		if decimalDigits > 0 && decimalDigits != 3 {
			return lastIndex
		}
		return -1
	}
	decimalDigits := len(text) - lastIndex - 1
	if decimalDigits > 0 && decimalDigits <= 2 {
		return lastIndex
	}
	return -1
}

func pyAdd(left, right decimal.Decimal) decimal.Decimal {
	return pyRound(left.Add(right))
}

func pySub(left, right decimal.Decimal) decimal.Decimal {
	return pyRound(left.Sub(right))
}

func pyMul(left, right decimal.Decimal) decimal.Decimal {
	return pyRound(left.Mul(right))
}

func pyDiv(left, right decimal.Decimal) decimal.Decimal {
	if right.IsZero() {
		return decimal.Zero
	}
	return pyRound(left.Div(right))
}

func pyRound(value decimal.Decimal) decimal.Decimal {
	if value.IsZero() {
		return decimal.Zero
	}
	coefficient := strings.TrimPrefix(value.Coefficient().String(), "-")
	coefficient = strings.TrimLeft(coefficient, "0")
	if coefficient == "" {
		return decimal.Zero
	}
	adjustedExponent := int32(len(coefficient)) + value.Exponent() - 1
	places := int32(28) - adjustedExponent - 1
	return value.RoundBank(places)
}

func makeGroupKey(rp, iin string) string {
	return rp + "\x00" + iin
}

func groupAssignmentKey(group *iinGroup) string {
	rp := group.rp
	if group.sourceRP != "" {
		rp = group.sourceRP
	}
	return makeGroupKey(rp, group.iin)
}

func groupsForRP(groups []*iinGroup, rp string) []*iinGroup {
	var out []*iinGroup
	for _, group := range groups {
		if group.rp == rp {
			out = append(out, group)
		}
	}
	return out
}

func containsLoginKey(items []loginKey, key loginKey) bool {
	for _, item := range items {
		if item == key {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
