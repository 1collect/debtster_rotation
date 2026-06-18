package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const rotationRecordsCollection = "rotation_records"

type appIntegrations struct {
	pg       *pgxpool.Pool
	mongo    *mongodriver.Database
	s3       *minio.Client
	s3Bucket string
}

type rotationRecordDoc struct {
	ID             primitive.ObjectID `bson:"_id"`
	UserID         int64              `bson:"user_id"`
	Type           string             `bson:"type"`
	FileName       string             `bson:"file_name"`
	FilePath       string             `bson:"file_path"`
	FileBucket     string             `bson:"file_bucket"`
	FileKey        string             `bson:"file_key"`
	FileSize       int64              `bson:"file_size"`
	Status         string             `bson:"status"`
	Progress       int                `bson:"progress"`
	Message        string             `bson:"message"`
	JobID          string             `bson:"job_id"`
	ResultPath     string             `bson:"result_path,omitempty"`
	ResultBucket   string             `bson:"result_bucket,omitempty"`
	ResultKey      string             `bson:"result_key,omitempty"`
	ResultFileName string             `bson:"result_file_name,omitempty"`
	CreatedAt      time.Time          `bson:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at"`
	StartedAt      *time.Time         `bson:"started_at,omitempty"`
	CompletedAt    *time.Time         `bson:"completed_at,omitempty"`
}

func initIntegrations(ctx context.Context) *appIntegrations {
	integrations := &appIntegrations{}

	if pg, err := newPostgresPool(ctx); err != nil {
		log.Printf("postgres disabled: %v", err)
	} else {
		integrations.pg = pg
	}

	if db, err := newMongoDatabase(ctx); err != nil {
		log.Printf("mongo disabled: %v", err)
	} else {
		integrations.mongo = db
	}

	if s3Client, bucket, err := newS3Client(ctx); err != nil {
		log.Printf("s3 disabled: %v", err)
	} else {
		integrations.s3 = s3Client
		integrations.s3Bucket = bucket
	}

	return integrations
}

func newPostgresPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		env("PG_HOST", env("DB_HOST", "127.0.0.1")),
		env("PG_PORT", env("DB_PORT", "5432")),
		env("PG_USER", env("DB_USERNAME", "root")),
		env("PG_PASSWORD", env("DB_PASSWORD", "hello-world")),
		env("PG_DB", env("DB_DATABASE", "debtster")),
		env("PG_SSLMODE", "disable"),
	)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func newMongoDatabase(ctx context.Context) (*mongodriver.Database, error) {
	uri := mongoURI()
	client, err := mongodriver.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}

	return client.Database(env("MONGO_DB", env("DB_IMPORT_DATABASE", "pkb_imports"))), nil
}

func mongoURI() string {
	scheme := env("MONGO_SCHEME", "mongodb")
	user := env("MONGO_USER", env("DB_IMPORT_USERNAME", ""))
	pass := env("MONGO_PASSWORD", env("DB_IMPORT_PASSWORD", ""))
	host := env("MONGO_HOST", env("DB_IMPORT_HOST", "127.0.0.1"))
	port := env("MONGO_PORT", env("DB_IMPORT_PORT", "27017"))
	db := env("MONGO_DB", env("DB_IMPORT_DATABASE", "pkb_imports"))
	authSource := env("MONGO_AUTH_SOURCE", "")

	auth := ""
	if user != "" {
		auth = user
		if pass != "" {
			auth += ":" + pass
		}
		auth += "@"
	}

	query := ""
	if authSource != "" {
		query = "?authSource=" + authSource
	}

	return fmt.Sprintf("%s://%s%s:%s/%s%s", scheme, auth, host, port, db, query)
}

func newS3Client(ctx context.Context) (*minio.Client, string, error) {
	endpoint := strings.TrimPrefix(strings.TrimPrefix(env("AWS_ENDPOINT", "127.0.0.1:3304"), "http://"), "https://")
	useSSL := env("AWS_USE_SSL", "false") == "true" || strings.HasPrefix(env("AWS_ENDPOINT", ""), "https://")
	bucket := env("AWS_BUCKET", "debtster")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(env("AWS_ACCESS_KEY_ID", "minioadmin"), env("AWS_SECRET_ACCESS_KEY", "minioadmin"), ""),
		Secure: useSSL,
		Region: env("AWS_DEFAULT_REGION", "us-east-1"),
	})
	if err != nil {
		return nil, "", err
	}

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, "", err
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, "", err
		}
	}

	return client, bucket, nil
}

func (s *server) uploadAndStart(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		writeCORS(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.integrations == nil || s.integrations.mongo == nil || s.integrations.s3 == nil {
		writeJSONError(w, "Mongo или MinIO не настроены в go_rotation.", http.StatusServiceUnavailable)
		return
	}

	userID, ok := s.authUserID(r)
	if !ok {
		writeJSONError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if err := r.ParseMultipartForm(128 << 20); err != nil {
		writeJSONError(w, "Загрузите XLSX файл.", http.StatusBadRequest)
		return
	}

	processType := r.FormValue("type")
	if processType == "" {
		processType = r.FormValue("process")
	}
	if !validProcessType(processType) {
		writeJSONError(w, "Неверный тип обработки.", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSONError(w, "Загрузите XLSX файл.", http.StatusBadRequest)
		return
	}
	defer file.Close()
	if !strings.HasSuffix(strings.ToLower(header.Filename), ".xlsx") {
		writeJSONError(w, "Поддерживается только формат .xlsx.", http.StatusBadRequest)
		return
	}

	content, err := io.ReadAll(file)
	if err != nil {
		writeJSONError(w, "Не удалось прочитать файл.", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if s.hasRunningJobLocked() {
		s.mu.Unlock()
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}
	s.mu.Unlock()

	jobID := newJobID()
	now := time.Now()
	fileName := path.Base(header.Filename)
	key := fmt.Sprintf("rotation-records/source/%s/%s", jobID, fileName)

	info, err := s.integrations.s3.PutObject(r.Context(), s.integrations.s3Bucket, key, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{
		ContentType: header.Header.Get("Content-Type"),
	})
	if err != nil {
		writeJSONError(w, "Не удалось сохранить файл в MinIO: "+err.Error(), http.StatusInternalServerError)
		return
	}

	record := rotationRecordDoc{
		ID:         primitive.NewObjectID(),
		UserID:     userID,
		Type:       normalizeProcessType(processType),
		FileName:   fileName,
		FileBucket: s.integrations.s3Bucket,
		FileKey:    key,
		FilePath:   fmt.Sprintf("s3://%s/%s", s.integrations.s3Bucket, key),
		FileSize:   info.Size,
		Status:     "running",
		Progress:   1,
		Message:    "Процесс запущен.",
		JobID:      jobID,
		CreatedAt:  now,
		UpdatedAt:  now,
		StartedAt:  &now,
	}

	if _, err := s.integrations.mongo.Collection(rotationRecordsCollection).InsertOne(r.Context(), record); err != nil {
		writeJSONError(w, "Не удалось создать запись Mongo: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !s.startJob(content, jobID, record.Type, resultFilename(record.Type), configForProcess(record.Type)) {
		_ = s.updateRotationRecord(r.Context(), jobID, bson.M{
			"status":       "failed",
			"message":      "Дождитесь завершения предыдущей операции или отмените ее.",
			"completed_at": time.Now(),
		})
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}

	writeCORS(w)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":     record.ID.Hex(),
		"job_id": jobID,
		"state":  "running",
		"record": map[string]any{
			"id":         record.ID.Hex(),
			"user_id":    record.UserID,
			"type":       record.Type,
			"file_name":  record.FileName,
			"file_path":  record.FilePath,
			"file_key":   record.FileKey,
			"status":     record.Status,
			"progress":   record.Progress,
			"message":    record.Message,
			"job_id":     record.JobID,
			"created_at": record.CreatedAt,
			"updated_at": record.UpdatedAt,
			"started_at": record.StartedAt,
		},
	})
}

func (s *server) authUserID(r *http.Request) (int64, bool) {
	if s.integrations == nil || s.integrations.pg == nil {
		return 0, false
	}
	token := bearerToken(r)
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		return 0, false
	}

	tokenID, tokenPart := splitSanctumToken(token)
	sum := sha256.Sum256([]byte(tokenPart))
	hash := fmt.Sprintf("%x", sum)

	var userID int64
	var dbToken string

	if tokenID != nil {
		err := s.integrations.pg.QueryRow(r.Context(), `
			SELECT tokenable_id, token
			FROM personal_access_tokens
			WHERE id = $1
			  AND tokenable_type = 'App\Infrastructure\Persistence\Models\User'
			  AND (expires_at IS NULL OR expires_at > NOW())
		`, *tokenID).Scan(&userID, &dbToken)
		if err == nil && (dbToken == hash || dbToken == tokenPart) {
			return userID, true
		}
	}

	err := s.integrations.pg.QueryRow(r.Context(), `
		SELECT tokenable_id
		FROM personal_access_tokens
		WHERE tokenable_type = 'App\Infrastructure\Persistence\Models\User'
		  AND token IN ($1, $2)
		  AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY created_at DESC
		LIMIT 1
	`, hash, tokenPart).Scan(&userID)

	return userID, err == nil
}

func bearerToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	}
	return ""
}

func splitSanctumToken(token string) (*int64, string) {
	if idx := strings.Index(token, "|"); idx > 0 {
		if id, err := strconv.ParseInt(token[:idx], 10, 64); err == nil {
			return &id, token[idx+1:]
		}
	}
	return nil, token
}

func (s *server) updateRotationRecord(ctx context.Context, jobID string, set bson.M) error {
	if s.integrations == nil || s.integrations.mongo == nil {
		return nil
	}
	set["updated_at"] = time.Now()
	_, err := s.integrations.mongo.Collection(rotationRecordsCollection).UpdateOne(ctx, bson.M{"job_id": jobID}, bson.M{"$set": set})
	return err
}

func (s *server) saveRotationResult(ctx context.Context, jobID, filename string, data []byte) (string, error) {
	if s.integrations == nil || s.integrations.s3 == nil {
		return "", nil
	}

	key := fmt.Sprintf("rotation-records/results/%s/%s", jobID, filename)
	_, err := s.integrations.s3.PutObject(ctx, s.integrations.s3Bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	})
	if err != nil {
		return "", err
	}

	path := fmt.Sprintf("s3://%s/%s", s.integrations.s3Bucket, key)
	return path, s.updateRotationRecord(ctx, jobID, bson.M{
		"result_path":      path,
		"result_bucket":    s.integrations.s3Bucket,
		"result_key":       key,
		"result_file_name": filename,
	})
}

func validProcessType(process string) bool {
	switch normalizeProcessType(process) {
	case "rotation", "rotation_no_rp", "alignment":
		return true
	default:
		return false
	}
}

func normalizeProcessType(process string) string {
	switch process {
	case "rotation_parallel":
		return "rotation"
	case "balance", "alignment_slow", "alignment_parallel":
		return "alignment"
	default:
		return process
	}
}

func configForProcess(process string) workbookConfig {
	switch normalizeProcessType(process) {
	case "rotation_no_rp":
		return workbookConfig{
			fixedStatuses: rotationFixedStatuses,
			sourceColumn:  "detach",
			strategy:      "full_parallel",
			processName:   "ротации без РП",
			summaryTitle:  "Итоги ротации без РП",
			ignoreRP:      true,
		}
	case "alignment":
		return workbookConfig{
			fixedStatuses: alignmentFixedStatuses,
			sourceColumn:  "attach",
			strategy:      "partial",
			processName:   "выравнивания",
			summaryTitle:  "Итоги выравнивания",
		}
	default:
		return workbookConfig{
			fixedStatuses: rotationFixedStatuses,
			sourceColumn:  "detach",
			strategy:      "full_parallel",
			processName:   "параллельной ротации",
			summaryTitle:  "Итоги параллельной ротации",
		}
	}
}

func resultFilename(process string) string {
	switch normalizeProcessType(process) {
	case "rotation_no_rp":
		return "rotation_no_rp_result.xlsx"
	case "alignment":
		return "alignment_result.xlsx"
	default:
		return "rotation_parallel_result.xlsx"
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func writeCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Rotation-Token")
}
