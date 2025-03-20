package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/operator-framework/operator-registry/alpha/declcfg"
)

// LocalDirV1 is a storage Instance. When Storing a new FBC contained in
// fs.FS, the content is first written to a temporary file, after which
// it is copied to its final destination in RootDir/<catalogName>.jsonl. This is
// done so that clients accessing the content stored in RootDir/<catalogName>.json1
// have an atomic view of the content for a catalog.
type LocalDirV1 struct {
	RootDir            string
	RootURL            *url.URL
	EnableMetasHandler bool

	m sync.RWMutex
}

var (
	_                Instance = (*LocalDirV1)(nil)
	errInvalidParams          = errors.New("invalid parameters")
)

func (s *LocalDirV1) Store(ctx context.Context, catalog string, fsys fs.FS) error {
	s.m.Lock()
	defer s.m.Unlock()

	if err := os.MkdirAll(s.RootDir, 0700); err != nil {
		return err
	}
	tmpCatalogDir, err := os.MkdirTemp(s.RootDir, fmt.Sprintf(".%s-*", catalog))
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpCatalogDir)

	storeMetaFuncs := []storeMetasFunc{storeCatalogData}
	if s.EnableMetasHandler {
		storeMetaFuncs = append(storeMetaFuncs, storeIndexData)
	}

	eg, egCtx := errgroup.WithContext(ctx)
	metaChans := []chan *declcfg.Meta{}

	for range storeMetaFuncs {
		metaChans = append(metaChans, make(chan *declcfg.Meta, 1))
	}
	for i, f := range storeMetaFuncs {
		eg.Go(func() error {
			return f(tmpCatalogDir, metaChans[i])
		})
	}
	err = declcfg.WalkMetasFS(egCtx, fsys, func(path string, meta *declcfg.Meta, err error) error {
		if err != nil {
			return err
		}
		for _, ch := range metaChans {
			select {
			case ch <- meta:
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
		return nil
	}, declcfg.WithConcurrency(1))
	for _, ch := range metaChans {
		close(ch)
	}
	if err != nil {
		return fmt.Errorf("error walking FBC root: %w", err)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	catalogDir := s.catalogDir(catalog)
	return errors.Join(
		os.RemoveAll(catalogDir),
		os.Rename(tmpCatalogDir, catalogDir),
	)
}

func (s *LocalDirV1) Delete(catalog string) error {
	s.m.Lock()
	defer s.m.Unlock()

	return os.RemoveAll(s.catalogDir(catalog))
}

func (s *LocalDirV1) ContentExists(catalog string) bool {
	s.m.RLock()
	defer s.m.RUnlock()

	catalogFileStat, err := os.Stat(catalogFilePath(s.catalogDir(catalog)))
	if err != nil {
		return false
	}
	if !catalogFileStat.Mode().IsRegular() {
		// path is not valid content
		return false
	}

	if s.EnableMetasHandler {
		// Check for index metadata file instead of the old index file
		metaFileStat, err := os.Stat(filepath.Join(s.catalogDir(catalog), "index.meta.json"))
		if err != nil {
			return false
		}
		if !metaFileStat.Mode().IsRegular() {
			return false
		}
	}

	return true
}

func (s *LocalDirV1) catalogDir(catalog string) string {
	return filepath.Join(s.RootDir, catalog)
}

func catalogFilePath(catalogDir string) string {
	return filepath.Join(catalogDir, "catalog.jsonl")
}

func catalogIndexFilePath(catalogDir string) string {
	return filepath.Join(catalogDir, "index.json")
}

type storeMetasFunc func(catalogDir string, metaChan <-chan *declcfg.Meta) error

func storeCatalogData(catalogDir string, metas <-chan *declcfg.Meta) error {
	f, err := os.Create(catalogFilePath(catalogDir))
	if err != nil {
		return err
	}
	// Use a buffered writer for efficient writing
	bufWriter := bufio.NewWriter(f)
	defer func() {
		// Drain the channel to prevent producer goroutine from blocking
		// if we had an error during processing
		for range metas {
			// Just drain, don't process
		}

		bufWriter.Flush() // Flush any remaining buffered data
		f.Sync()          // Ensure data is committed to disk
		f.Close()         // Close the file
	}()

	// Process the channel
	for m := range metas {
		if _, err := bufWriter.Write(m.Blob); err != nil {
			return err
		}

		// Periodically flush to avoid excessive memory usage
		if bufWriter.Buffered() > 1024*1024 { // 1MB threshold
			if err := bufWriter.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// storeIndexData stores the index data with batching
// storeIndexData processes metas and stores them in batches on disk
func storeIndexData(catalogDir string, metas <-chan *declcfg.Meta) error {
	idx := &diskIndex{
		catalogDir: catalogDir,
		meta: &indexMetadata{
			SchemaKeys:  make(map[string][]int),
			PackageKeys: make(map[string][]int),
			NameKeys:    make(map[string][]int),
		},
	}

	currentBatch := &batchFile{
		Sections: make(map[string][]section),
	}

	batchNum := 0
	count := 0
	offset := int64(0)

	// Track which keys are in the current batch
	schemaKeysInBatch := make(map[string]bool)
	packageKeysInBatch := make(map[string]bool)
	nameKeysInBatch := make(map[string]bool)

	// Function to write the current batch to disk and start a new one
	writeBatch := func(batchNum int) error {
		// Update metadata
		for schema := range schemaKeysInBatch {
			idx.meta.SchemaKeys[schema] = append(idx.meta.SchemaKeys[schema], batchNum)
		}
		for pkg := range packageKeysInBatch {
			idx.meta.PackageKeys[pkg] = append(idx.meta.PackageKeys[pkg], batchNum)
		}
		for name := range nameKeysInBatch {
			idx.meta.NameKeys[name] = append(idx.meta.NameKeys[name], batchNum)
		}

		// Write batch to disk
		batchPath := idx.getBatchPath(batchNum)
		f, err := os.Create(batchPath)
		if err != nil {
			return err
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(currentBatch); err != nil {
			return err
		}

		// Increment batch counter and reset for next batch
		batchNum++
		idx.meta.BatchCount = batchNum

		// Reset tracking
		currentBatch = &batchFile{
			Sections: make(map[string][]section),
		}
		schemaKeysInBatch = make(map[string]bool)
		packageKeysInBatch = make(map[string]bool)
		nameKeysInBatch = make(map[string]bool)
		count = 0

		return nil
	}

	for meta := range metas {
		start := offset
		length := int64(len(meta.Blob))
		offset += length
		s := section{Offset: start, Length: length}

		// Add to schema index
		if meta.Schema != "" {
			if _, ok := currentBatch.Sections[meta.Schema]; !ok {
				currentBatch.Sections[meta.Schema] = []section{}
			}
			currentBatch.Sections[meta.Schema] = append(currentBatch.Sections[meta.Schema], s)
			schemaKeysInBatch[meta.Schema] = true
		}

		// Add to package index
		if meta.Package != "" {
			if _, ok := currentBatch.Sections[meta.Package]; !ok {
				currentBatch.Sections[meta.Package] = []section{}
			}
			currentBatch.Sections[meta.Package] = append(currentBatch.Sections[meta.Package], s)
			packageKeysInBatch[meta.Package] = true
		}

		// Add to name index
		if meta.Name != "" {
			if _, ok := currentBatch.Sections[meta.Name]; !ok {
				currentBatch.Sections[meta.Name] = []section{}
			}
			currentBatch.Sections[meta.Name] = append(currentBatch.Sections[meta.Name], s)
			nameKeysInBatch[meta.Name] = true
		}

		count++
		if count >= BatchSize {
			if err := writeBatch(batchNum); err != nil {
				return err
			}
		}
	}

	// Write final batch if there's any data
	if count > 0 {
		if err := writeBatch(batchNum); err != nil {
			return err
		}
	}

	// Save metadata
	return idx.saveMetadata()
}

func (s *LocalDirV1) BaseURL(catalog string) string {
	return s.RootURL.JoinPath(catalog).String()
}

func (s *LocalDirV1) StorageServerHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(s.RootURL.JoinPath("{catalog}", "api", "v1", "all").Path, s.handleV1All)
	if s.EnableMetasHandler {
		mux.HandleFunc(s.RootURL.JoinPath("{catalog}", "api", "v1", "metas").Path, s.handleV1Metas)
	}
	allowedMethodsHandler := func(next http.Handler, allowedMethods ...string) http.Handler {
		allowedMethodSet := sets.New[string](allowedMethods...)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !allowedMethodSet.Has(r.Method) {
				http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	return allowedMethodsHandler(mux, http.MethodGet, http.MethodHead)
}

func (s *LocalDirV1) handleV1All(w http.ResponseWriter, r *http.Request) {
	s.m.RLock()
	defer s.m.RUnlock()

	catalog := r.PathValue("catalog")
	catalogFile, catalogStat, err := s.catalogData(catalog)
	if err != nil {
		httpError(w, err)
		return
	}
	w.Header().Add("Content-Type", "application/jsonl")
	http.ServeContent(w, r, "", catalogStat.ModTime(), catalogFile)
}

func (s *LocalDirV1) handleV1Metas(w http.ResponseWriter, r *http.Request) {
	s.m.RLock()
	defer s.m.RUnlock()

	// Check for unexpected query parameters
	expectedParams := map[string]bool{
		"schema":  true,
		"package": true,
		"name":    true,
	}

	for param := range r.URL.Query() {
		if !expectedParams[param] {
			httpError(w, errInvalidParams)
			return
		}
	}

	catalog := r.PathValue("catalog")
	catalogFile, catalogStat, err := s.catalogData(catalog)
	if err != nil {
		httpError(w, err)
		return
	}
	defer catalogFile.Close()

	w.Header().Set("Last-Modified", catalogStat.ModTime().UTC().Format(timeFormat))
	done := checkPreconditions(w, r, catalogStat.ModTime())
	if done {
		return
	}

	schema := r.URL.Query().Get("schema")
	pkg := r.URL.Query().Get("package")
	name := r.URL.Query().Get("name")

	if schema == "" && pkg == "" && name == "" {
		// If no parameters are provided, return the entire catalog (this is the same as /api/v1/all)
		serveJSONLines(w, r, catalogFile)
		return
	}
	idx, err := s.loadIndex(catalog)
	if err != nil {
		httpError(w, err)
		return
	}
	indexReader := idx.Get(catalogFile, schema, pkg, name)
	serveJSONLines(w, r, indexReader)
}

func (s *LocalDirV1) catalogData(catalog string) (*os.File, os.FileInfo, error) {
	catalogFile, err := os.Open(catalogFilePath(s.catalogDir(catalog)))
	if err != nil {
		return nil, nil, err
	}
	catalogFileStat, err := catalogFile.Stat()
	if err != nil {
		return nil, nil, err
	}
	return catalogFile, catalogFileStat, nil
}

func httpError(w http.ResponseWriter, err error) {
	var code int
	switch {
	case errors.Is(err, fs.ErrNotExist):
		code = http.StatusNotFound
	case errors.Is(err, fs.ErrPermission):
		code = http.StatusForbidden
	case errors.Is(err, errInvalidParams):
		code = http.StatusBadRequest
	default:
		code = http.StatusInternalServerError
	}
	http.Error(w, fmt.Sprintf("%d %s", code, http.StatusText(code)), code)
}

func serveJSONLines(w http.ResponseWriter, r *http.Request, rs io.Reader) {
	w.Header().Add("Content-Type", "application/jsonl")

	// Return early for HEAD requests
	if r.Method == http.MethodHead {
		return
	}

	// Set appropriate headers for streaming
	w.Header().Set("Transfer-Encoding", "chunked")

	// Use a buffered writer to improve performance
	bufWriter := bufio.NewWriterSize(w, 64*1024) // 64KB buffer

	// Create a copy operation that can be canceled
	done := make(chan struct{})

	go func() {
		defer close(done)

		// Use the request context for cancellation
		ctx := r.Context()

		// Create a buffer that will be reused
		buf := make([]byte, 32*1024) // 32KB buffer

		for {
			// Check if the client has disconnected
			select {
			case <-ctx.Done():
				return
			default:
				// Continue with copy
			}

			// Read a chunk
			nr, readErr := rs.Read(buf)
			if nr > 0 {
				// Write the chunk
				_, writeErr := bufWriter.Write(buf[:nr])
				if writeErr != nil {
					return
				}

				// Flush periodically to avoid keeping too much in memory
				if bufWriter.Buffered() > 32*1024 {
					if flushErr := bufWriter.Flush(); flushErr != nil {
						return
					}
				}
			}

			if readErr != nil {
				if readErr != io.EOF {
					httpError(w, readErr)
				}
				break
			}
		}

		// Final flush
		bufWriter.Flush()
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Copy completed normally
	case <-time.After(5 * time.Minute): // Reasonable timeout
		// Log timeout but don't return an error to the client as headers are already sent
		log.Printf("Response timeout for large content stream")
	}
}

// loadIndex loads an index from disk
func (s *LocalDirV1) loadIndex(catalog string) (*diskIndex, error) {
	idx := &diskIndex{
		catalogDir: s.catalogDir(catalog),
	}

	// Load metadata
	metaPath := idx.getMetadataPath()
	f, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	idx.meta = &indexMetadata{}
	if err := dec.Decode(idx.meta); err != nil {
		return nil, err
	}

	return idx, nil
}
