package storage

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// BatchSize controls how many metas to process before writing to disk
const BatchSize = 10000

// indexMetadata contains information about the batch files
type indexMetadata struct {
	BatchCount  int              `json:"batch_count"`
	SchemaKeys  map[string][]int `json:"schema_keys"`  // Maps schema keys to batch numbers
	PackageKeys map[string][]int `json:"package_keys"` // Maps package keys to batch numbers
	NameKeys    map[string][]int `json:"name_keys"`    // Maps name keys to batch numbers
}

// batchFile represents a single batch of sections
type batchFile struct {
	Sections map[string][]section `json:"sections"` // Maps key to sections
}

// section represents a byte range in the FBC file
type section struct {
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

// diskIndex is an index implementation that stores batches on disk
// to reduce memory fragmentation when processing large datasets
type diskIndex struct {
	catalogDir string
	meta       *indexMetadata
	sync.RWMutex
}

// Get retrieves sections matching the given criteria
func (i *diskIndex) Get(r io.ReaderAt, schema, packageName, name string) io.Reader {
	i.RLock()
	defer i.RUnlock()

	sectionSet := i.getSectionSet(schema, packageName, name)
	sections := sectionSet.UnsortedList()
	slices.SortFunc(sections, func(a, b section) int {
		return cmp.Compare(a.Offset, b.Offset)
	})

	srs := make([]io.Reader, 0, len(sections))
	for _, s := range sections {
		sr := io.NewSectionReader(r, s.Offset, s.Length)
		srs = append(srs, sr)
	}
	return io.MultiReader(srs...)
}

// getSectionSet retrieves and combines sections based on query parameters
func (i *diskIndex) getSectionSet(schema, packageName, name string) sets.Set[section] {
	batchesToLoad := make(map[int]bool)

	// Determine which batches to load based on query parameters
	if schema == "" {
		// Load all batches if no schema specified
		for batchNum := 0; batchNum < i.meta.BatchCount; batchNum++ {
			batchesToLoad[batchNum] = true
		}
	} else if batches, ok := i.meta.SchemaKeys[schema]; ok {
		for _, batchNum := range batches {
			batchesToLoad[batchNum] = true
		}
	}

	// Filter by package name
	if packageName != "" {
		if batches, ok := i.meta.PackageKeys[packageName]; ok {
			if schema != "" {
				// Intersect with existing batch set
				packageBatches := make(map[int]bool)
				for _, batchNum := range batches {
					packageBatches[batchNum] = true
				}

				// Keep only batches that exist in both sets
				for batchNum := range batchesToLoad {
					if !packageBatches[batchNum] {
						delete(batchesToLoad, batchNum)
					}
				}
			} else {
				// Set batches directly if no schema filter
				newBatchesToLoad := make(map[int]bool)
				for _, batchNum := range batches {
					newBatchesToLoad[batchNum] = true
				}
				batchesToLoad = newBatchesToLoad
			}
		} else {
			// No matches for package name
			return sets.New[section]()
		}
	}

	// Filter by name
	if name != "" {
		if batches, ok := i.meta.NameKeys[name]; ok {
			nameBatches := make(map[int]bool)
			for _, batchNum := range batches {
				nameBatches[batchNum] = true
			}

			// Keep only batches that exist in both sets
			for batchNum := range batchesToLoad {
				if !nameBatches[batchNum] {
					delete(batchesToLoad, batchNum)
				}
			}
		} else {
			// No matches for name
			return sets.New[section]()
		}
	}

	// Load and combine sections from the determined batches
	result := sets.New[section]()
	for batchNum := range batchesToLoad {
		batch, err := i.loadBatch(batchNum)
		if err != nil {
			// Log error and continue
			fmt.Printf("Error loading batch %d: %v\n", batchNum, err)
			continue
		}

		// Add relevant sections to the result set
		if schema != "" {
			for _, s := range batch.Sections[schema] {
				result.Insert(s)
			}
		} else {
			// Add all sections if no schema filter
			for _, sections := range batch.Sections {
				result.Insert(sections...)
			}
		}
	}

	return result
}

// loadBatch loads a batch file from disk
func (i *diskIndex) loadBatch(batchNum int) (*batchFile, error) {
	batchPath := i.getBatchPath(batchNum)

	f, err := os.Open(batchPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var batch batchFile
	dec := json.NewDecoder(f)
	if err := dec.Decode(&batch); err != nil {
		return nil, err
	}

	return &batch, nil
}

// getBatchPath returns the path to a batch file
func (i *diskIndex) getBatchPath(batchNum int) string {
	return filepath.Join(i.catalogDir, fmt.Sprintf("index.batch.%d.json", batchNum))
}

// getMetadataPath returns the path to the index metadata file
func (i *diskIndex) getMetadataPath() string {
	return filepath.Join(i.catalogDir, "index.meta.json")
}

// saveMetadata saves the index metadata to disk
func (i *diskIndex) saveMetadata() error {
	metaPath := i.getMetadataPath()
	f, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	return enc.Encode(i.meta)
}
