package main

import (
	"container/heap"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/go-homedir"

	"golang.org/x/text/unicode/norm"
	"google.golang.org/api/drive/v3"
)

// TODO
/*
- Try to identify multiple files with same name (on Google Drive side) and flag
	separately or use different strategy to validate
- REFACTOR! especially main
*/

// Uncomment the following to allow profiling via http
// import "net/http"
// import _ "net/http/pprof"

// File stores the result of either Dropbox API or local file listing
type File struct {
	Path         string
	OriginalPath string
	ContentHash  string
}

// FileError records a local file that could not be read due to an error
type FileError struct {
	Path  string
	Error error
}

type progressType int

const (
	remoteProgress progressType = iota
	localProgress
	errorProgress
)

type scanProgressUpdate struct {
	Type  progressType
	Count int
}

type googleDriveDirectory struct {
	Path string
	Id   string
}

var ignoredExtensions = [...]string{".gdoc", ".gsheet", ".gmap", ".gslides", ".gdraw"}
var ignoredFiles = [...]string{"Icon\r", ".DS_Store"}
var ignoredDirectories = [...]string{"@eaDir", ".tmp.drivedownload"}

// lowercased by the time we filter
var ignoredRemoteFiles = [...]string{".ds_store"}

var localConflictMarkerRegexp = regexp.MustCompile(`\(slash conflict\)(/|$)`)
var trailingSpaceRegexp = regexp.MustCompile(` /`)

func main() {
	homeDir, err := homedir.Dir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Please set $HOME to a readable path!")
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	configDir := filepath.Join(homeDir, ".googledrive-sync-verifier")
	srv, err := NewDriveService(filepath.Join(configDir, "credentials.json"), filepath.Join(configDir, "token.json"))

	// Uncomment the following to allow profiling via http
	// go func() {
	// 	fmt.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	var opts struct {
		Verbose            bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		RemoteRoot         string `short:"r" long:"remote" description:"Directory in Google Drive to verify" default:""`
		LocalRoot          string `short:"l" long:"local" description:"Local directory to compare to Google Drive contents" default:"."`
		SelectiveSync      bool   `long:"selective" description:"Assume local is selectively synced - only check contents of top-level folders in local directory"`
		SkipContentHash    bool   `long:"skip-hash" description:"Skip checking content hash of local files"`
		WorkerCount        int    `short:"w" long:"workers" description:"Number of worker threads to use (defaults to 8) - set to 0 to use all CPU cores" default:"8"`
		FreeMemoryInterval int    `long:"free-memory-interval" description:"Interval (in seconds) to manually release unused memory back to the OS on low-memory systems" default:"0"`
		Synology           bool   `long:"synology" description:"Skip files known to have sync issues under Synology's Cloud Sync client"`
	}

	args, err := flags.Parse(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if len(args) > 0 {
		fmt.Fprintln(os.Stderr, "Extra arguments provided! Did you mean to use `--local`?")
		os.Exit(1)
	}

	localRoot, _ := filepath.Abs(opts.LocalRoot)
	var localDirs []string
	if opts.SelectiveSync {
		localDirs, err = listFolders(opts.LocalRoot)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}

	remoteRoot := opts.RemoteRoot
	if remoteRoot == "" {
		remoteRoot = defaultRemoteRoot(localRoot)
	}
	if remoteRoot[0] != '/' {
		remoteRoot = "/" + remoteRoot
	}

	if opts.SelectiveSync {
		fmt.Printf("Comparing subfolders of Google Drive directory \"%v\" to local directory \"%v\"\n", remoteRoot, localRoot)
	} else {
		fmt.Printf("Comparing Google Drive directory \"%v\" to local directory \"%v\"\n", remoteRoot, localRoot)
	}
	// TODO add caveat about using non-default remote root - may be slow with
	// many files in account since it's filtering post API calls
	if !opts.SkipContentHash {
		fmt.Println("Checking content hashes.")
	}
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = int(math.Max(1, float64(runtime.NumCPU())))
	}
	fmt.Printf("Using %d local worker threads.\n", workerCount)
	fmt.Println("")

	// set up manual garbage collection routine
	if opts.FreeMemoryInterval > 0 {
		go timedManualGC(opts.FreeMemoryInterval, opts.Verbose)
	}

	progressChan := make(chan *scanProgressUpdate)
	var wg sync.WaitGroup
	wg.Add(2)

	var driveManifest *FileHeap
	var driveError error
	go func() {
		driveManifest, driveError = getGoogleDriveManifest(progressChan, srv, remoteRoot, localDirs, opts.Synology)
		wg.Done()
	}()

	var localManifest *FileHeap
	var errored []*FileError
	var localErr error
	go func() {
		localManifest, errored, localErr = getLocalManifest(progressChan, localRoot, localDirs, opts.SkipContentHash, workerCount)
		wg.Done()
	}()

	go func() {
		remoteCount := 0
		localCount := 0
		errorCount := 0
		for update := range progressChan {
			switch update.Type {
			case remoteProgress:
				remoteCount = update.Count
			case localProgress:
				localCount = update.Count
			case errorProgress:
				errorCount = update.Count
			}

			if opts.Verbose {
				fmt.Fprintf(os.Stderr, "Scanning: %d (remote) %d (local) %d (errored)\r", remoteCount, localCount, errorCount)
			}
		}
		fmt.Fprintf(os.Stderr, "\n")
	}()

	// wait until remote and local scans are complete, then close progress reporting channel
	wg.Wait()
	close(progressChan)
	fmt.Printf("\nGenerated manifests for %d remote files, %d local files, with %d local errors\n\n", driveManifest.Len(), localManifest.Len(), len(errored))

	// check for fatal errors
	if driveError != nil {
		panic(driveError)
	}
	if localErr != nil {
		panic(localErr)
	}

	fmt.Println("")

	manifestComparison := compareManifests(driveManifest, localManifest, errored, opts.Synology)
	manifestComparison.PrintResults()

	if opts.SelectiveSync {
		fmt.Println("Subfolders verified:")
		for _, f := range localDirs {
			fmt.Println(f)
		}
	}

	if !manifestComparison.IsSuccessful() {
		os.Exit(1)
	}
}

func timedManualGC(freeMemoryInterval int, verbose bool) {
	for range time.Tick(time.Duration(freeMemoryInterval) * time.Second) {
		var m, m2 runtime.MemStats
		if verbose {
			runtime.ReadMemStats(&m)
		}
		debug.FreeOSMemory()
		if verbose {
			runtime.ReadMemStats(&m2)
			fmt.Fprintf(
				os.Stderr,
				"\n[%s] Alloc: %s -> %s / Sys: %s -> %s / HeapInuse: %s -> %s / HeapReleased: %s -> %s\n",
				time.Now().Format("15:04:05"),
				humanize.Bytes(m.Alloc),
				humanize.Bytes(m2.Alloc),
				humanize.Bytes(m.Sys),
				humanize.Bytes(m2.Sys),
				humanize.Bytes(m.HeapInuse),
				humanize.Bytes(m2.HeapInuse),
				humanize.Bytes(m.HeapReleased),
				humanize.Bytes(m2.HeapReleased),
			)
		}
	}
}

func defaultRemoteRoot(localRoot string) string {
	relPath := ""
	for {
		base := filepath.Base(localRoot)
		dir := filepath.Dir(localRoot)
		if base == "Google Drive" || base == "GoogleDrive" {
			return "/" + relPath
		} else if dir == "/" {
			return "/"
		} else {
			relPath = filepath.Join(base, relPath)
			localRoot = dir
		}
	}
}

func listFolders(localRoot string) (folders []string, err error) {
	root, err := filepath.Abs(localRoot)
	if err != nil {
		return
	}
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return
	}
	for _, f := range files {
		if f.IsDir() && !skipLocalDir(f.Name()) {
			folders = append(folders, f.Name())
		}
	}

	return
}

func getLocalManifest(progressChan chan<- *scanProgressUpdate, localRoot string, localDirs []string, skipContentHash bool, workerCount int) (manifest *FileHeap, errored []*FileError, err error) {
	contentHash := !skipContentHash
	localRootLowercase := strings.ToLower(localRoot)
	manifest = &FileHeap{}
	heap.Init(manifest)
	processChan := make(chan string)
	resultChan := make(chan *File)
	errorChan := make(chan *FileError)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		// spin up workers
		wg.Add(1)
		go handleLocalFile(localRootLowercase, contentHash, processChan, resultChan, errorChan, &wg)
	}

	// walk in separate goroutine so that sends to errorChan don't block
	go func() {
		var pathsToWalk []string
		if len(localDirs) > 0 {
			for _, dir := range localDirs {
				pathsToWalk = append(pathsToWalk, filepath.Join(localRoot, dir))
			}
		} else {
			pathsToWalk = append(pathsToWalk, localRoot)
		}
		for _, path := range pathsToWalk {
			filepath.Walk(path, func(entryPath string, info os.FileInfo, err error) error {
				if err != nil {
					errorChan <- &FileError{Path: entryPath, Error: err}
					return nil
				}

				if info.Mode().IsDir() && skipLocalDir(entryPath) {
					return filepath.SkipDir
				}

				if info.Mode().IsRegular() && !skipLocalFile(entryPath) {
					processChan <- entryPath
				}

				return nil
			})
		}

		close(processChan)
	}()

	// Once processing goroutines are done, close result and error channels to indicate no more results streaming in
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	for {
		select {
		case result, ok := <-resultChan:
			if ok {
				heap.Push(manifest, result)
				progressChan <- &scanProgressUpdate{Type: localProgress, Count: manifest.Len()}
			} else {
				resultChan = nil
			}

		case e, ok := <-errorChan:
			if ok {
				errored = append(errored, e)
				progressChan <- &scanProgressUpdate{Type: errorProgress, Count: len(errored)}
			} else {
				errorChan = nil
			}
		}

		if resultChan == nil && errorChan == nil {
			break
		}
	}

	return
}

// fill in args etc
func handleLocalFile(localRootLowercase string, contentHash bool, processChan <-chan string, resultChan chan<- *File, errorChan chan<- *FileError, wg *sync.WaitGroup) {
	for entryPath := range processChan {
		relPath, err := relativePath(localRootLowercase, strings.ToLower(entryPath))
		if err != nil {
			errorChan <- &FileError{Path: entryPath, Error: err}
			continue
		}
		relPath = normalizeUnicodeCharacters(relPath)
		filteredPath := filterLocalPath(relPath)
		originalPath := ""
		if relPath != filteredPath {
			originalPath = relPath
		}

		hash := ""
		if contentHash {
			hash, err = hashLocalFile(entryPath)
			if err != nil {
				// use relPath here because the error relates to the local file
				errorChan <- &FileError{Path: relPath, Error: err}
				continue
			}
		}

		resultChan <- &File{
			Path:         filteredPath,
			OriginalPath: originalPath,
			ContentHash:  hash,
		}
	}
	wg.Done()
}

func hashLocalFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func relativePath(root string, entryPath string) (string, error) {
	relPath, err := filepath.Rel(root, entryPath)
	if err != nil {
		return "", err
	}
	if len(relPath) >= 3 && relPath[0:3] == "../" {
		// try lowercase root instead
		relPath, err = filepath.Rel(strings.ToLower(root), entryPath)
		if err != nil {
			return "", err
		}
	}

	return relPath, nil
}

// Normalize Unicode combining characters
func normalizeUnicodeCharacters(entryPath string) string {
	return norm.NFC.String(entryPath)
}

func filterLocalPath(entryPath string) string {
	filtered := entryPath
	filtered = localConflictMarkerRegexp.ReplaceAllString(filtered, "$1")
	return filtered
}

func filterRemotePath(entryPath string, synologyMode bool) string {
	if synologyMode {
		return trailingSpaceRegexp.ReplaceAllString(entryPath, "/")
	}
	return entryPath
}

func skipLocalFile(path string) bool {
	base := filepath.Base(path)
	for _, ignoredFile := range ignoredFiles {
		if base == ignoredFile {
			return true
		}
	}

	ext := filepath.Ext(path)
	for _, ignoredExt := range ignoredExtensions {
		if ext == ignoredExt {
			return true
		}
	}

	return false
}

func skipLocalDir(path string) bool {
	base := filepath.Base(path)
	for _, ignore := range ignoredDirectories {
		if base == ignore {
			return true
		}
	}
	return false
}

func skipRemoteFile(path string) bool {
	base := filepath.Base(path)
	for _, ignoredFile := range ignoredRemoteFiles {
		if base == ignoredFile {
			return true
		}
	}

	return false
}

func getGoogleDriveManifest(progressChan chan<- *scanProgressUpdate, srv *drive.Service, rootPath string, subdirectories []string, synologyMode bool) (manifest *FileHeap, err error) {
	manifest = &FileHeap{}
	heap.Init(manifest)

	listing := NewDriveListing(srv, rootPath, subdirectories)
	updateChan := make(chan int)
	go func() {
		for updateCount := range updateChan {
			progressChan <- &scanProgressUpdate{Type: remoteProgress, Count: updateCount}
		}
	}()
	files, err := listing.Files(updateChan)
	if err != nil {
		return
	}
	for _, file := range files {
		if skipRemoteFile(file.Path) {
			continue
		}
		originalPath := file.Path
		file.Path = filterRemotePath(file.Path, synologyMode)
		if file.Path != originalPath {
			file.OriginalPath = originalPath
		}
		heap.Push(manifest, file)
	}

	return manifest, nil
}
