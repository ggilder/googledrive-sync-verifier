package main

import (
	"container/heap"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jessevdk/go-flags"

	"golang.org/x/text/unicode/norm"
	"google.golang.org/api/drive/v3"
)

// TODO
/*
- Speed up remote manifest! It's so slow!
- Revise local manifest to ignore placeholder google docs files - file extensions gsheet, gdoc, gmap
- REFACTOR! especially main, and passing around drive service object everywhere
*/

// Uncomment the following to allow profiling via http
// import "net/http"
// import _ "net/http/pprof"

// File stores the result of either Dropbox API or local file listing
type File struct {
	Path        string
	ContentHash string
}

// FileError records a local file that could not be read due to an error
type FileError struct {
	Path  string
	Error error
}

// ManifestComparison records the relative paths that differ between remote and
// local versions of a directory
type ManifestComparison struct {
	OnlyRemote      []string
	OnlyLocal       []string
	ContentMismatch []string
	Errored         []*FileError
	Matches         int
	Misses          int
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

func main() {
	srv, err := NewGoogleDriveService("credentials.json")

	// Uncomment the following to allow profiling via http
	// go func() {
	// 	fmt.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	var opts struct {
		Verbose            bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		RemoteRoot         string `short:"r" long:"remote" description:"Directory in Google Drive to verify" default:""`
		LocalRoot          string `short:"l" long:"local" description:"Local directory to compare to Google Drive contents" default:"."`
		SkipContentHash    bool   `long:"skip-hash" description:"Skip checking content hash of local files"`
		WorkerCount        int    `short:"w" long:"workers" description:"Number of worker threads to use (defaults to 8) - set to 0 to use all CPU cores" default:"8"`
		FreeMemoryInterval int    `long:"free-memory-interval" description:"Interval (in seconds) to manually release unused memory back to the OS on low-memory systems" default:"0"`
	}

	_, err = flags.Parse(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	localRoot, _ := filepath.Abs(opts.LocalRoot)

	remoteRoot := opts.RemoteRoot
	if remoteRoot == "" {
		remoteRoot = defaultRemoteRoot(localRoot)
	}
	if remoteRoot[0] != '/' {
		remoteRoot = "/" + remoteRoot
	}

	fmt.Printf("Comparing Google Drive directory \"%v\" to local directory \"%v\"\n", remoteRoot, localRoot)
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
		driveManifest, driveError = getGoogleDriveManifest(progressChan, srv, remoteRoot)
		wg.Done()
	}()

	var localManifest *FileHeap
	var errored []*FileError
	var localErr error
	go func() {
		localManifest, errored, localErr = getLocalManifest(progressChan, localRoot, opts.SkipContentHash, workerCount)
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

	manifestComparison := compareManifests(driveManifest, localManifest, errored)

	fmt.Println("")

	printFileList(manifestComparison.OnlyRemote, "Files only in remote")
	printFileList(manifestComparison.OnlyLocal, "Files only in local")
	printFileList(manifestComparison.ContentMismatch, "Files whose contents don't match")

	fmt.Printf("Errored: %d\n\n", len(manifestComparison.Errored))
	if len(manifestComparison.Errored) > 0 {
		for _, rec := range manifestComparison.Errored {
			fmt.Printf("%s: %s\n", rec.Path, rec.Error)
		}
		if len(manifestComparison.Errored) > 0 {
			fmt.Print("\n\n")
		}
	}

	total := manifestComparison.Matches + manifestComparison.Misses
	fmt.Println("SUMMARY:")
	fmt.Printf("Files matched: %d/%d\n", manifestComparison.Matches, total)
	fmt.Printf("Files not matched: %d/%d\n", manifestComparison.Misses, total)
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

func getLocalManifest(progressChan chan<- *scanProgressUpdate, localRoot string, skipContentHash bool, workerCount int) (manifest *FileHeap, errored []*FileError, err error) {
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
		filepath.Walk(localRoot, func(entryPath string, info os.FileInfo, err error) error {
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
		relPath = normalizePath(relPath)

		hash := ""
		if contentHash {
			hash, err = hashLocalFile(entryPath)
			if err != nil {
				errorChan <- &FileError{Path: relPath, Error: err}
				continue
			}
		}

		resultChan <- &File{
			Path:        relPath,
			ContentHash: hash,
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
	if relPath[0:3] == "../" {
		// try lowercase root instead
		relPath, err = filepath.Rel(strings.ToLower(root), entryPath)
		if err != nil {
			return "", err
		}
	}

	return relPath, nil
}

func normalizePath(entryPath string) string {
	// Normalize Unicode combining characters
	return norm.NFC.String(entryPath)
}

func skipLocalFile(path string) bool {
	if filepath.Base(path) == ".DS_Store" {
		return true
	}
	return false
}

func skipLocalDir(path string) bool {
	if filepath.Base(path) == "@eaDir" {
		return true
	}
	return false
}

func getGoogleDriveManifest(progressChan chan<- *scanProgressUpdate, srv *drive.Service, rootPath string) (manifest *FileHeap, err error) {
	manifest = &FileHeap{}
	heap.Init(manifest)

	rootId := "root"
	if rootPath != "/" {
		rootId, err = resolveGoogleDrivePath(srv, rootPath)
		if err != nil {
			return nil, err
		}
	}

	walkGoogleDriveDirectory(srv, rootId, func(file *File) error {
		heap.Push(manifest, file)
		progressChan <- &scanProgressUpdate{Type: remoteProgress, Count: manifest.Len()}
		return nil
	})

	return manifest, nil
}

func resolveGoogleDrivePath(srv *drive.Service, path string) (string, error) {
	pathParts := pathComponents(path)
	nextId := "root"

	for _, targetPart := range pathParts {
		targetId := ""
		files, err := listGoogleDriveDirectory(srv, nextId)
		if err != nil {
			return "", err
		}

		for _, file := range files {
			if file.MimeType == "application/vnd.google-apps.folder" && file.Name == targetPart {
				targetId = file.Id
				break
			}
		}

		if targetId == "" {
			return "", errors.New(fmt.Sprintf("Can't resolve directory \"%s\" in path \"%s\"", targetPart, path))
		}
		nextId = targetId
	}

	return nextId, nil
}

func pathComponents(targetPath string) (components []string) {
	if targetPath[0] != '/' {
		targetPath = "/" + targetPath
	}
	for targetPath != "/" {
		dir := path.Dir(targetPath)
		base := path.Base(targetPath)
		components = append([]string{base}, components...)
		targetPath = dir
	}
	return
}

func walkGoogleDriveDirectory(srv *drive.Service, rootId string, walkFunc func(file *File) error) error {
	var parent *googleDriveDirectory
	toWalk := []*googleDriveDirectory{{Id: rootId, Path: ""}}

	for len(toWalk) > 0 {
		parent, toWalk = toWalk[0], toWalk[1:]
		files, err := listGoogleDriveDirectory(srv, parent.Id)
		if err != nil {
			return err
		}
		for _, file := range files {
			if file.MimeType == "application/vnd.google-apps.folder" {
				toWalk = append(toWalk, &googleDriveDirectory{Path: path.Join(parent.Path, file.Name), Id: file.Id})
			} else if file.Md5Checksum != "" {
				normalizedPath := strings.ToLower(normalizePath(path.Join(parent.Path, file.Name)))
				err := walkFunc(&File{Path: normalizedPath, ContentHash: file.Md5Checksum})
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func listGoogleDriveDirectory(srv *drive.Service, id string) (files []*drive.File, err error) {
	nextPageToken := ""

	for {
		result, err := srv.Files.List().
			PageToken(nextPageToken).
			PageSize(1000).
			Fields("nextPageToken, files(id, name, parents, ownedByMe, trashed, md5Checksum, mimeType)").
			Q("trashed != true and '" + id + "' in parents").
			Do()
		if err != nil {
			break
		}

		nextPageToken = result.NextPageToken
		files = append(files, result.Files...)

		if nextPageToken == "" {
			break
		}
	}

	return files, err
}

func compareManifests(remoteManifest, localManifest *FileHeap, errored []*FileError) *ManifestComparison {
	// 1. Pop a path off both remote and local manifests.
	// 2. While remote & local are both not nil:
	//    Compare remote & local:
	//    a. If local is nil or local > remote, this file is only in remote. Record and pop remote again.
	//    b. If remote is nil or local < remote, this file is only in local. Record and pop local again.
	//    c. If local == remote, check for content mismatch. Record if necessary and pop both again.
	comparison := &ManifestComparison{Errored: errored}
	local := localManifest.PopOrNil()
	remote := remoteManifest.PopOrNil()
	for local != nil || remote != nil {
		if local == nil {
			comparison.OnlyRemote = append(comparison.OnlyRemote, remote.Path)
			comparison.Misses++
			remote = remoteManifest.PopOrNil()
		} else if remote == nil {
			comparison.OnlyLocal = append(comparison.OnlyLocal, local.Path)
			comparison.Misses++
			local = localManifest.PopOrNil()
		} else if local.Path > remote.Path {
			comparison.OnlyRemote = append(comparison.OnlyRemote, remote.Path)
			comparison.Misses++
			remote = remoteManifest.PopOrNil()
		} else if local.Path < remote.Path {
			comparison.OnlyLocal = append(comparison.OnlyLocal, local.Path)
			comparison.Misses++
			local = localManifest.PopOrNil()
		} else {
			// this must mean that remote.Path == local.Path
			if compareFileContents(remote, local) {
				comparison.Matches++
			} else {
				comparison.ContentMismatch = append(comparison.ContentMismatch, local.Path)
				comparison.Misses++
			}
			local = localManifest.PopOrNil()
			remote = remoteManifest.PopOrNil()
		}
	}
	return comparison
}

func compareFileContents(remote, local *File) bool {
	if remote.ContentHash == "" || local.ContentHash == "" {
		// Missing content hash for one of the files, possibly intentionally,
		// so can't compare. Assume that presence of both is enough to
		// validate.
		return true
	}
	return remote.ContentHash == local.ContentHash
}

func printFileList(files []string, description string) {
	fmt.Printf("%s: %d\n\n", description, len(files))
	for _, path := range files {
		fmt.Println(path)
	}
	if len(files) > 0 {
		fmt.Print("\n\n")
	}
}
