package main

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// ManifestComparison records the relative paths that differ between remote and
// local versions of a directory
type ManifestComparison struct {
	OnlyRemote      []*File
	OnlyLocal       []*File
	ContentMismatch []string
	PossibleMatches []*PossibleMatch
	KnownSyncIssues []string
	Errored         []*FileError
	Matches         int
	Misses          int
}

type PossibleMatch struct {
	LocalPath  string
	RemotePath string
}

var possibleDuplicateRegexp = regexp.MustCompile(` \(1\)(/|$)`)

func compareManifests(remoteManifest, localManifest *FileHeap, errored []*FileError, synologyMode bool) *ManifestComparison {
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
			comparison.OnlyRemote = append(comparison.OnlyRemote, remote)
			comparison.Misses++
			remote = remoteManifest.PopOrNil()
		} else if remote == nil {
			comparison.OnlyLocal = append(comparison.OnlyLocal, local)
			comparison.Misses++
			local = localManifest.PopOrNil()
		} else if local.Path > remote.Path {
			comparison.OnlyRemote = append(comparison.OnlyRemote, remote)
			comparison.Misses++
			remote = remoteManifest.PopOrNil()
		} else if local.Path < remote.Path {
			comparison.OnlyLocal = append(comparison.OnlyLocal, local)
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
	if synologyMode {
		comparison.FindKnownSyncIssues()
	}
	comparison.FindPossibleMatches()
	return comparison
}

func compareFileContents(remote, local *File) bool {
	// if remote.ContentHash == "" || local.ContentHash == "" {
	// 	// Missing content hash for one of the files, possibly intentionally,
	// 	// so can't compare. Assume that presence of both is enough to
	// 	// validate.
	// 	return true
	// }
	return remote.ContentHash == local.ContentHash
}

func (mc *ManifestComparison) FindPossibleMatches() {
	remoteMatchIndices := []int{}
	for i, remoteFile := range mc.OnlyRemote {
		for j, localFile := range mc.OnlyLocal {
			if isPossibleMatch(remoteFile, localFile) {
				mc.PossibleMatches = append(
					mc.PossibleMatches,
					&PossibleMatch{
						LocalPath:  localFile.Path,
						RemotePath: remoteFile.Path,
					},
				)
				// prepend index so highest gets deleted first
				remoteMatchIndices = append([]int{i}, remoteMatchIndices...)
				mc.OnlyLocal = deleteFromSlice(mc.OnlyLocal, j)
				// misses decreases by 2 because it counts entries in both OnlyLocal and OnlyRemote
				mc.Misses -= 2
				mc.Matches++
				break
			}
		}
	}
	for _, index := range remoteMatchIndices {
		mc.OnlyRemote = deleteFromSlice(mc.OnlyRemote, index)
	}
}

func deleteFromSlice(coll []*File, index int) []*File {
	copy(coll[index:], coll[index+1:])
	coll[len(coll)-1] = nil
	return coll[:len(coll)-1]
}

func isPossibleMatch(remoteFile, localFile *File) bool {
	// Content hash must match
	if remoteFile.ContentHash != localFile.ContentHash {
		return false
	}
	// Try file path transformations to make paths match
	localPath := localFile.Path
	remotePath := remoteFile.Path

	// 1. Strip file extensions
	localPath = strings.TrimSuffix(localPath, filepath.Ext(localPath))
	remotePath = strings.TrimSuffix(remotePath, filepath.Ext(remotePath))

	// 2. Remote duplication markers
	localPath = possibleDuplicateRegexp.ReplaceAllString(localPath, "$1")
	remotePath = possibleDuplicateRegexp.ReplaceAllString(remotePath, "$1")

	// 3. Special characters represented as underscores vs. spaces in files
	// downloaded by new (2021) version of Google Drive app
	localPath = strings.ReplaceAll(localPath, "_", " ")
	remotePath = strings.ReplaceAll(remotePath, "_", " ")

	// Test match
	if localPath == remotePath {
		return true
	}
	return false
}

// Filter known sync issues on Synology
func (mc *ManifestComparison) FindKnownSyncIssues() {
	// iterate in reverse so we can delete safely
	for i := len(mc.OnlyRemote) - 1; i >= 0; i-- {
		file := mc.OnlyRemote[i]
		if hasKnownSyncIssue(file.Path) {
			mc.KnownSyncIssues = append([]string{file.Path}, mc.KnownSyncIssues...)
			mc.OnlyRemote = deleteFromSlice(mc.OnlyRemote, i)
		}
	}
}

func hasKnownSyncIssue(path string) bool {
	return strings.Contains(path, ":")
}

func (mc *ManifestComparison) IsSuccessful() bool {
	return mc.Misses <= 0
}

func (mc *ManifestComparison) PrintResults() {
	mc.PrintStatus()
	printFileList(mc.OnlyRemote, "Files only in remote")
	printFileList(mc.OnlyLocal, "Files only in local")
	printStringList(mc.ContentMismatch, "Files whose contents don't match")
	printPossibleMatchList(mc.PossibleMatches, "Possible matches")
	printKnownSyncList(mc.KnownSyncIssues, "Known sync issues")
	mc.PrintErrored()
	mc.PrintSummary()
}

func (mc *ManifestComparison) PrintStatus() {
	if mc.IsSuccessful() {
		fmt.Printf("✅ SUCCESS: verified local sync.\n")
	} else {
		fmt.Printf("❌ FAILURE: %d sync mismatches detected.\n", mc.Misses)
	}
	fmt.Println("")
}

func printFileList(files []*File, description string) {
	fmt.Printf("%s: %d\n\n", description, len(files))
	for _, file := range files {
		fmt.Println(file.Path)
	}
	if len(files) > 0 {
		fmt.Print("\n\n")
	}
}

func printStringList(files []string, description string) {
	fmt.Printf("%s: %d\n\n", description, len(files))
	for _, path := range files {
		fmt.Println(path)
	}
	if len(files) > 0 {
		fmt.Print("\n\n")
	}
}

func printPossibleMatchList(matches []*PossibleMatch, description string) {
	fmt.Printf("%s: %d\n\n", description, len(matches))
	for _, match := range matches {
		fmt.Printf("\"%s\" -> \"%s\"\n", match.RemotePath, match.LocalPath)
	}
	if len(matches) > 0 {
		fmt.Print("\n\n")
	}
}

func printKnownSyncList(issues []string, description string) {
	fmt.Printf("%s: %d\n\n", description, len(issues))
	for _, path := range issues {
		fmt.Println(path)
	}
	if len(issues) > 0 {
		fmt.Print("\n\n")
	}
}

func (mc *ManifestComparison) PrintErrored() {
	fmt.Printf("Errored: %d\n\n", len(mc.Errored))
	if len(mc.Errored) > 0 {
		for _, rec := range mc.Errored {
			fmt.Printf("%s: %s\n", rec.Path, rec.Error)
		}
		if len(mc.Errored) > 0 {
			fmt.Print("\n\n")
		}
	}
}

func (mc *ManifestComparison) PrintSummary() {
	total := mc.Matches + mc.Misses
	fmt.Println("SUMMARY:")
	fmt.Printf("Files matched: %d/%d\n", mc.Matches, total)
	fmt.Printf("Files not matched: %d/%d\n", mc.Misses, total)
}
