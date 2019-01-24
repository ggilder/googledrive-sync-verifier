package main

import (
	"fmt"
)

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

func (mc *ManifestComparison) PrintResults() {
	printFileList(mc.OnlyRemote, "Files only in remote")
	printFileList(mc.OnlyLocal, "Files only in local")
	printFileList(mc.ContentMismatch, "Files whose contents don't match")
	mc.PrintErrored()
	mc.PrintSummary()
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
