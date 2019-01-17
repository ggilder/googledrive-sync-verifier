package main

import "container/heap"

// FileHeap is a list of Files sorted by path
type FileHeap []*File

func (h FileHeap) Len() int           { return len(h) }
func (h FileHeap) Less(i, j int) bool { return h[i].Path < h[j].Path }
func (h FileHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push a File onto the heap
func (h *FileHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*File))
}

// Pop a File off the heap
func (h *FileHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PopOrNil pops a File off the heap or returns nil if there's nothing left
func (h *FileHeap) PopOrNil() *File {
	if h.Len() > 0 {
		return heap.Pop(h).(*File)
	}
	return nil
}
