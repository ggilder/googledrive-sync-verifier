package main

import "errors"

type DriveDirectoryCache struct {
	Parents           map[string]string
	Paths             map[string]string
	Names             map[string]string
	SharedDirectories map[string]bool
}

func NewDriveDirectoryCache(rootId string) *DriveDirectoryCache {
	d := DriveDirectoryCache{}

	d.Parents = make(map[string]string)
	d.Paths = make(map[string]string)
	d.Names = make(map[string]string)
	d.SharedDirectories = make(map[string]bool)

	d.Paths[rootId] = ""

	return &d
}

func (d *DriveDirectoryCache) AddFolder(id, name, parentId string) {
	d.Parents[id] = parentId
	d.Names[id] = name
}

// TODO use custom error type
func (d *DriveDirectoryCache) PathLookup(id string) (string, error) {
	if path, ok := d.Paths[id]; ok {
		return path, nil
	}
	if _, ok := d.SharedDirectories[id]; ok {
		return "", errors.New("Shared directory")
	}
	if parentId, ok := d.Parents[id]; !ok {
		d.SharedDirectories[id] = true
		return "", errors.New("Shared directory")
	} else {
		parentPath, err := d.PathLookup(parentId)
		if err != nil {
			d.SharedDirectories[id] = true
			return "", errors.New("Shared directory")
		} else {
			d.Paths[id] = parentPath + "/" + d.Names[id]
			return d.Paths[id], nil
		}
	}
}
