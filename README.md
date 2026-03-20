# googledrive-sync-verifier
Validate a local directory against Google Drive to make sure all your files are correctly synced.

## Install

Install from this repository (module-aware Go):

```
go install .
```

Or install directly by module path:

```
go install github.com/ggilder/googledrive-sync-verifier@latest
```

## Notes

Cross-compile to make a binary that can run on the Synology NAS:

```
GOOS=linux GOARCH=amd64 go build
```
