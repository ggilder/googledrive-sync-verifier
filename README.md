# googledrive-sync-verifier
Validate a local directory against Google Drive to make sure all your files are correctly synced.

## Notes

Cross-compile to make a binary that can run on the Synology NAS:

```
GOOS=linux GOARCH=amd64 go build
```
