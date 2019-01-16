package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

func main() {
	b, err := ioutil.ReadFile("credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveMetadataReadonlyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config)

	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	// Get root id
	f, err := srv.Files.Get("root").Fields("id").Do()
	if err != nil {
		log.Fatalf("Unable to retrieve root: %v", err)
	}
	rootId := f.Id
	fmt.Printf("Root: %s\n", rootId)

	// Keep track of nesting
	driveDirectoryCache := NewDriveDirectoryCache(rootId)

	listPageSize := int64(1000)
	listFields := googleapi.Field("nextPageToken, files(id, name, parents, ownedByMe, trashed, md5Checksum, mimeType)")
	totalListed := 0
	pages := 0
	maxPages := 50
	nextPageToken := ""

	for pages <= maxPages {
		r, err := srv.Files.List().PageToken(nextPageToken).PageSize(listPageSize).
			Fields(listFields).
			Q("trashed != true").
			Do()
		if err != nil {
			log.Fatalf("Unable to retrieve files: %v", err)
			break
		}
		nextPageToken = r.NextPageToken
		pages++

		if len(r.Files) == 0 {
			fmt.Println("No files found.")
		} else {
			totalListed += len(r.Files)
			for _, i := range r.Files {
				// print debugging info
				// fmt.Printf("%s\n", i.Name)
				// fmt.Printf("Id: %s\n", i.Id)
				// fmt.Printf("Parents: %v\n", i.Parents)
				// fmt.Printf("Checksum: %v\n", i.Md5Checksum)
				// fmt.Printf("Type: %v\n", i.MimeType)
				// fmt.Println()

				// keep track of folders
				if i.MimeType == "application/vnd.google-apps.folder" && len(i.Parents) > 0 {
					driveDirectoryCache.AddFolder(i.Id, i.Name, i.Parents[0])
				}
			}
		}

		if nextPageToken == "" {
			break
		}
	}

	// TODO
	/*
		- Copy in code from Dropbox version
		- Anything that has a checksum should be verified against a local file
		- Maybe: if recursive folder lookup is too annoying (or listing shared
		  files takes too much time), could search with query "'root' in parents"
		  and then query each subfolder
	*/

	fmt.Printf("%d files listed\n", totalListed)

	for id, _ := range driveDirectoryCache.Parents {
		path, err := driveDirectoryCache.PathLookup(id)
		if err != nil {
			fmt.Printf("error for %s: %s\n", id, err)
		} else {
			fmt.Printf("path for %s: %s\n", id, path)
		}
	}
}

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
