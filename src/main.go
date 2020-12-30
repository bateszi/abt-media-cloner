package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type AbtImage struct {
	FileId        int64
	PostId        int64
	ExternalUrl   *url.URL
	FileCategory  string
	MimeType      string
	FileSize      int64
	LocalFilename string
	State         string
	Created       string
	Modified      string
	FileExt       string
	S3Url         string
	Attempts      int64
}

type AppConfig struct {
	Db   DbConfig  `json:"db"`
	Solr string    `json:"solr"`
	Aws  AwsConfig `json:"aws"`
}

type DbConfig struct {
	User     string `json:"user"`
	Password string `json:"pass"`
	Server   string `json:"server"`
	DbName   string `json:"dbName"`
}

type AwsConfig struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Secret   string `json:"secret"`
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
	Bucket   string `json:"bucket"`
	Folder   string `json:"folder"`
	ACL      string `json:"acl"`
}

type AbtSolrDocs []AbtSolrDocument

type AbtSolrDocument struct {
	Id        int64           `json:"id"`
	PostImage SolrSetDocument `json:"post_image"`
}

type SolrSetDocument struct {
	Set string `json:"set"`
}

func makeDbConnection(config AppConfig) (*sql.DB, error) {

	dbParams := make(map[string]string)
	dbParams["charset"] = "utf8mb4"

	dbConfig := mysql.Config{
		User:   config.Db.User,
		Passwd: config.Db.Password,
		Net:    "tcp",
		Addr:   config.Db.Server,
		DBName: config.Db.DbName,
		Params: dbParams,
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}

	fmt.Println("opened database connection")

	return db, nil
}

func getImagesFromDb(db *sql.DB) ([]AbtImage, error) {
	var images []AbtImage

	getRows, err := db.Query(
		"SELECT pk_file_id, fk_post_id, external_url, state, created, attempts " +
			"FROM rss_aggregator.files " +
			"WHERE state = 'pending' " +
			"AND created >= now() - INTERVAL 2 hour " +
			"ORDER BY created DESC",
	)

	if err != nil {
		return images, err
	}

	defer func(getRows *sql.Rows) {
		err := getRows.Close()
		if err != nil {
			panic(err)
		}
	}(getRows)

	for getRows.Next() {
		var pkFileId int64
		var fkPostId int64
		var attempts int64
		var externalUrl string
		var state string
		var created string

		err = getRows.Scan(
			&pkFileId,
			&fkPostId,
			&externalUrl,
			&state,
			&created,
			&attempts,
		)

		if err != nil {
			return images, err
		}

		externalUrlObj, err := url.Parse(externalUrl)

		if err != nil {
			return images, err
		}

		image := AbtImage{
			FileId:      pkFileId,
			PostId:      fkPostId,
			ExternalUrl: externalUrlObj,
			State:       state,
			Created:     created,
			Attempts:    attempts,
		}

		images = append(images, image)
	}

	return images, nil
}

func setIngestedFilename(image *AbtImage) {
	if image.FileExt != "" {
		image.LocalFilename = fmt.Sprintf(
			"%d.%d.%d%s", time.Now().Unix(), image.FileId, image.PostId, image.FileExt,
		)
	}
}

func fetchStoreImageFromUrl(image *AbtImage) error {
	fmt.Println("fetching", image.ExternalUrl.String())

	startRequest := time.Now()

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(image.ExternalUrl.String())

	if err != nil {
		return err
	}

	defer func(resp *http.Response) {
		err := resp.Body.Close()

		if err != nil {
			panic(err)
		}
	}(resp)

	fmt.Printf("took %v to get file\n", time.Since(startRequest))

	image.MimeType = resp.Header.Get("content-type")
	image.FileSize = resp.ContentLength

	if image.MimeType == "image/jpeg" {
		image.FileExt = ".jpg"
	} else if image.MimeType == "image/png" {
		image.FileExt = ".png"
	} else if image.MimeType == "image/gif" {
		image.FileExt = ".gif"
	} else if image.MimeType == "" {
		fileExt := filepath.Ext(image.ExternalUrl.String())

		if fileExt != "" {
			image.FileExt = fileExt
		}
	}

	if image.FileExt != "" {
		setIngestedFilename(image)

		out, err := os.Create(image.LocalFilename)

		if err != nil {
			return err
		}

		defer func(out *os.File) {
			err := out.Close()

			if err != nil {
				panic(err)
			}
		}(out)

		_, err = io.Copy(out, resp.Body)
	} else {
		return errors.New(fmt.Sprintf("invalid mime type: %s", image.MimeType))
	}

	return err
}

func uploadImageToCloud(s3Client *s3.S3, bucket string, baseFolder string, acl string, image *AbtImage) (string, error) {
	t := time.Now()
	dateTimeFolder := t.Format("20060102")
	s3ObjectKey := "/" + baseFolder + "/" + dateTimeFolder + "/" + image.LocalFilename

	file, err := os.Open(image.LocalFilename)

	if err != nil {
		return s3ObjectKey, err
	}

	object := s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(s3ObjectKey),
		Body:        file,
		ACL:         aws.String(acl),
		ContentType: aws.String(image.MimeType),
	}

	_, err = s3Client.PutObject(&object)

	if err != nil {
		return s3ObjectKey, err
	}

	return s3ObjectKey, err
}

func updateImageRefInDb(db *sql.DB, image AbtImage) error {
	stmt, err := db.Prepare("UPDATE `files` " +
		"SET `mime_type` = ?, `file_size` = ?, `ingested_uri` = ?, `state` = ?, `modified` = ?, attempts = attempts + 1 " +
		"WHERE `pk_file_id` = ?")

	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		image.MimeType,
		image.FileSize,
		image.S3Url,
		image.State,
		time.Now().UTC().Format("2006-01-02 15:04:05"),
		image.FileId,
	)

	return err
}

func updateSolrWithImageRef(image AbtImage, solrBaseUrl string) {
	docs := AbtSolrDocs{
		AbtSolrDocument{
			Id: image.PostId,
			PostImage: SolrSetDocument{
				Set: image.S3Url,
			},
		},
	}

	postBody, err := json.Marshal(docs)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	solrUrl := solrBaseUrl + "/update?commit=true"

	req, err := http.NewRequest("POST", solrUrl, bytes.NewBuffer(postBody))

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	defer func(cancel context.CancelFunc) {
		cancel()
	}(cancel)

	req = req.WithContext(ctx)

	httpClient := &http.Client{}

	resp, err := httpClient.Do(req)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(resp)
}

func deleteLocalImage(image AbtImage) error {
	err := os.Remove(image.LocalFilename)
	return err
}

func start() {
	fmt.Println("starting media cloner")

	encodedJson, err := ioutil.ReadFile("config/config.json")

	if err != nil {
		panic(err)
	}

	config := AppConfig{}

	err = json.Unmarshal(encodedJson, &config)

	if err != nil {
		panic(err)
	}

	db, err := makeDbConnection(config)

	if err != nil {
		fmt.Println("could not open db connection", err)
		return
	}

	defer func(db *sql.DB) {
		fmt.Println("closing database connection at", time.Now().Format(time.RFC1123Z))
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	images, err := getImagesFromDb(db)

	if err != nil {
		fmt.Println("error getting images from db", err)
		return
	}

	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(config.Aws.Key, config.Aws.Secret, ""),
		Endpoint:    aws.String(config.Aws.Endpoint),
		Region:      aws.String(config.Aws.Region),
	}

	newSession, err := session.NewSession(s3Config)

	if err != nil {
		fmt.Println("could not connect to s3 storage provider", err)
		return
	}

	s3Client := s3.New(newSession)

	var storedImages []AbtImage

	for _, image := range images {
		err := fetchStoreImageFromUrl(&image)

		if err != nil {
			fmt.Println("could not fetch image", image.ExternalUrl, err)

			if image.Attempts >= 3 {
				image.State = "failed"
				err := updateImageRefInDb(db, image)

				if err != nil {
					fmt.Println("could not update db with file's failed state", err)
				}
			} else {
				err := updateImageRefInDb(db, image)

				if err != nil {
					fmt.Println("could not increment file retrieval attempt", err)
				}
			}

			continue
		}

		fmt.Println("stored image to local from", image.ExternalUrl, "as", image.LocalFilename)
		storedImages = append(storedImages, image)

		image.S3Url, err = uploadImageToCloud(s3Client, config.Aws.Bucket, config.Aws.Folder, config.Aws.ACL, &image)

		if err != nil {
			fmt.Println("could not upload", image.ExternalUrl, "for this reason:", err)
			continue
		}

		fmt.Println("uploaded image to s3 account. URI is", image.S3Url)

		image.State = "retrieved"

		err = updateImageRefInDb(db, image)

		if err != nil {
			fmt.Println("could not update db with file's retrieved state", err)
		}

		updateSolrWithImageRef(image, config.Solr)
	}

	for _, image := range storedImages {
		err := deleteLocalImage(image)

		if err != nil {
			fmt.Println("could not delete", image.LocalFilename)
			continue
		}

		fmt.Println("removed local copy of file", image.LocalFilename)
	}
}

func runService(d time.Duration) {
	ticker := time.NewTicker(d)

	for _ = range ticker.C {
		start()
	}
}

func main() {
	start()

	interval := 10 * time.Minute
	go runService(interval)

	fmt.Println("starting ticker to clone media every", interval)

	// Run application indefinitely
	select {}
}
