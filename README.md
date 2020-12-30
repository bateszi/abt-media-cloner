# abt-media-cloner1

1. Pull media URL from queue
2. Download file to temp local location
3. Generate a unique name for the file
4. Upload file to s3 storage location 
5. Store reference (in db) to uploaded file, along with mime type, file size
6. Ping Solr
7. Delete tmp file