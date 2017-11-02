package main

// Test

import (
	"flag"
	"utils"
)

func main() {
	file := flag.String("file", "", "File to Send to S3")
	bucket := flag.String("bucket", "bucket", "Bucket name")
	s3path := flag.String("s3path", "Loaded_Data/", "Path in the Bucket")
	access_key_id := flag.String("id", "access_key_id", "Access Key ID")               // replace
	secret_access_key := flag.String("key", "secret_access_key", "Secret Access Key")  // replace
	region := flag.String("region", "us-east-1", "Region")
	archive := flag.Bool("archive", false, "archive?")
	flag.Parse()
	utils.LoadToS3(*file, *bucket, *s3path, *access_key_id, *secret_access_key, *region, *archive)
}
