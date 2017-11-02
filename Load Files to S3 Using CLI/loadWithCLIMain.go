package main

// Test
import (
	"flag"
	"utils"
)

func main() {
	file := flag.String("file", "", "File to Send to S3")
	s3path := flag.String("s3path", "s3://bucket/Loaded_Data/", "Path in the Bucket")
	archive := flag.Bool("archive", false, "Archive existing files in the S3 path?")
	zip := flag.Bool("zip", false, "Zip this file?")
	flag.Parse()
	utils.LoadToS3CLI(*file, *s3path, *archive, *zip)
}
