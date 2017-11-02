package utils

// Use AWS SDK to load a file into S3. Additional options include zipping on the fly and archiving existing files to a different S3 path
import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func LoadToS3(fpath, bucket, s3path, access_key_id, secret_access_key, region string, archive bool) { //
	// String manipulation to get file name from the S3 path
	sl := strings.Split(fpath, "/")
	filename := sl[len(sl)-1]
	token := ""

	// Use credentials to create a new session
	creds := credentials.NewStaticCredentials(access_key_id, secret_access_key, token)
	_, err := creds.Get()
	if err != nil {
		fmt.Printf("bad credentials: %s", err)
	}
	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)

	// If archive is true:
	if archive {
		fmt.Println("archive handler")

		// Get all the objects in the bucket and s3 path
		resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String(s3path)})
		if err != nil {
			fmt.Println("Unable to list items in bucket %q, %v", bucket, err)
		}
		num_objs := len(resp.Contents)

		// Create Delete object with slots for the objects to delete
		var items s3.Delete
		var objs = make([]*s3.ObjectIdentifier, num_objs)

		// Iterate through the objects and copy them to the Archive folder within the same bucket
		for i, o := range resp.Contents {
			objs[i] = &s3.ObjectIdentifier{Key: aws.String(*o.Key)}
			fmt.Println(*o.Key)
			_, err := svc.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucket), CopySource: aws.String(bucket + "/" + *o.Key), Key: aws.String("Archive/TomTest/" + *o.Key)})
			if err != nil {
				fmt.Println("Unable to copy objects from bucket %q, %v", bucket, err)
			}

		}

		// Add list of objects to the Delete object and delete them
		items.SetObjects(objs)
		_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{Bucket: &bucket, Delete: &items})
		if err != nil {
			fmt.Println("Unable to delete objects from bucket %q, %v", bucket, err)
		}
		fmt.Println("Deleted", num_objs, "object(s) from bucket", bucket)

	}

	// Archive end

	// Open the file
	file, err := os.Open(fpath)
	if err != nil {
		fmt.Printf("err opening file %q: %s", filename, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("err getting file stat")
	}
	var size int64 = fileInfo.Size()

	// Create a buffer
	buffer := make([]byte, size)
	file.Read(buffer)
	//fileBytes := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)

	// Use s3.PutObject while including a bunch of parameters such as Bucket, Key, Body etc
	params := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(s3path + filename),
		Body:          file,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(fileType),
		Metadata: map[string]*string{
			"Key": aws.String("MetadataValue"),
		},
	}

	resp, err := svc.PutObject(params)
	if err != nil {
		fmt.Printf("bad response: %s", err)
	}
	fmt.Printf("response %s", awsutil.StringValue(resp))
}

func LoadToS3Gzip(fpath, bucket, s3path, access_key_id, secret_access_key, region string) {
	// String manipulation to get file name from the S3 path
	sl := strings.Split(fpath, "/")
	filename := sl[len(sl)-1]
	token := ""

	// Use credentials to create a new session
	creds := credentials.NewStaticCredentials(access_key_id, secret_access_key, token)
	_, err := creds.Get()
	if err != nil {
		fmt.Printf("bad credentials: %s", err)
	}
	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)
	sess := session.New(cfg)

	file, err := os.Open(fpath)
	if err != nil {
		fmt.Printf("err opening file %q: %s", filename, err)
	}

	// Use io.Pipe to create a reader and writer to zip the file on the fly
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		io.Copy(gw, file)
		file.Close()
		gw.Close()
		writer.Close()
	}()

	// Replace filename with ".gz" extension
	filename = strings.Replace(filename, ".csv", ".gz", -1)

	// Use s3manager.newUploader
	uploader := s3manager.NewUploader(sess)

	// Use the Uploader object, and call the Upload method with a few parameters
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(bucket),
		Key:    aws.String(s3path + filename),
	})
	if err != nil {
		log.Fatalln("Failed to upload", err)
	}

	log.Println("Successfully uploaded to", result.Location)

}
