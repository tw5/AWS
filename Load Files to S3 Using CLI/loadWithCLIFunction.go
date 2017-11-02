package utils

// Use AWS CLI to load a file into S3. Additional options include zipping and archiving existing files to a different S3 path
import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
)

func LoadToS3CLI(file, s3path string, archive bool, zip bool) {
	var copyToS3_command string
	fulls3path := s3path + file

	// Handle if archive is true
	if archive == true {
		archive_command := fmt.Sprintf("aws s3 cp %s s3://bucket/Archive/TomTest/ --recursive && aws s3 rm --recursive %s", s3path, s3path)
		archiveCommand := exec.Command("bash", "-c", archive_command)
		err := archiveCommand.Run()
		if err != nil {
			log.Println(err)
		}

	}

	// Handle if zip is true or false
	if zip == true {
		copyToS3_command = fmt.Sprintf("gzip %s && aws s3 cp %s %s", file, file+".gz", fulls3path+".gz")
	} else {
		copyToS3_command = fmt.Sprintf("aws s3 cp %s %s", file, fulls3path)
	}

	command := exec.Command("bash", "-c", copyToS3_command)
	var out bytes.Buffer
	command.Stdout = &out
	err := command.Run()
	if err != nil {
		log.Println(err)
	}

	fmt.Println(out.String())

}
