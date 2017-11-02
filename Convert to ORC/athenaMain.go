package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

func main() {

	// Create a Athena client with additional configuration
	creds := credentials.NewStaticCredentials("access_key_id", "secret_key", "")
	svc := athena.New(session.New(), aws.NewConfig().WithRegion("us-east-1").WithCredentials(creds))

	var s athena.StartQueryExecutionInput
	s.SetQueryString("SELECT * FROM d_agency_rpm limit 10;")

	var q athena.QueryExecutionContext
	q.SetDatabase("dev")
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	r.SetOutputLocation("s3://bucket")
	s.SetResultConfiguration(&r)

	result, err := svc.StartQueryExecution(&s)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("StartQueryExecution result:")
	fmt.Println(result.GoString())

}
