package main

// Lambda function that ingests data into Aurora RDS once a file is put inside a specified S3 path
import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/eawsy/aws-lambda-go-core/service/lambda/runtime"
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/s3evt"
	_ "github.com/go-sql-driver/mysql"
)

// Grabs the bucket that the go-app lambda function is connected to
func getBucket(evt *s3evt.Event) string {
	return evt.Records[0].S3.Bucket.Name
}

// Grabs the name of the file uploaded to S3 and its path
func getFilenameAndPath(evt *s3evt.Event) (string, string) {
	path := evt.Records[0].S3.Object.Key
	sl := strings.Split(path, "/")
	filename := sl[len(sl)-1]
	return filename, path
}

// Gets the option and table name from the file name
func getOptionAndTablename(filename string) (string, string) {
	components := strings.Split(filename, "-")
	option := components[0]
	tablename := components[1]
	return option, tablename
}

func Handle(evt *s3evt.Event, ctx *runtime.Context) (interface{}, error) {

	// Use driver to access Aurora database
	var (
		//id         int
		country    string
		tennis     string
		basketball string
		columnName string
	)
	db, err := sql.Open("mysql", "turbine:password@tcp(endpoint)/development") 
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// First actual connection to check that the Aurora database is available and accessible
	err = db.Ping()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Grabs the bucket that the go-app lambda function is connected to
	// Bucket Example: as-lambda-test
	bucket := getBucket(evt)
	log.Println("The bucket name is " + bucket)

	// Grabs the name of the file uploaded to S3 and its path
	// Path Example: Folder1/append-sports-2017.csv
	// Filename Example: append-sports-2017.csv
	filename, path := getFilenameAndPath(evt)
	log.Println("The path is " + path)
	log.Println("The file name is " + filename)

	// Gets the option and table name from the file name
	// Option example: append
	// Table name example: sports
	option, tablename := getOptionAndTablename(filename)
	log.Println("The option is " + option)
	log.Println("The table name is " + tablename)

	// Find the S3 Path of the file
	// s3://as-lambda-test/Folder1/append-sports-2017.csv
	s3path := "s3://" + bucket + "/" + path
	log.Println("The s3 path is " + s3path)

	// Access Database
	_, err = db.Exec("USE development")
	if err != nil {
		panic(err)
	}

	// Create the table if it doesn't exist already
	createQuery := `
	CREATE TABLE IF NOT EXISTS %s (
	    country varchar(255)  NOT NULL,
	    tennis varchar(255) NOT NULL,
	    basketball varchar(255) NOT NULL
	);
	`
	createQuery = fmt.Sprintf(createQuery, tablename)
	_, err = db.Exec(createQuery)
	if err != nil {
		log.Fatal(err)
	}

	// Cases for append or truncate
	if option == "append" {
		log.Println("Appending...")
	} else if option == "truncate" {

		log.Println("Truncating...")
		truncateQuery := "TRUNCATE TABLE " + tablename
		_, err := db.Exec(truncateQuery)
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("NOOOO")
	}

	// Try obtaining all the column names
	var loadQueryComponent string = ""
	colNamesQuery := `select COLUMN_NAME
	 from information_schema.columns
	where TABLE_SCHEMA = 'development'
	  and TABLE_NAME = '%s'; `

	colNamesQuery = fmt.Sprintf(colNamesQuery, tablename)

	/*colNamesQuery := `select column_name
	from information_schema.columns
	 and table_name = ‘testing’;`*/

	colNames, err := db.Query(colNamesQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer colNames.Close()
	//colNames.Next()
	for colNames.Next() {
		err := colNames.Scan(&columnName)
		loadQueryComponent = loadQueryComponent + columnName + ", "
		if err != nil {
			log.Fatal(err)
		}
		log.Println(columnName)
	}
	err = colNames.Err()
	if err != nil {
		log.Fatal(err)
	}
	loadQueryComponent = loadQueryComponent[0 : len(loadQueryComponent)-2]
	log.Println("The column names component is: " + loadQueryComponent)

	// Load from S3 using newly configured S3 path and column names
	query := `
	  LOAD DATA FROM S3 %s
	  INTO TABLE %s
	  FIELDS TERMINATED BY ','
	  ENCLOSED BY '"'
	  ESCAPED BY '\\'
	  LINES TERMINATED BY '\n'
	  (%s);
	  `
	loadQuery := fmt.Sprintf(query, "'"+s3path+"'", tablename, loadQueryComponent)

	log.Println("Query is " + loadQuery)
	_, err = db.Exec(loadQuery)
	if err != nil {
		log.Println("The exact error string is: " + err.Error())
		log.Fatal(err)
	}

	// TESTING
	// Show query results on CloudWatch
	rows, err := db.Query("select country, tennis, basketball from testing")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&country, &tennis, &basketball)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(country, tennis, basketball)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("YAYYY")

	return nil, nil
}
