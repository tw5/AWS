package main

// Ingest data in an S3 path into Aurora RDS 
import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	LoadToAurora("s3://bucket/Loaded_Data/table.csv", "tablename", true) //
}

func LoadToAurora(s3filepath string, tablename string, truncate bool) {

	// Use driver to access Aurora database
	var (
		columnName string
	)

	db, err := sql.Open("mysql", "backend_new:password@tcp(endpoint:3306)/development")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// First actual connection to check that the Aurora database is available and accessible
	err = db.Ping()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Access Database
	_, err = db.Exec("USE development")
	if err != nil {
		panic(err)
	}

	// Create the table if it doesn't exist already
	/*createQuery := `
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
	}*/

	// Truncate table if necessary
	if truncate {
		fmt.Println("Truncating...")
		truncateQuery := "TRUNCATE TABLE " + tablename
		_, err := db.Exec(truncateQuery)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("Appending...")
	}

	// Try obtaining all the column names
	var loadQueryComponent string = ""
	colNamesQuery := `select COLUMN_NAME
	 from information_schema.columns
	where TABLE_SCHEMA = 'development'
	  and TABLE_NAME = '%s'; `

	colNamesQuery = fmt.Sprintf(colNamesQuery, tablename)

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
		fmt.Println(columnName)
	}
	err = colNames.Err()
	if err != nil {
		log.Fatal(err)
	}
	loadQueryComponent = loadQueryComponent[0 : len(loadQueryComponent)-2]
	fmt.Println("The column names component is: " + loadQueryComponent)

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
	loadQuery := fmt.Sprintf(query, "'"+s3filepath+"'", tablename, loadQueryComponent)

	log.Println("Query is " + loadQuery)
	_, err = db.Exec(loadQuery)
	if err != nil {
		fmt.Println("The exact error string is: " + err.Error())
		log.Fatal(err)
	}

}
