package utils

// Partitions a date interval by year, quarter, or month
import (
	"fmt"
	"log"
	"strconv"
)

func PartitionDates(startdate, enddate, partitionBy string) {
	startYear := startdate[0:4]
	startMonth := startdate[4:6]
	startDay := startdate[6:8]
	endYear := enddate[0:4]
	endMonth := enddate[4:6]
	endDay := enddate[6:8]
	fmt.Println("startYear: " + startYear)
	fmt.Println("startMonth: " + startMonth)
	fmt.Println("startDay: " + startDay)
	fmt.Println()
	fmt.Println("endYear: " + endYear)
	fmt.Println("endMonth: " + endMonth)
	fmt.Println("endDay: " + endDay)

	startYearInt, err := strconv.Atoi(startYear)
	if err != nil {
		log.Println(err)
	}
	endYearInt, err := strconv.Atoi(endYear)
	if err != nil {
		log.Println(err)
	}
	startMonthInt, err := strconv.Atoi(startMonth)
	if err != nil {
		log.Println(err)
	}
	endMonthInt, err := strconv.Atoi(endMonth)
	if err != nil {
		log.Println(err)
	}
	startDayInt, err := strconv.Atoi(startDay)
	if err != nil {
		log.Println(err)
	}
	endDayInt, err := strconv.Atoi(endDay)
	if err != nil {
		log.Println(err)
	}

	if startYearInt > endYearInt {
		fmt.Println("Start date has to come before the end date")
	} else if startYearInt == endYearInt {
		if startMonthInt > endMonthInt {
			fmt.Println("Start date has to come before the end date")
		} else if startMonthInt == endMonthInt {
			if startDayInt > endDayInt {
				fmt.Println("Start date has to come before the end date")
			} else {
				printPartitions(startdate, enddate, partitionBy, startYearInt, endYearInt, startMonthInt, endMonthInt, startDayInt, endDayInt)
			}
		} else {
			printPartitions(startdate, enddate, partitionBy, startYearInt, endYearInt, startMonthInt, endMonthInt, startDayInt, endDayInt)
		}
	} else {
		printPartitions(startdate, enddate, partitionBy, startYearInt, endYearInt, startMonthInt, endMonthInt, startDayInt, endDayInt)
	}

	/*
		if partitionBy == "year" {
			if startYear == endYear {
				fmt.Println(startdate + "     " + enddate)
			}

		}*/
}

func printPartitions(startdate, enddate, partitionBy string, startYearInt, endYearInt, startMonthInt, endMonthInt, startDayInt, endDayInt int) {
	if partitionBy == "year" {
		if startYearInt == endYearInt {
			fmt.Println(startdate + "   " + enddate)
		} else {
			t := strconv.Itoa(startYearInt)
			fmt.Println(startdate + "   " + t + "1231")

			for i := startYearInt; i < endYearInt; i++ {
				t = strconv.Itoa(i)
				fmt.Println(t + "0101" + "   " + t + "1231")
			}

			t = strconv.Itoa(endYearInt)
			fmt.Println(t + "0101" + "   " + enddate)
		}
	}
}
