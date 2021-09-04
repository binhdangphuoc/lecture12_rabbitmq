package main

import (
	"context"
	"database/sql"
	"fmt"
	"lecture12_rabbitmq/native/rabbitmq"
	"lecture12_rabbitmq/native/scheduler"
	"time"

	"lecture12_rabbitmq/native/mail"
	"lecture12_rabbitmq/native/worker"

	_ "github.com/go-sql-driver/mysql"
)
const (
	driver 		= 	"mysql"
	dataSource 	= 	"root:@tcp(localhost:3306)/"
	database 	= 	"db1"
)

func main() {
	db,err := sql.Open(driver,dataSource+database)
	if err != nil {
		fmt.Println("Can not connect mysql")
		return
	}
	defer db.Close()
	ctx := context.Context(context.Background())
	testMQ := rabbitmq.NewChannelMQ("test")

	go func() {
		sched := scheduler.NewScheduler(db, testMQ, ctx)
		sched.Start()
	}()

	mySendgrid := mail.NewSendgrid("apiKey")
	emailer := mail.Emailer(mySendgrid)
	testWork := worker.NewWorker(emailer, testMQ, db, ctx)
	testWork.Start()
	fmt.Println("Exit at ",time.Now())
}
