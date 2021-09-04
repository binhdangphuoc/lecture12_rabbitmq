package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"lecture12_rabbitmq/native/mail"
	"lecture12_rabbitmq/native/rabbitmq"
)

//struct Worker define a worker
type Worker struct {
	emailer 	mail.Emailer
	channel 	*rabbitmq.ChannelMQ
	db      	*sql.DB
	ctx			context.Context
}

//func NewWorker create a new Worker
func NewWorker(emailer mail.Emailer, MQchan *rabbitmq.ChannelMQ, db *sql.DB, ctx context.Context) *Worker {
	return &Worker{
		emailer: emailer,
		channel: MQchan,
		db:      db,
		ctx:     ctx,
	}
}

/*
func (Worker) Start starts Worker
process logic:
	1. Wait message in channel inChan
	2. Send email with each Emailer
	3. Update database (thankyou_email_sent = true)
*/
func (w *Worker) Start() {
	if w.emailer == nil || w.db == nil {
		fmt.Println("Can't start Worker with emailer nil (or db nil)")
		return
	}
	sttm,err := w.db.Prepare("UPDATE `order` SET thankyou_email_sent = ? WHERE id =?;")
	if err != nil {
		fmt.Println("Can't prepare statement to update thankyou_email_sent")
	}
	msgs, err := w.channel.ConsumeMQ()
	fmt.Println("Worker start sending ...")
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			fmt.Println("Have email to send ",d.Body)
			var ec mail.EmailContent
			err := json.Unmarshal(d.Body,&ec)
			if err != nil {
				fmt.Println("Can't unMarshal body msgs")
				continue
			}
			err = w.emailer.Send(&ec)
			if err != nil {
				fmt.Println("Can't send msgs in sendgrid")
				continue
			}
			_,err = sttm.Query(true, ec.Id)
			if err != nil {
				fmt.Println("Can't query to update thankyou_email_sent to true")
			}
		}
	}()
	<-forever
}