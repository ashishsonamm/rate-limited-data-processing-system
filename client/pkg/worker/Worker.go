package worker

import (
	"bufio"
	"fmt"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/bean"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/connection"
	"log"
	"time"
)

type Worker struct {
	id                int
	jobChannel        chan bean.Record
	quitChannel       chan bool
	connectionService connection.ConnectionService
}

func NewWorker(id int, jobChannel chan bean.Record, connectionService connection.ConnectionService) *Worker {
	return &Worker{
		id:                id,
		jobChannel:        jobChannel,
		quitChannel:       make(chan bool),
		connectionService: connectionService,
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			select {
			case job := <-w.jobChannel:
				// process job
				w.processJob(job)
			case <-w.quitChannel:
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.quitChannel <- true
}

func (w *Worker) processJob(record bean.Record) {
	log.Printf("worker %d started processing record %s", w.id, record.ID)
	conn, err := w.connectionService.GetConnection()
	if err != nil {
		return
	}
	//defer w.connectionService.ReleaseConnection(conn)

	if conn == nil { // no available connection
		return
	}

	time.Sleep(time.Second) // simulate some heavy task
	_, err = fmt.Fprint(conn, fmt.Sprintf("%s,%s\n", record.ID, record.Data))
	if err != nil {
		return
	}

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	log.Printf("worker %d processed record %s, got response: %s", w.id, record.ID, resp)
}
