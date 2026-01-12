package service

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/vinaymanala/nationpulse-data-ingestion-svc/internal/types"
	"github.com/vinaymanala/nationpulse-data-ingestion-svc/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	CURRENT_YEAR   = time.Now().Year()
	FORMER_YEAR    = CURRENT_YEAR - 16
	notifyMessage  *pb.NotifyBFFMessage
	isJobProccesed int32
)

type DataIngestionSvc struct {
	configs *types.Configs
	pb.UnimplementedDataIngestionServer
}

func NewDataIngestionSvc(configs *types.Configs) *DataIngestionSvc {
	return &DataIngestionSvc{
		configs: configs,
	}
}

func (d *DataIngestionSvc) Initialize() {
	log.Println("Cleaning up idle connections...")
	_, err := d.configs.DB.Client.Exec(d.configs.Ctx, `
		SELECT pg_terminate_backend(pid) 
		FROM pg_stat_activity 
		WHERE usename = 'postgres' 
		  AND pid <> pg_backend_pid();
	`)
	if err != nil {
		log.Printf("Warning: Could not kill idle connections: %v\n", err)
	}
	log.Println("Closed connections successfully..")
	time.Sleep(1 * time.Second)
}

func (d *DataIngestionSvc) Serve() {
	var wg sync.WaitGroup
	errCh := make(chan struct{})
	ctx, cancel := context.WithCancel(d.configs.Ctx)
	defer cancel()

	now := time.Now()
	indicators := map[string]string{
		"Population":       d.configs.Cfg.POPULATION_INDICATOR,
		"Health":           d.configs.Cfg.HEALTH_INDICATOR,
		"EconomyGDP":       d.configs.Cfg.ECONOMY_GDP_INDICATOR,
		"EconomyGov":       d.configs.Cfg.ECONOMY_GOV_INDICATOR,
		"GrowthGDP":        d.configs.Cfg.GROWTH_GDP_INDICATOR,
		"GrowthPopulation": d.configs.Cfg.GROWTH_POPULATION_INDICATOR,
	}

	for indicator, indicatorCode := range indicators {
		formattedUrl := ConstructOEDC_URL(d.configs.Cfg.BASE_URL, indicatorCode, strconv.Itoa(FORMER_YEAR))
		fmt.Println("URL constructed", formattedUrl)
		wg.Go(func() {
			ETLDataFeed(d.configs, ctx, formattedUrl, indicator, errCh)
		})

	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for e := range errCh {
		if e != struct{}{} {
			close(errCh)
			cancel()
			// isJobProccesed = 1
			return
		}
	}

	nm := &NotifyMessage{
		Message:     "All jobs processed successfully",
		Type:        MessageType(1),
		CreatedAt:   now,
		CompletedAt: time.Now(),
	}
	d.SetNotifyMessage(nm)
	// isJobProccesed = 1
	log.Println("Time taken to finish the job: ", time.Since(now))
}

func (d *DataIngestionSvc) SetNotifyMessage(message *NotifyMessage) {
	insertSqlStatement := `INSERT INTO jobScheduleLogs(message, type, created_at, completed_at) VALUES ($1, $2, $3, $4)`
	ctx, cancel := context.WithTimeout(d.configs.Ctx, 5*time.Second)
	defer cancel()
	_, err := d.configs.DB.Client.Query(ctx, insertSqlStatement, message.Message, MessageType(message.Type), message.CreatedAt, message.CompletedAt)
	if err != nil {
		log.Fatalf("Failed to update the log table: %v", err)
	}

	notifyMessage = &pb.NotifyBFFMessage{
		Message:     message.Message,
		Type:        pb.MessageType(message.Type),
		CreatedAt:   timestamppb.New(message.CreatedAt),
		CompletedAt: timestamppb.New(message.CompletedAt),
	}

}

func (d *DataIngestionSvc) NotifyBFF(_ *pb.NotifyBFFRequest, stream pb.DataIngestion_NotifyBFFServer) error {

	response := &pb.NotifyBFFResponse{
		Message:     notifyMessage.Message,
		Type:        notifyMessage.Type,
		CreatedAt:   notifyMessage.CreatedAt,
		CompletedAt: notifyMessage.CompletedAt,
	}
	notifyMessage = &pb.NotifyBFFMessage{}
	if err := stream.Send(response); err != nil {
		return err
	}
	return nil
}
