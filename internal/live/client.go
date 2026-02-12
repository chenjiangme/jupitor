package live

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "jupitor/internal/api/pb"
	"jupitor/internal/store"
)

// Client connects to a live trade gRPC server and populates a local LiveModel,
// providing an automatic mirror of the server-side model.
type Client struct {
	addr  string
	model *LiveModel
	log   *slog.Logger
}

// NewClient creates a client targeting the given gRPC address.
func NewClient(addr string, model *LiveModel, log *slog.Logger) *Client {
	return &Client{addr: addr, model: model, log: log}
}

// Sync connects to the gRPC server and streams live trades into the local
// model. It blocks until ctx is cancelled or the stream ends.
func (c *Client) Sync(ctx context.Context) error {
	conn, err := grpc.NewClient(c.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", c.addr, err)
	}
	defer conn.Close()

	client := pb.NewMarketDataClient(conn)
	stream, err := client.StreamLiveTrades(ctx, &pb.StreamLiveTradesRequest{})
	if err != nil {
		return fmt.Errorf("starting stream: %w", err)
	}

	c.log.Info("connected to live trade stream", "addr", c.addr)

	for {
		lt, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receiving trade: %w", err)
		}

		record := store.TradeRecord{
			Symbol:     lt.Symbol,
			Timestamp:  lt.Timestamp,
			Price:      lt.Price,
			Size:       lt.Size,
			Exchange:   lt.Exchange,
			ID:         lt.Id,
			Conditions: lt.Conditions,
		}

		rawID, _ := strconv.ParseInt(lt.Id, 10, 64)
		c.model.Add(record, rawID, lt.IsIndex)
	}
}
