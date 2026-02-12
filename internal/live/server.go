package live

import (
	"log/slog"

	"google.golang.org/grpc"

	pb "jupitor/internal/api/pb"
	"jupitor/internal/store"
)

// Server implements the StreamLiveTrades gRPC endpoint.
type Server struct {
	pb.UnimplementedMarketDataServer
	model *LiveModel
	log   *slog.Logger
}

// NewServer creates a gRPC server backed by the given LiveModel.
func NewServer(model *LiveModel, log *slog.Logger) *Server {
	return &Server{model: model, log: log}
}

// RegisterGRPC registers the server on the given gRPC server instance.
func (s *Server) RegisterGRPC(gs *grpc.Server) {
	pb.RegisterMarketDataServer(gs, s)
}

// StreamLiveTrades sends a snapshot of all current trades, then streams
// new trade events as they arrive. The stream ends when the client disconnects.
func (s *Server) StreamLiveTrades(req *pb.StreamLiveTradesRequest, stream grpc.ServerStreamingServer[pb.LiveTrade]) error {
	exOnly := req.GetExIndexOnly()

	// Send snapshot first.
	todayIdx, todayExIdx := s.model.TodaySnapshot()
	nextIdx, nextExIdx := s.model.NextSnapshot()

	sendSlice := func(records []store.TradeRecord, isIndex, isToday bool) error {
		if exOnly && isIndex {
			return nil
		}
		for i := range records {
			if err := stream.Send(recordToProto(&records[i], isIndex, isToday)); err != nil {
				return err
			}
		}
		return nil
	}

	if err := sendSlice(todayIdx, true, true); err != nil {
		return err
	}
	if err := sendSlice(todayExIdx, false, true); err != nil {
		return err
	}
	if err := sendSlice(nextIdx, true, false); err != nil {
		return err
	}
	if err := sendSlice(nextExIdx, false, false); err != nil {
		return err
	}

	// Subscribe for live updates.
	subID, ch := s.model.Subscribe(4096)
	defer s.model.Unsubscribe(subID)

	s.log.Info("grpc client subscribed", "subID", subID, "exOnly", exOnly)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("grpc client disconnected", "subID", subID)
			return nil
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			if exOnly && evt.IsIndex {
				continue
			}
			if err := stream.Send(recordToProto(&evt.Record, evt.IsIndex, evt.IsToday)); err != nil {
				return err
			}
		}
	}
}

func recordToProto(r *store.TradeRecord, isIndex, isToday bool) *pb.LiveTrade {
	return &pb.LiveTrade{
		Symbol:     r.Symbol,
		Timestamp:  r.Timestamp,
		Price:      r.Price,
		Size:       r.Size,
		Exchange:   r.Exchange,
		Id:         r.ID,
		Conditions: r.Conditions,
		IsIndex:    isIndex,
		IsToday:    isToday,
	}
}
