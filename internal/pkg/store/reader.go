package store

import (
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	spannerModel "github.com/voi-oss/jaeger-spanner/internal/pkg/store/model"
)

var _ spanstore.Reader = (*Reader)(nil)
var _ io.Closer = (*Reader)(nil)

const (
	defaultNumTraces = 100

	// limitMultiple exists because many spans that are returned from indices can have the same trace, limitMultiple increases
	// the number of responses from the index, so we can respect the user's limit value they provided.
	limitMultiple = 3
)

type Reader struct {
	client *spanner.Client
}

func NewReader(projectID, instance string) (*Reader, error) {
	client, err := spanner.NewClient(context.Background(), fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to create read client: %w", err)
	}

	return &Reader{
		client: client,
	}, nil
}

func (r *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer span.Finish()

	results, err := r.fetchTracesByIDs(ctx, []string{traceID.String()})
	if err != nil {
		return nil, fmt.Errorf("could not find trace by ID: %s", err)
	}

	if len(results) != 1 {
		return nil, spanstore.ErrTraceNotFound
	}

	return results[0], nil
}

func (r *Reader) GetServices(ctx context.Context) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer span.Finish()

	var names []string
	iter := r.client.Single().Read(ctx, serviceNamesTable, spanner.AllKeys(), []string{"ServiceName"})
	err := iter.Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return err
		}

		names = append(names, name)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return names, nil
}

func (r *Reader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	// Get operations for given service
	stmt := spanner.NewStatement("SELECT SpanKind, OperationName FROM operation_names WHERE ServiceName = @service")

	if query.SpanKind != "" {
		// Get operations for given spanKind
		stmt = spanner.NewStatement("SELECT SpanKind, OperationName FROM operation_names WHERE SpanKind = @spanKind")
		stmt.Params["spanKind"] = query.SpanKind
	}

	stmt.Params["service"] = query.ServiceName

	var operations []spanstore.Operation
	iter := r.client.Single().Query(ctx, stmt)
	err := iter.Do(func(row *spanner.Row) error {
		var op spanstore.Operation
		if err := row.Columns(&op.SpanKind, &op.Name); err != nil {
			return err
		}

		operations = append(operations, op)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return operations, nil
}

func (r *Reader) FindTraces(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	if err := validateQuery(traceQuery); err != nil {
		return nil, err
	}

	uniqueTraceIDs, err := r.findTraceIDs(ctx, traceQuery)
	if err != nil {
		return nil, err
	}

	traceIDs := make([]string, 0, len(uniqueTraceIDs))
	for t := range uniqueTraceIDs {
		if len(traceIDs) >= traceQuery.NumTraces {
			break
		}

		traceIDs = append(traceIDs, t)
	}

	return r.fetchTracesByIDs(ctx, traceIDs)
}

func (r *Reader) FindTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	if err := validateQuery(traceQuery); err != nil {
		return nil, err
	}

	dbTraceIDs, err := r.findTraceIDs(ctx, traceQuery)
	if err != nil {
		return nil, err
	}

	traceIDs := make([]model.TraceID, 0, traceQuery.NumTraces)
	for t := range dbTraceIDs {
		if len(traceIDs) >= traceQuery.NumTraces {
			break
		}

		traceIDs = append(traceIDs, spannerModel.TraceIDToDomain(t))
	}

	return traceIDs, nil
}

func (r *Reader) Close() error {
	r.client.Close()

	return nil
}

func (r *Reader) findTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (spannerModel.UniqueTraceIDs, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "findTraceIDs")
	defer span.Finish()

	if traceQuery.DurationMin != 0 || traceQuery.DurationMax != 0 {
		return r.queryByDuration(ctx, traceQuery)
	}

	if traceQuery.OperationName != "" {
		traceIds, err := r.queryByServiceNameAndOperation(ctx, traceQuery)
		if err != nil {
			return nil, err
		}

		if len(traceQuery.Tags) > 0 {
			tagTraceIds, err := r.queryByTagsAndLogs(ctx, traceQuery)
			if err != nil {
				return nil, err
			}

			return spannerModel.IntersectTraceIDs([]spannerModel.UniqueTraceIDs{
				traceIds,
				tagTraceIds,
			}), nil
		}
		return traceIds, nil
	}

	if len(traceQuery.Tags) > 0 {
		return r.queryByTagsAndLogs(ctx, traceQuery)
	}

	return r.queryByService(ctx, traceQuery)
}

func (r *Reader) queryByTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) (spannerModel.UniqueTraceIDs, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "queryByTagsAndLogs")
	defer span.Finish()

	results := make([]spannerModel.UniqueTraceIDs, 0, len(tq.Tags))
	for tagKey, tagValue := range tq.Tags {
		stmt := spanner.NewStatement("SELECT TraceId FROM tag_index WHERE ServiceName = @service AND TagKey = @tagKey AND TagValue = @tagValue AND StartTime BETWEEN @start AND @end ORDER BY StartTime DESC LIMIT @limit")
		stmt.Params["service"] = tq.ServiceName
		stmt.Params["tagKey"] = tagKey
		stmt.Params["tagValue"] = tagValue
		stmt.Params["start"] = int64(model.TimeAsEpochMicroseconds(tq.StartTimeMin))
		stmt.Params["end"] = int64(model.TimeAsEpochMicroseconds(tq.StartTimeMax))
		stmt.Params["limit"] = tq.NumTraces * limitMultiple

		iter := r.client.Single().Query(ctx, stmt)

		tagResults, err := readUniqueTraceIDsFromIter(iter)
		if err != nil {
			return nil, err
		}

		results = append(results, tagResults)
	}

	return spannerModel.IntersectTraceIDs(results), nil
}

func (r *Reader) queryByDuration(ctx context.Context, tq *spanstore.TraceQueryParameters) (spannerModel.UniqueTraceIDs, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "queryByDuration")
	defer span.Finish()

	minDurationMicros := tq.DurationMin.Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	maxDurationMicros := (time.Hour * 24).Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	if tq.DurationMax != 0 {
		maxDurationMicros = tq.DurationMax.Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	}

	stmt := spanner.NewStatement("SELECT TraceId FROM traces WHERE ServiceName = @service AND OperationName = @operation AND Duration BETWEEN @start AND @end ORDER BY StartTime DESC LIMIT @limit")
	stmt.Params["service"] = tq.ServiceName
	stmt.Params["operation"] = tq.OperationName
	stmt.Params["start"] = minDurationMicros
	stmt.Params["end"] = maxDurationMicros
	stmt.Params["limit"] = tq.NumTraces * limitMultiple

	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	return readUniqueTraceIDsFromIter(iter)
}

func (r *Reader) queryByServiceNameAndOperation(ctx context.Context, tq *spanstore.TraceQueryParameters) (spannerModel.UniqueTraceIDs, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "queryByServiceNameAndOperation")
	defer span.Finish()

	stmt := spanner.NewStatement("SELECT TraceId FROM traces@{FORCE_INDEX=traces_by_service_operation_idx} WHERE ServiceName = @service AND OperationName = @operation AND StartTime BETWEEN @start AND @end ORDER BY StartTime DESC LIMIT @limit")
	stmt.Params["service"] = tq.ServiceName
	stmt.Params["operation"] = tq.OperationName
	stmt.Params["start"] = int64(model.TimeAsEpochMicroseconds(tq.StartTimeMin))
	stmt.Params["end"] = int64(model.TimeAsEpochMicroseconds(tq.StartTimeMax))
	stmt.Params["limit"] = tq.NumTraces * limitMultiple

	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	return readUniqueTraceIDsFromIter(iter)
}

func (r *Reader) queryByService(ctx context.Context, tq *spanstore.TraceQueryParameters) (spannerModel.UniqueTraceIDs, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "queryByService")
	defer span.Finish()

	stmt := spanner.NewStatement("SELECT TraceId FROM traces@{FORCE_INDEX=traces_by_service_operation_idx} WHERE ServiceName = @service AND StartTime BETWEEN @start AND @end LIMIT @limit")
	stmt.Params["service"] = tq.ServiceName
	stmt.Params["start"] = int64(model.TimeAsEpochMicroseconds(tq.StartTimeMin))
	stmt.Params["end"] = int64(model.TimeAsEpochMicroseconds(tq.StartTimeMax))
	stmt.Params["limit"] = tq.NumTraces * limitMultiple

	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	return readUniqueTraceIDsFromIter(iter)
}

func (r *Reader) fetchTracesByIDs(ctx context.Context, traceIDs []string) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "fetchTracesByIDs")
	defer span.Finish()

	tracesMap := make(map[string]*model.Trace, len(traceIDs))

	stmt := spanner.NewStatement("SELECT TraceId, SpanId, ServiceName, OperationName, Flags, StartTime, Duration, Process, Tags FROM traces WHERE TraceId IN UNNEST(@traces)")
	stmt.Params["traces"] = traceIDs

	iter := r.client.Single().Query(ctx, stmt)
	err := iter.Do(func(row *spanner.Row) error {
		dbSpan := spannerModel.Span{}
		if err := row.ToStruct(&dbSpan); err != nil {
			return fmt.Errorf("could not map row to struct: %w", err)
		}

		span, err := spannerModel.ToDomain(&dbSpan)
		if err != nil {
			return err
		}

		if _, ok := tracesMap[dbSpan.TraceID]; !ok {
			tracesMap[dbSpan.TraceID] = &model.Trace{}
		}

		tracesMap[dbSpan.TraceID].Spans = append(tracesMap[dbSpan.TraceID].Spans, span)

		return nil
	})
	if err != nil {
		return nil, err
	}

	traces := make([]*model.Trace, 0, len(tracesMap))
	for _, trace := range tracesMap {
		traces = append(traces, trace)
	}

	return traces, nil
}

func readUniqueTraceIDsFromIter(iter *spanner.RowIterator) (spannerModel.UniqueTraceIDs, error) {
	traces := spannerModel.UniqueTraceIDs{}
	err := iter.Do(func(row *spanner.Row) error {
		var trace string
		if err := row.Columns(&trace); err != nil {
			return err
		}

		traces.Add(trace)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return traces, nil
}

// Stolen from https://github.com/jaegertracing/jaeger/blob/779027c14b9623cdc1f382a90c82f1f5425a3f56/plugin/storage/cassandra/spanstore/reader.go#L216
func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}
	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}
	if !p.StartTimeMin.IsZero() && !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	if (p.DurationMin != 0 || p.DurationMax != 0) && len(p.Tags) > 0 {
		return ErrDurationAndTagQueryNotSupported
	}

	if p.NumTraces == 0 {
		p.NumTraces = defaultNumTraces
	}

	return nil
}
