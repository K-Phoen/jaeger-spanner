package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	spannerModel "github.com/voi-oss/jaeger-spanner/internal/pkg/store/model"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ spanstore.Writer = (*Writer)(nil)
var _ io.Closer = (*Writer)(nil)

const (
	dbName = "spanner_jaeger_v1"

	tracesTable         = "traces"
	serviceNamesTable   = "service_names"
	operationNamesTable = "operation_names"

	tagIndexTable                    = "tag_index"
	tracesByServiceAndOperationIndex = "traces_by_service_operation_idx"

	maximumTagKeyOrValueSize = 256
)

type Writer struct {
	logger hclog.Logger
	client *spanner.Client
}

func NewWriter(logger hclog.Logger, projectID, instance string) (*Writer, error) {
	writer := &Writer{
		logger: logger,
	}

	ctx := context.Background()

	if err := writer.createDatabase(ctx, projectID, instance); err != nil {
		logger.Error("failed to create database", "error", err)
		return nil, err
	}

	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to create write client: %w", err)
	}

	writer.client = client

	return writer, nil
}

func (w *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	spanModel, err := spannerModel.FromDomain(span)
	if err != nil {
		return err
	}

	if err := w.writeSpan(ctx, spanModel); err != nil {
		return err
	}

	return w.writeIndexes(ctx, span, spanModel)
}

func (w *Writer) writeSpan(ctx context.Context, spanModel *spannerModel.Span) error {
	tracesColumns := []string{"TraceId", "SpanId", "ServiceName", "OperationName", "Flags", "StartTime", "Duration", "Process", "Tags"}
	m := []*spanner.Mutation{
		spanner.Insert(tracesTable, tracesColumns, []interface{}{
			spanModel.TraceID,
			spanModel.SpanID,
			spanModel.ServiceName,
			spanModel.OperationName,
			spanModel.Flags,
			spanModel.StartTime,
			spanModel.Duration,
			spanModel.Process,
			spanModel.Tags,
		}),
	}
	_, err := w.client.Apply(ctx, m)

	return err
}

func (w *Writer) writeIndexes(ctx context.Context, span *model.Span, spanModel *spannerModel.Span) error {
	spanKind, _ := span.GetSpanKind()

	m := []*spanner.Mutation{
		spanner.InsertOrUpdate(serviceNamesTable, []string{"ServiceName"}, []interface{}{spanModel.ServiceName}),
		spanner.InsertOrUpdate(operationNamesTable, []string{
			"ServiceName",
			"SpanKind",
			"OperationName",
		}, []interface{}{
			spanModel.ServiceName,
			spanKind,
			spanModel.OperationName,
		}),
	}
	_, err := w.client.Apply(ctx, m)
	if err != nil {
		return fmt.Errorf("could not write service name: %w", err)
	}

	if span.Flags.IsFirehoseEnabled() {
		return nil // skipping expensive indexing
	}

	if err := w.indexByTags(ctx, span, spanModel); err != nil {
		return fmt.Errorf("could not write tag index: %w", err)
	}

	return nil
}

func (w *Writer) indexByTags(ctx context.Context, span *model.Span, spanModel *spannerModel.Span) error {
	uniqueTags := spannerModel.GetAllUniqueTags(span)
	mutations := make([]*spanner.Mutation, 0, len(uniqueTags))

	for _, v := range uniqueTags {
		// we should introduce retries or just ignore failures imo, retrying each individual tag insertion might be better
		// we should consider bucketing.
		if !w.shouldIndexTag(v) {
			continue
		}

		mutations = append(mutations, spanner.Insert(tagIndexTable, []string{
			"ServiceName",
			"TagKey",
			"TagValue",
			"StartTime",
			"TraceId",
			"SpanId",
		}, []interface{}{
			spanModel.ServiceName,
			v.TagKey,
			v.TagValue,
			spanModel.StartTime,
			spanModel.TraceID,
			spanModel.SpanID,
		}))
	}

	_, err := w.client.Apply(ctx, mutations)
	if err != nil {
		return fmt.Errorf("could not index by tags: %w", err)
	}

	return nil
}

// shouldIndexTag checks to see if the tag is json or not, if it's UTF8 valid and it's not too large
func (w *Writer) shouldIndexTag(tag spannerModel.TagInsertion) bool {
	isJSON := func(s string) bool {
		var js json.RawMessage
		// poor man's string-is-a-json check short-circuits full unmarshalling
		return strings.HasPrefix(s, "{") && json.Unmarshal([]byte(s), &js) == nil
	}

	return len(tag.TagKey) < maximumTagKeyOrValueSize &&
		len(tag.TagValue) < maximumTagKeyOrValueSize &&
		utf8.ValidString(tag.TagValue) &&
		utf8.ValidString(tag.TagKey) &&
		!isJSON(tag.TagValue)
}

func (w *Writer) Close() error {
	w.client.Close()

	return nil
}

func (w *Writer) createDatabase(ctx context.Context, projectID string, instance string) error {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			w.logger.Warn(fmt.Sprintf("could not close admin spanner client: %s", err))
		}
	}()

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instance),
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
		ExtraStatements: []string{
			// Tables
			fmt.Sprintf(
				`CREATE TABLE %s (
					TraceId STRING(MAX) NOT NULL,
					SpanId INT64 NOT NULL,

					ServiceName STRING(MAX),
					OperationName STRING(MAX),
					
					Flags INT64,
					
					StartTime INT64,
					Duration INT64,

					Process BYTES(MAX),
					Tags BYTES(MAX),
				) PRIMARY KEY (TraceId, SpanId)`,
				tracesTable,
			),

			fmt.Sprintf(
				`CREATE TABLE %s (
					ServiceName STRING(MAX) NOT NULL,
				) PRIMARY KEY (ServiceName)`,
				serviceNamesTable,
			),

			fmt.Sprintf(
				`CREATE TABLE %s (
					ServiceName STRING(MAX) NOT NULL,
					SpanKind STRING(MAX) NOT NULL,
					OperationName STRING(MAX) NOT NULL,
				) PRIMARY KEY (ServiceName, SpanKind, OperationName)`,
				operationNamesTable,
			),

			// Index tables
			fmt.Sprintf(
				`CREATE TABLE %s (
					ServiceName STRING(MAX) NOT NULL,
					TagKey STRING(MAX) NOT NULL,
					TagValue STRING(MAX) NOT NULL,
					StartTime INT64 NOT NULL,
					TraceId STRING(MAX) NOT NULL,
					SpanId INT64 NOT NULL,
				) PRIMARY KEY (ServiceName, TagKey, TagValue, StartTime DESC, TraceId)`,
				tagIndexTable,
			),

			// Secondary indexes
			fmt.Sprintf(
				`CREATE INDEX %s ON %s(ServiceName, OperationName, StartTime, TraceId)`,
				tracesByServiceAndOperationIndex, tracesTable,
			),
		},
	})
	if err != nil {
		errStatus, ok := status.FromError(err)
		if ok && errStatus.Code() == codes.AlreadyExists {
			return nil
		}

		return fmt.Errorf("failed to create database: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}
