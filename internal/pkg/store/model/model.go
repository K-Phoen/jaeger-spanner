package model

import (
	"bytes"
)

const (
	childOf     = "child-of"
	followsFrom = "follows-from"

	stringType  = "string"
	boolType    = "bool"
	int64Type   = "int64"
	float64Type = "float64"
	binaryType  = "binary"
)

// Span is the database representation of a span.
type Span struct {
	TraceID       string    `spanner:"TraceId"`
	SpanID        int64     `spanner:"SpanId"`
	OperationName string    `spanner:"OperationName"`
	Flags         int64     `spanner:"Flags"`
	StartTime     int64     `spanner:"StartTime"` // microseconds since epoch
	Duration      int64     `spanner:"Duration"`  // microseconds
	Tags          []byte    `spanner:"Tags"`
	Logs          []Log     `spanner:"-"` // TODO
	Refs          []SpanRef `spanner:"-"` // TODO
	Process       []byte    `spanner:"Process"`
	ServiceName   string    `spanner:"ServiceName"`
}

// Operation defines schema for records saved in operation_names_v2 table
type Operation struct {
	ServiceName   string
	SpanKind      string
	OperationName string
}

// KeyValue is the UDT representation of a Jaeger KeyValue.
type KeyValue struct {
	Key          string
	ValueType    string
	ValueString  string
	ValueBool    bool
	ValueInt64   int64
	ValueFloat64 float64
	ValueBinary  []byte
}

// Log is the UDT representation of a Jaeger Log.
type Log struct {
	Timestamp int64
	Fields    []KeyValue
}

// SpanRef is the UDT representation of a Jaeger Span Reference.
type SpanRef struct {
	RefType string
	TraceID string
	SpanID  int64
}

// Process is the UDT representation of a Jaeger Process.
type Process struct {
	ServiceName string
	Tags        []KeyValue
}

// TagInsertion contains the items necessary to insert a tag for a given span
type TagInsertion struct {
	ServiceName string
	TagKey      string
	TagValue    string
}

func (t TagInsertion) String() string {
	const uniqueTagDelimiter = ":"
	var buffer bytes.Buffer
	buffer.WriteString(t.ServiceName)
	buffer.WriteString(uniqueTagDelimiter)
	buffer.WriteString(t.TagKey)
	buffer.WriteString(uniqueTagDelimiter)
	buffer.WriteString(t.TagValue)
	return buffer.String()
}
