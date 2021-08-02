package model

import (
	"github.com/jaegertracing/jaeger/model"
)

// GetAllUniqueTags creates a list of all unique tags from a set of tags.
func GetAllUniqueTags(span *model.Span) []TagInsertion {
	allTags := append(model.KeyValues{}, span.Process.Tags...)
	allTags = append(allTags, span.Tags...)
	for _, log := range span.Logs {
		allTags = append(allTags, log.Fields...)
	}
	allTags.Sort()
	uniqueTags := make([]TagInsertion, 0, len(allTags))
	for i := range allTags {
		if allTags[i].VType == model.BinaryType {
			continue // do not index binary tags
		}
		if i > 0 && allTags[i-1].Equal(&allTags[i]) {
			continue // skip identical tags
		}
		uniqueTags = append(uniqueTags, TagInsertion{
			ServiceName: span.Process.ServiceName,
			TagKey:      allTags[i].Key,
			TagValue:    allTags[i].AsString(),
		})
	}
	return uniqueTags
}
