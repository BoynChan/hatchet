package sqlcv1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWP3PartitionSQLQueries(t *testing.T) {
	// Core: only 4 active partitioned tables
	assert.Contains(t, createPartitions, "create_v1_range_partition('v1_task'")
	assert.Contains(t, createPartitions, "create_v1_range_partition('v1_task_event'")
	assert.Contains(t, createPartitions, "create_v1_range_partition('v1_payload'")
	assert.Contains(t, createPartitions, "create_v1_monthly_range_partition('v1_lookup_table'")
	for _, unusedCore := range []string{
		"'v1_dag'",
		"'v1_dag_to_task'",
		"'v1_dag_data'",
		"'v1_durable_event_log_file'",
		"'v1_durable_event_log_entry'",
		"'v1_durable_event_log_branch_point'",
		"'v1_event'",
		"'v1_log_line'",
		"'v1_task_expression_eval'",
		"'v1_stream_producer_cursor'",
		"'v1_serverless_endpoint_event'",
		"'v1_sandbox_event'",
	} {
		assert.NotContains(t, createPartitions, unusedCore)
	}

	// EnsureTablePartitionsExist: only the 3 active daily core tables
	assert.Contains(t, ensureTablePartitionsExist, "v1_task_")
	assert.Contains(t, ensureTablePartitionsExist, "v1_task_event_")
	assert.Contains(t, ensureTablePartitionsExist, "v1_payload_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_dag_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_log_line_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_event_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_durable_event_log_")

	// OLAP: only 10 active partitioned tables (4 hash + 6 range/monthly)
	for _, activeOLAP := range []string{
		"create_v1_hash_partitions('v1_task_events_olap_tmp'",
		"create_v1_hash_partitions('v1_task_status_updates_tmp'",
		"create_v1_hash_partitions('v1_agg_task_statuses_minute_delta_olap'",
		"create_v1_hash_partitions('v1_agg_task_events_minute_delta_olap'",
		"create_v1_range_partition('v1_tasks_olap'",
		"create_v1_range_partition('v1_runs_olap'",
		"create_v1_range_partition('v1_agg_task_statuses_minute_olap'",
		"create_v1_range_partition('v1_agg_task_events_minute_olap'",
		"create_v1_range_partition('v1_task_events_olap'",
		"create_v1_monthly_range_partition('v1_lookup_table_olap'",
	} {
		assert.Contains(t, createOLAPPartitions, activeOLAP)
	}
	for _, unusedOLAP := range []string{
		"'v1_agg_events_minute_delta_olap'",
		"'v1_dags_olap'",
		"'v1_payloads_olap'",
		"'v1_agg_events_minute_olap'",
		"'v1_agg_stream_bytes_minute_olap'",
		"'v1_events_olap'",
		"'v1_event_to_run_olap'",
		"'v1_event_lookup_table_olap'",
		"'v1_incoming_webhook_validation_failures_olap'",
		"'v1_cel_evaluation_failures_olap'",
		"'v1_dag_to_task_olap'",
		"'v1_statuses_olap'",
		"'v1_otel_trace_olap'",
		"'v1_otel_trace_lookup_olap'",
		"'v1_audit_log'",
	} {
		assert.NotContains(t, createOLAPPartitions, unusedOLAP)
	}

	// ListOLAPPartitionsBeforeDate still includes legacy tables so previously created partitions are cleaned up.
	assert.Contains(t, listOLAPPartitionsBeforeDate, "'v1_events_olap:daily'")
	assert.Contains(t, listOLAPPartitionsBeforeDate, "'v1_statuses_olap:monthly'")
	assert.Contains(t, listOLAPPartitionsBeforeDate, "'v1_task_events_olap:daily'")
	assert.Contains(t, listOLAPPartitionsBeforeDate, "'v1_lookup_table_olap:monthly'")
}
