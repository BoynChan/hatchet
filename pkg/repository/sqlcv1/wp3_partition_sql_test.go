package sqlcv1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWP3PartitionSQLQueries(t *testing.T) {
	assert.Contains(t, createPartitions, "create_v1_range_partition('v1_task'")
	assert.Contains(t, createPartitions, "create_v1_range_partition('v1_task_event'")
	assert.Contains(t, createPartitions, "create_v1_range_partition('v1_payload'")
	assert.Contains(t, createPartitions, "create_v1_monthly_range_partition('v1_lookup_table'")
	assert.NotContains(t, createPartitions, "'v1_dag'")
	assert.NotContains(t, createPartitions, "'v1_dag_to_task'")
	assert.NotContains(t, createPartitions, "'v1_dag_data'")
	assert.NotContains(t, createPartitions, "'v1_durable_event_log_file'")
	assert.NotContains(t, createPartitions, "'v1_durable_event_log_entry'")
	assert.NotContains(t, createPartitions, "'v1_durable_event_log_branch_point'")
	assert.NotContains(t, createPartitions, "'v1_event'")
	assert.NotContains(t, createPartitions, "'v1_log_line'")
	assert.NotContains(t, createPartitions, "'v1_task_expression_eval'")
	assert.NotContains(t, createPartitions, "'v1_stream_producer_cursor'")

	assert.Contains(t, ensureTablePartitionsExist, "v1_task_")
	assert.Contains(t, ensureTablePartitionsExist, "v1_task_event_")
	assert.Contains(t, ensureTablePartitionsExist, "v1_payload_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_dag_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_log_line_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_event_")
	assert.NotContains(t, ensureTablePartitionsExist, "v1_durable_event_log_")

	assert.Contains(t, createOLAPPartitions, "create_v1_range_partition('v1_tasks_olap'")
	assert.Contains(t, createOLAPPartitions, "create_v1_range_partition('v1_runs_olap'")
	assert.Contains(t, createOLAPPartitions, "create_v1_range_partition('v1_payloads_olap'")
	assert.Contains(t, createOLAPPartitions, "create_v1_range_partition('v1_task_events_olap'")
	assert.Contains(t, createOLAPPartitions, "create_v1_monthly_range_partition('v1_lookup_table_olap'")
	assert.NotContains(t, createOLAPPartitions, "'v1_dags_olap'")

	// Even when shouldManageOptionalTablePartitions ($1) is false, v1_task_events_olap and v1_lookup_table_olap
	// must NOT be in the excluded parent_table list so their expired partitions continue to be cleaned up.
	assert.Contains(t, listOLAPPartitionsBeforeDate, "'v1_events_olap'")
	assert.Contains(t, listOLAPPartitionsBeforeDate, "'v1_statuses_olap'")
	assert.NotContains(t, listOLAPPartitionsBeforeDate, "            'v1_task_events_olap'")
	assert.NotContains(t, listOLAPPartitionsBeforeDate, "            'v1_lookup_table_olap'")
}
