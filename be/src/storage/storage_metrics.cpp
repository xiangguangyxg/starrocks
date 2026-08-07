// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/storage_metrics.h"

#include <functional>

#include "base/time/time.h"
#include "common/config_lake_fwd.h"
#include "common/thread/threadpool.h"
#include "gutil/macros.h"

namespace starrocks {

StorageMetrics* StorageMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new StorageMetrics();
    return instance;
}

void StorageMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

#define REGISTER_STORAGE_METRIC(name) registry->register_metric(#name, &name)
#define REGISTER_ENGINE_REQUEST_METRIC(type, status, metric)                                                     \
    registry->register_metric("engine_requests_total", MetricLabels().add("type", #type).add("status", #status), \
                              &metric)

    registry->register_metric("push_requests_total", MetricLabels().add("status", "SUCCESS"),
                              &push_requests_success_total);
    registry->register_metric("push_requests_total", MetricLabels().add("status", "FAIL"), &push_requests_fail_total);
    REGISTER_STORAGE_METRIC(push_request_duration_us);
    REGISTER_STORAGE_METRIC(push_request_write_bytes);
    REGISTER_STORAGE_METRIC(push_request_write_rows);

    registry->register_metric("meta_request_total", MetricLabels().add("type", "write"), &meta_write_request_total);
    registry->register_metric("meta_request_total", MetricLabels().add("type", "read"), &meta_read_request_total);
    registry->register_metric("meta_request_duration", MetricLabels().add("type", "write"),
                              &meta_write_request_duration_us);
    registry->register_metric("meta_request_duration", MetricLabels().add("type", "read"),
                              &meta_read_request_duration_us);

    registry->register_metric("segment_read", MetricLabels().add("type", "segment_total_read_times"),
                              &segment_read_total);
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_total_row_num"), &segment_row_total);
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_rows_by_short_key"),
                              &segment_rows_by_short_key);
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_rows_read_by_zone_map"),
                              &segment_rows_read_by_zone_map);

    REGISTER_STORAGE_METRIC(txn_persist_total);
    REGISTER_STORAGE_METRIC(txn_persist_duration_us);
    REGISTER_STORAGE_METRIC(segment_file_not_found_total);

    REGISTER_STORAGE_METRIC(update_primary_index_num);
    REGISTER_STORAGE_METRIC(update_primary_index_bytes_total);
    REGISTER_STORAGE_METRIC(update_del_vector_num);
    REGISTER_STORAGE_METRIC(update_del_vector_dels_num);
    REGISTER_STORAGE_METRIC(update_del_vector_bytes_total);
    REGISTER_STORAGE_METRIC(update_del_vector_deletes_total);
    REGISTER_STORAGE_METRIC(update_del_vector_deletes_new);
    REGISTER_STORAGE_METRIC(column_partial_update_apply_total);
    REGISTER_STORAGE_METRIC(column_partial_update_apply_duration_us);
    REGISTER_STORAGE_METRIC(delta_column_group_get_total);
    REGISTER_STORAGE_METRIC(delta_column_group_get_hit_cache);
    REGISTER_STORAGE_METRIC(delta_column_group_get_non_pk_total);
    REGISTER_STORAGE_METRIC(delta_column_group_get_non_pk_hit_cache);
    REGISTER_STORAGE_METRIC(primary_key_table_error_state_total);
    REGISTER_STORAGE_METRIC(primary_key_wait_apply_done_duration_ms);
    REGISTER_STORAGE_METRIC(primary_key_wait_apply_done_total);
    REGISTER_STORAGE_METRIC(pk_index_sst_read_error_total);
    REGISTER_STORAGE_METRIC(pk_index_sst_write_error_total);

    registry->register_metric("invalidating_cache_drops_total", MetricLabels().add("site", "page_io"),
                              &invalidating_cache_drops_page_io);
    registry->register_metric("invalidating_cache_drops_total", MetricLabels().add("site", "sstable"),
                              &invalidating_cache_drops_sstable);
    registry->register_metric("invalidating_cache_drops_total", MetricLabels().add("site", "segment_footer"),
                              &invalidating_cache_drops_segment_footer);
    registry->register_metric("invalidating_cache_drops_total", MetricLabels().add("site", "gtid_mismatch"),
                              &invalidating_cache_drops_gtid_mismatch);
    registry->register_metric("invalidating_cache_drops_total", MetricLabels().add("site", "bundle_page"),
                              &invalidating_cache_drops_bundle_page);
    registry->register_metric("invalidating_cache_drops_total", MetricLabels().add("site", "other"),
                              &invalidating_cache_drops_other);
    REGISTER_STORAGE_METRIC(self_heal_reread_attempts_total);
    REGISTER_STORAGE_METRIC(self_heal_reread_success_total);
    REGISTER_STORAGE_METRIC(repeated_invalidation_in_window);

    REGISTER_STORAGE_METRIC(readable_blocks_total);
    REGISTER_STORAGE_METRIC(writable_blocks_total);
    REGISTER_STORAGE_METRIC(blocks_created_total);
    REGISTER_STORAGE_METRIC(blocks_deleted_total);
    REGISTER_STORAGE_METRIC(bytes_read_total);
    REGISTER_STORAGE_METRIC(bytes_written_total);
    REGISTER_STORAGE_METRIC(disk_sync_total);
    REGISTER_STORAGE_METRIC(blocks_open_reading);
    REGISTER_STORAGE_METRIC(blocks_open_writing);

    REGISTER_ENGINE_REQUEST_METRIC(storage_migrate, total, storage_migrate_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_tablet, total, create_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_tablet, failed, create_tablet_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(drop_tablet, total, drop_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_all_tablets, total, report_all_tablets_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(report_tablet, total, report_tablet_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_rollup, total, create_rollup_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(create_rollup, failed, create_rollup_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(delete, total, delete_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(delete, failed, delete_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(lake_add_index, total, lake_add_index_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(lake_add_index, failed, lake_add_index_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(lake_drop_index, total, lake_drop_index_requests_total);
    REGISTER_STORAGE_METRIC(lake_idg_files_written_total);
    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, total, base_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, failed, base_compaction_request_failed);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, total, cumulative_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, failed, cumulative_compaction_request_failed);
    REGISTER_ENGINE_REQUEST_METRIC(update_compaction, total, update_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(update_compaction, failed, update_compaction_request_failed);

    registry->register_metric("compaction_deltas_total", MetricLabels().add("type", "base"),
                              &base_compaction_deltas_total);
    registry->register_metric("compaction_deltas_total", MetricLabels().add("type", "cumulative"),
                              &cumulative_compaction_deltas_total);
    registry->register_metric("compaction_bytes_total", MetricLabels().add("type", "base"),
                              &base_compaction_bytes_total);
    registry->register_metric("compaction_bytes_total", MetricLabels().add("type", "cumulative"),
                              &cumulative_compaction_bytes_total);
    registry->register_metric("compaction_deltas_total", MetricLabels().add("type", "update"),
                              &update_compaction_deltas_total);
    registry->register_metric("compaction_bytes_total", MetricLabels().add("type", "update"),
                              &update_compaction_bytes_total);
    registry->register_metric("update_compaction_outputs_total", MetricLabels().add("type", "update"),
                              &update_compaction_outputs_total);
    registry->register_metric("update_compaction_outputs_bytes_total", MetricLabels().add("type", "update"),
                              &update_compaction_outputs_bytes_total);
    registry->register_metric("update_compaction_duration_us", MetricLabels().add("type", "update"),
                              &update_compaction_duration_us);

    REGISTER_STORAGE_METRIC(async_delta_writer_execute_total);
    REGISTER_STORAGE_METRIC(async_delta_writer_task_total);
    REGISTER_STORAGE_METRIC(async_delta_writer_task_execute_duration_us);
    REGISTER_STORAGE_METRIC(async_delta_writer_task_pending_duration_us);

    REGISTER_STORAGE_METRIC(delta_writer_commit_task_total);
    REGISTER_STORAGE_METRIC(delta_writer_wait_flush_task_total);
    REGISTER_STORAGE_METRIC(delta_writer_wait_flush_duration_us);
    REGISTER_STORAGE_METRIC(delta_writer_wait_replica_duration_us);
    REGISTER_STORAGE_METRIC(delta_writer_txn_commit_duration_us);

    REGISTER_STORAGE_METRIC(memtable_flush_total);
    REGISTER_STORAGE_METRIC(memtable_finalize_task_total);
    REGISTER_STORAGE_METRIC(memtable_finalize_duration_us);
    REGISTER_STORAGE_METRIC(memtable_flush_duration_us);
    REGISTER_STORAGE_METRIC(memtable_flush_io_time_us);
    REGISTER_STORAGE_METRIC(memtable_flush_memory_bytes_total);
    REGISTER_STORAGE_METRIC(memtable_flush_disk_bytes_total);
    REGISTER_STORAGE_METRIC(segment_flush_total);
    REGISTER_STORAGE_METRIC(segment_flush_duration_us);
    REGISTER_STORAGE_METRIC(segment_flush_io_time_us);
    REGISTER_STORAGE_METRIC(segment_flush_bytes_total);

    REGISTER_STORAGE_METRIC(update_rowset_commit_request_total);
    REGISTER_STORAGE_METRIC(update_rowset_commit_request_failed);
    REGISTER_STORAGE_METRIC(update_rowset_commit_apply_total);
    REGISTER_STORAGE_METRIC(update_rowset_commit_apply_duration_us);

    REGISTER_STORAGE_METRIC(tablet_cumulative_max_compaction_score);
    REGISTER_STORAGE_METRIC(tablet_base_max_compaction_score);
    REGISTER_STORAGE_METRIC(tablet_update_max_compaction_score);
    REGISTER_STORAGE_METRIC(max_tablet_rowset_num);
    REGISTER_STORAGE_METRIC(wait_cumulative_compaction_task_num);
    REGISTER_STORAGE_METRIC(wait_base_compaction_task_num);
    REGISTER_STORAGE_METRIC(running_cumulative_compaction_task_num);
    REGISTER_STORAGE_METRIC(running_base_compaction_task_num);
    REGISTER_STORAGE_METRIC(running_update_compaction_task_num);
    REGISTER_STORAGE_METRIC(cumulative_compaction_task_cost_time_ms);
    REGISTER_STORAGE_METRIC(base_compaction_task_cost_time_ms);
    REGISTER_STORAGE_METRIC(update_compaction_task_cost_time_ns);
    REGISTER_STORAGE_METRIC(base_compaction_task_byte_per_second);
    REGISTER_STORAGE_METRIC(cumulative_compaction_task_byte_per_second);
    REGISTER_STORAGE_METRIC(update_compaction_task_byte_per_second);
    REGISTER_STORAGE_METRIC(push_request_write_bytes_per_second);

    for (const auto& pending : _pending_thread_pool_metrics) {
        _register_thread_pool_metrics(pending.name, pending.metric_group, pending.threadpool);
    }
    _pending_thread_pool_metrics.clear();

    for (auto& pending : _pending_int_gauge_hooks) {
        _register_int_gauge_hook(pending.name, pending.metric, std::move(pending.value_fn));
    }
    _pending_int_gauge_hooks.clear();

    for (auto& pending : _pending_uint_gauge_hooks) {
        _register_uint_gauge_hook(pending.name, pending.metric, std::move(pending.value_fn));
    }
    _pending_uint_gauge_hooks.clear();

#undef REGISTER_ENGINE_REQUEST_METRIC
#undef REGISTER_STORAGE_METRIC
}

IntCounter& StorageMetrics::invalidating_cache_drops(CacheDropSite site) {
    switch (site) {
    case CacheDropSite::kPageIo:
        return invalidating_cache_drops_page_io;
    case CacheDropSite::kSstable:
        return invalidating_cache_drops_sstable;
    case CacheDropSite::kSegmentFooter:
        return invalidating_cache_drops_segment_footer;
    case CacheDropSite::kGtidMismatch:
        return invalidating_cache_drops_gtid_mismatch;
    case CacheDropSite::kBundlePage:
        return invalidating_cache_drops_bundle_page;
    case CacheDropSite::kOther:
        return invalidating_cache_drops_other;
    }
    // Every enumerator is returned above; this only catches a value cast in from outside the enum.
    return invalidating_cache_drops_other;
}

void StorageMetrics::record_invalidating_cache_drop(CacheDropSite site, std::string_view cache_key) {
    invalidating_cache_drops(site).increment(1);
    if (config::lake_track_repeated_cache_invalidation) {
        _observe_repeated_invalidation(cache_key);
    }
}

void StorageMetrics::_observe_repeated_invalidation(std::string_view cache_key) {
    // The key is hashed on the way in and only the hash is kept, so nothing here can be turned
    // back into a path and nothing here is exported as a label.
    const uint64_t key_hash = std::hash<std::string_view>{}(cache_key);
    const int64_t now_ms = MonotonicMillis();
    auto& slot = _invalidation_slots[key_hash % kInvalidationSlotCount];
    // Relaxed, unsynchronised load/store pairs: two threads landing on the same slot can interleave
    // and one of them then sees the other's write instead of its own predecessor. The cost is at
    // most one lost observation of a diagnostic counter that drives no decision, which is why no
    // lock or CAS is warranted on a read path.
    const uint64_t previous_hash = slot.key_hash.load(std::memory_order_relaxed);
    const int64_t previous_ms = slot.last_seen_ms.load(std::memory_order_relaxed);
    if (previous_ms != 0 && previous_hash == key_hash && now_ms - previous_ms <= kInvalidationWindowMs) {
        repeated_invalidation_in_window.increment(1);
    }
    slot.key_hash.store(key_hash, std::memory_order_relaxed);
    slot.last_seen_ms.store(now_ms, std::memory_order_relaxed);
}

void StorageMetrics::register_thread_pool_metrics(const std::string& name, ThreadPoolMetricGroup* metric_group,
                                                  ThreadPool* threadpool) {
    DCHECK(metric_group != nullptr);
    DCHECK(threadpool != nullptr);
    if (_registry == nullptr) {
        _pending_thread_pool_metrics.emplace_back(PendingThreadPoolMetrics{name, metric_group, threadpool});
        return;
    }
    _register_thread_pool_metrics(name, metric_group, threadpool);
}

void StorageMetrics::register_metadata_cache_bytes_total_hook(std::function<int64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_int_gauge_hooks.emplace_back(
                PendingIntGaugeHook{"metadata_cache_bytes_total", &metadata_cache_bytes_total, std::move(value_fn)});
        return;
    }
    _register_int_gauge_hook("metadata_cache_bytes_total", &metadata_cache_bytes_total, std::move(value_fn));
}

void StorageMetrics::register_unused_rowsets_count_hook(std::function<uint64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_uint_gauge_hooks.emplace_back(
                PendingUIntGaugeHook{"unused_rowsets_count", &unused_rowsets_count, std::move(value_fn)});
        return;
    }
    _register_uint_gauge_hook("unused_rowsets_count", &unused_rowsets_count, std::move(value_fn));
}

void StorageMetrics::register_rowset_count_generated_and_in_use_hook(std::function<uint64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_uint_gauge_hooks.emplace_back(PendingUIntGaugeHook{
                "rowset_count_generated_and_in_use", &rowset_count_generated_and_in_use, std::move(value_fn)});
        return;
    }
    _register_uint_gauge_hook("rowset_count_generated_and_in_use", &rowset_count_generated_and_in_use,
                              std::move(value_fn));
}

void StorageMetrics::_register_thread_pool_metrics(const std::string& name, ThreadPoolMetricGroup* metric_group,
                                                   ThreadPool* threadpool) {
    DCHECK(_registry != nullptr);
    DCHECK(metric_group != nullptr);
    DCHECK(threadpool != nullptr);

    metric_group->register_metrics(_registry, name, threadpool);
}

void StorageMetrics::_register_int_gauge_hook(const std::string& name, IntGauge* metric,
                                              std::function<int64_t()> value_fn) {
    DCHECK(_registry != nullptr);
    DCHECK(metric != nullptr);
    _registry->register_metric(name, metric);
    _registry->register_hook(name, [metric, value_fn = std::move(value_fn)] { metric->set_value(value_fn()); });
}

void StorageMetrics::_register_uint_gauge_hook(const std::string& name, UIntGauge* metric,
                                               std::function<uint64_t()> value_fn) {
    DCHECK(_registry != nullptr);
    DCHECK(metric != nullptr);
    _registry->register_metric(name, metric);
    _registry->register_hook(name, [metric, value_fn = std::move(value_fn)] { metric->set_value(value_fn()); });
}

} // namespace starrocks
