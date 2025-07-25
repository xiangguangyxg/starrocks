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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "runtime/descriptors.h"
#include "storage/tablet_schema.h"
#include "util/random.h"

namespace starrocks {

class MemPool;
class RuntimeState;

struct OlapTableColumnParam {
    std::vector<TabletColumn*> columns;
    std::vector<int32_t> sort_key_uid;
    int32_t short_key_column_count;

    void to_protobuf(POlapTableColumnParam* pcolumn) const;
};

struct OlapTableIndexSchema {
    int64_t index_id;
    std::vector<SlotDescriptor*> slots;
    int64_t schema_id;
    int32_t schema_hash;
    OlapTableColumnParam* column_param;
    ExprContext* where_clause = nullptr;
    std::map<std::string, std::string> column_to_expr_value;
    bool is_shadow = false;

    void to_protobuf(POlapTableIndexSchema* pindex) const;
};

class OlapTableSchemaParam {
public:
    OlapTableSchemaParam() = default;
    ~OlapTableSchemaParam() noexcept = default;

    Status init(const TOlapTableSchemaParam& tschema, RuntimeState* state = nullptr);
    Status init(const POlapTableSchemaParam& pschema);

    int64_t db_id() const { return _db_id; }
    int64_t table_id() const { return _table_id; }
    int64_t version() const { return _version; }

    TupleDescriptor* tuple_desc() const { return _tuple_desc; }
    const std::vector<OlapTableIndexSchema*>& indexes() const { return _indexes; }

    void to_protobuf(POlapTableSchemaParam* pschema) const;

    // NOTE: this function is not thread-safe.
    POlapTableSchemaParam* to_protobuf() const {
        if (_proto_schema == nullptr) {
            _proto_schema = _obj_pool.add(new POlapTableSchemaParam());
            to_protobuf(_proto_schema);
        }
        return _proto_schema;
    }

    int64_t shadow_index_size() const { return _shadow_indexes; }
    std::string debug_string() const;

private:
    int64_t _db_id = 0;
    int64_t _table_id = 0;
    int64_t _version = 0;

    TupleDescriptor* _tuple_desc = nullptr;
    mutable POlapTableSchemaParam* _proto_schema = nullptr;
    std::vector<OlapTableIndexSchema*> _indexes;
    mutable ObjectPool _obj_pool;

    int64_t _shadow_indexes = 0;
};

using OlapTableIndexTablets = TOlapTableIndexTablets;

using TabletLocation = TTabletLocation;

class OlapTableLocationParam {
public:
    explicit OlapTableLocationParam(const TOlapTableLocationParam& t_param) {
        for (auto& location : t_param.tablets) {
            _tablets.emplace(location.tablet_id, std::move(location));
        }
    }

    const TabletLocation* find_tablet(int64_t tablet_id) {
        auto it = _tablets.find(tablet_id);
        if (it != std::end(_tablets)) {
            return &it->second;
        }
        return nullptr;
    }

    void add_locations(std::vector<TTabletLocation>& locations) {
        for (auto& location : locations) {
            if (_tablets.try_emplace(location.tablet_id, std::move(location)).second) {
                VLOG(2) << "add location " << location;
            }
        }
    }

private:
    std::unordered_map<int64_t, TabletLocation> _tablets;
};

struct NodeInfo {
    int64_t id;
    int64_t option;
    std::string host;
    int32_t brpc_port;

    explicit NodeInfo(const TNodeInfo& tnode)
            : id(tnode.id), option(tnode.option), host(tnode.host), brpc_port(tnode.async_internal_port) {}
};

class StarRocksNodesInfo {
public:
    explicit StarRocksNodesInfo(const TNodesInfo& t_nodes) {
        for (auto& node : t_nodes.nodes) {
            _nodes.emplace(node.id, node);
        }
    }
    const NodeInfo* find_node(int64_t id) const {
        auto it = _nodes.find(id);
        if (it != std::end(_nodes)) {
            return &it->second;
        }
        return nullptr;
    }

    void add_nodes(const std::vector<TNodeInfo>& t_nodes) {
        for (const auto& node : t_nodes) {
            auto node_info = find_node(node.id);
            if (node_info == nullptr) {
                _nodes.emplace(node.id, node);
            }
        }
    }

private:
    std::unordered_map<int64_t, NodeInfo> _nodes;
};

struct ChunkRow {
    ChunkRow() = default;
    ChunkRow(const Columns* columns_, uint32_t index_) : columns(columns_), index(index_) {}

    std::string debug_string();

    const Columns* columns = nullptr;
    uint32_t index = 0;
};

struct OlapTablePartition {
    int64_t id = 0;
    ChunkRow start_key;
    ChunkRow end_key;
    std::vector<ChunkRow> in_keys;
    std::vector<OlapTableIndexTablets> indexes;
};

struct PartionKeyComparator {
    // return true if lhs < rhs
    // 'nullptr' is max value, but 'null' is min value
    bool operator()(const ChunkRow* lhs, const ChunkRow* rhs) const {
        if (lhs->columns == nullptr) {
            return false;
        } else if (rhs->columns == nullptr) {
            return true;
        }
        DCHECK_EQ(lhs->columns->size(), rhs->columns->size());

        for (size_t i = 0; i < lhs->columns->size(); ++i) {
            int cmp = _compare_at((*lhs->columns)[i], (*rhs->columns)[i], lhs->index, rhs->index);
            if (cmp != 0) {
                return cmp < 0;
            }
        }
        // equal, return false
        return false;
    }

private:
    /**
     * @brief Compare left column and right column at l_idx and r_idx which column can be nullable.
     * @param lc  left column
     * @param rc  right column
     * @param l_idx  left column index
     * @param r_idx  right column index
     * @return 0 if equal or left & right both null, -1 if left < right or left is null, 1 if left > right or right is null
     */
    int _compare_at(const ColumnPtr& lc, const ColumnPtr& rc, uint32_t l_idx, uint32_t r_idx) const {
        bool is_l_null = lc->is_null(l_idx);
        bool is_r_null = rc->is_null(r_idx);
        if (!is_l_null && !is_r_null) {
            const Column* ldc = ColumnHelper::get_data_column(lc.get());
            const Column* rdc = ColumnHelper::get_data_column(rc.get());
            return ldc->compare_at(l_idx, r_idx, *rdc, -1);
        } else {
            if (is_l_null && is_r_null) {
                return 0;
            } else if (is_l_null) {
                return -1;
            } else {
                return 1;
            }
        }
    }
};

// store an olap table's tablet information
class OlapTablePartitionParam {
public:
    OlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam> schema, const TOlapTablePartitionParam& param);
    ~OlapTablePartitionParam();

    Status init(RuntimeState* state);

    Status prepare(RuntimeState* state);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);

    int64_t db_id() const { return _t_param.db_id; }
    int64_t table_id() const { return _t_param.table_id; }
    int64_t version() const { return _t_param.version; }

    bool enable_automatic_partition() const { return _t_param.enable_automatic_partition; }

    // `invalid_row_index` stores index that chunk[index]
    // has been filtered out for not being able to find tablet.
    // it could be any row, becauset it's just for outputing error message for user to diagnose.
    Status find_tablets(Chunk* chunk, std::vector<OlapTablePartition*>* partitions, std::vector<uint32_t>* hashes,
                        std::vector<uint8_t>* selection, std::vector<int>* invalid_row_indexs, int64_t txn_id,
                        std::vector<std::vector<std::string>>* partition_not_exist_row_values);

    const std::map<int64_t, OlapTablePartition*>& get_partitions() const { return _partitions; }

    Status add_partitions(const std::vector<TOlapTablePartition>& partitions);

    Status remove_partitions(const std::vector<int64_t>& partition_ids);

    bool is_un_partitioned() const { return _partition_columns.empty(); }

    const TOlapTablePartitionParam& param() const { return _t_param; }

    Status test_add_partitions(OlapTablePartition* partition);

private:
    /**
     * @brief  find tablets with range partition table
     * @param chunk  input chunk
     * @param partition_columns input partition columns
     * @param hashes  input row hashes
     * @param partitions  output partitions
     * @param selection  chunk's selection
     * @param invalid_row_indexs output invalid row indexs
     * @param partition_not_exist_row_values  output partition not exist row values
     * @return Status 
     */
    Status _find_tablets_with_range_partition(Chunk* chunk, const Columns& partition_columns,
                                              const std::vector<uint32_t>& hashes,
                                              std::vector<OlapTablePartition*>* partitions,
                                              std::vector<uint8_t>* selection, std::vector<int>* invalid_row_indexs,
                                              std::vector<std::vector<std::string>>* partition_not_exist_row_values);

    /**
     * @brief  find tablets with list partition table
     * @param chunk  input chunk
     * @param partition_columns input partition columns
     * @param hashes  input row hashes
     * @param partitions  output partitions
     * @param selection  chunk's selection
     * @param invalid_row_indexs output invalid row indexs
     * @param partition_not_exist_row_values  output partition not exist row values
     * @return Status 
     */
    Status _find_tablets_with_list_partition(Chunk* chunk, const Columns& partition_columns,
                                             const std::vector<uint32_t>& hashes,
                                             std::vector<OlapTablePartition*>* partitions,
                                             std::vector<uint8_t>* selection, std::vector<int>* invalid_row_indexs,
                                             std::vector<std::vector<std::string>>* partition_not_exist_row_values);

    Status _create_partition_keys(const std::vector<TExprNode>& t_exprs, ChunkRow* part_key);

    void _compute_hashes(const Chunk* chunk, std::vector<uint32_t>* hashes);

    // check if this partition contain this key
    bool _part_contains(OlapTablePartition* part, ChunkRow* key) const {
        if (part->start_key.columns == nullptr) {
            // start_key is nullptr means the lower bound is boundless
            return true;
        }
        return !PartionKeyComparator()(key, &part->start_key);
    }

private:
    std::shared_ptr<OlapTableSchemaParam> _schema;
    TOlapTablePartitionParam _t_param;

    std::vector<SlotDescriptor*> _partition_slot_descs;
    std::vector<SlotDescriptor*> _distributed_slot_descs;
    Columns _partition_columns;
    std::vector<const Column*> _distributed_columns;
    std::vector<ExprContext*> _partitions_expr_ctxs;

    ObjectPool _obj_pool;
    std::map<int64_t, OlapTablePartition*> _partitions;
    // one partition have multi sub partition
    std::map<ChunkRow*, std::vector<int64_t>, PartionKeyComparator> _partitions_map;

    Random _rand{(uint32_t)time(nullptr)};
};

} // namespace starrocks
