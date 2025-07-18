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

#include "exec/schema_scanner/schema_pipes.h"

#include "exec/schema_scanner.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "types/date_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTablePipes::_s_columns[] = {
        {"DATABASE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"PIPE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"PIPE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"PROPERTIES", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"LOAD_STATUS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"LAST_ERROR", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"CREATED_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
};

SchemaTablePipes::SchemaTablePipes()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status SchemaTablePipes::start(RuntimeState* state) {
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    return SchemaScanner::start(state);
}

Status SchemaTablePipes::_list_pipes() {
    RETURN_IF(_param->ip == nullptr || _param->port == 0, Status::InternalError("unknown frontend address"));

    TListPipesParams params;
    if (_param->current_user_ident) {
        params.__set_user_ident(*_param->current_user_ident);
    }
    return SchemaHelper::list_pipes(_ss_state, params, &_pipes_result);
}

Status SchemaTablePipes::get_next(ChunkPtr* chunk, bool* eos) {
    while (_cur_row >= _pipes_result.pipes.size()) {
        if (!_fetched) {
            // send RPC
            _fetched = true;
            RETURN_IF_ERROR(_list_pipes());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return _fill_chunk(chunk);
}

DatumArray SchemaTablePipes::_build_row() {
    auto& pipe = _pipes_result.pipes.at(_cur_row++);
    return {
            Slice(pipe.database_name),
            pipe.pipe_id,
            Slice(pipe.pipe_name),
            Slice(pipe.properties),
            Slice(pipe.state),
            Slice(pipe.table_name),
            Slice(pipe.load_status),
            Slice(pipe.last_error),
            TimestampValue::create_from_unixtime(pipe.created_time, _runtime_state->timezone_obj()),
    };
}

Status SchemaTablePipes::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto datum_array = _build_row();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
}

} // namespace starrocks