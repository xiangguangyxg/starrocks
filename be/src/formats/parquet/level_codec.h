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

#include <glog/logging.h>
#include <string.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "formats/parquet/types.h"
#include "gen_cpp/parquet_types.h"
#include "util/bit_stream_utils.h"
#include "util/rle_encoding.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace starrocks {
class Slice;
}

namespace starrocks::parquet {

class LevelDecoder {
public:
    LevelDecoder(int64_t* const timer) : _timer(timer) {}
    ~LevelDecoder() = default;

    // Decode will try to decode data in slice, only some of the input slice will used.
    // This function will changed slice to undecoded part.
    // For example:
    //     input 1000 length data, and decoder digest 100 bytes. slice will be set to
    //     the last 900.
    Status parse(tparquet::Encoding::type encoding, level_t max_level, uint32_t num_levels, Slice* slice);
    Status parse_v2(uint32_t num_bytes, level_t max_level, uint32_t num_levels, Slice* slice);

    size_t next_repeated_count() {
        DCHECK_EQ(_encoding, tparquet::Encoding::RLE);
        return _rle_decoder.repeated_count();
    }

    level_t get_repeated_value(size_t count) { return _rle_decoder.get_repeated_value(count); }

    void get_levels(level_t** levels, size_t* num_levels) {
        *levels = &_levels[0];
        *num_levels = _levels_parsed;
    }

    level_t* get_forward_levels(size_t num_levels) { return &_levels[_levels_parsed - num_levels]; }

    void reset() {
        size_t num_levels = _levels_decoded - _levels_parsed;
        if (num_levels == 0) {
            _levels_parsed = _levels_decoded = 0;
            return;
        }
        if (_levels_parsed == 0) {
            return;
        }

        memmove(&_levels[0], &_levels[_levels_parsed], num_levels * sizeof(level_t));
        _levels_decoded -= _levels_parsed;
        _levels_parsed = 0;
    }

    void consume_levels(size_t num_levels) { _levels_parsed += num_levels; }

    size_t get_avail_levels(size_t row, level_t** levels) {
        size_t batch_size = _get_level_to_decode_batch_size(row);
        if (batch_size > 0) {
            size_t new_capacity = batch_size + _levels_decoded;
            if (new_capacity > _levels_capacity) {
                _levels.resize(new_capacity);
                _levels_capacity = new_capacity;
            }
            size_t res_def = _decode_batch(batch_size, &_levels[_levels_decoded]);
            _levels_decoded += res_def;
        }
        *levels = &_levels[_levels_parsed];
        return _levels_decoded - _levels_parsed;
    }

    void append_default_levels(size_t level_nums) {
        size_t new_capacity = _levels_parsed + level_nums;
        if (new_capacity > _levels_capacity) {
            _levels.resize(new_capacity);
            _levels_capacity = new_capacity;
        }
        memset(&_levels[_levels_parsed], 0x0, level_nums * sizeof(level_t));
        _levels_parsed += level_nums;
        _levels_decoded = _levels_parsed;
    }

private:
    size_t _get_level_to_decode_batch_size(size_t row);

    // Try to decode n levels into levels;
    size_t _decode_batch(size_t n, level_t* levels) {
        SCOPED_RAW_TIMER(_timer);
        if (_encoding == tparquet::Encoding::RLE) {
            // NOTE(zc): Because RLE can only record elements that are multiples of 8,
            // it must be ensured that the incoming parameters cannot exceed the boundary.
            n = std::min((size_t)_num_levels, n);
            if (PREDICT_FALSE(!_rle_decoder.GetBatch(levels, n))) {
                return 0;
            }
            _num_levels -= n;
            return n;
        } else if (_encoding == tparquet::Encoding::BIT_PACKED) {
            DCHECK(false);
        }
        return 0;
    }

    tparquet::Encoding::type _encoding;
    level_t _bit_width = 0;
    [[maybe_unused]] level_t _max_level = 0;
    uint32_t _num_levels = 0;
    RleDecoder<level_t> _rle_decoder;
    BitReader _bit_packed_decoder;

    int64_t* const _timer;

    // used for level decoding batch then batch
    size_t _levels_parsed = 0;
    size_t _levels_decoded = 0;
    size_t _levels_capacity = 0;
    std::vector<level_t> _levels;
};

} // namespace starrocks::parquet
