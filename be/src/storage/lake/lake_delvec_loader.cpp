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

#include "storage/lake/lake_delvec_loader.h"

#include "common/logging.h"
#include "common/status.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/options.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

Status LakeDelvecLoader::load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    if (_pk_builder != nullptr) {
        // 1. find in meta builder first
        auto found = _pk_builder->find_delvec(tsid, pdelvec);
        if (!found.ok()) {
            return found.status();
        }
        if (*found) {
            return Status::OK();
        }
    }
    return load_from_file(tsid, version, pdelvec);
}

Status LakeDelvecLoader::load_from_meta(const TabletMetadataPtr& metadata, const DelvecPagePB& delvec_page,
                                        DelVectorPtr* pdelvec) {
    *pdelvec = std::make_shared<DelVector>();
    return lake::get_del_vec(_tablet_manager, *metadata, delvec_page, _fill_cache, _lake_io_opts, pdelvec->get());
}

Status LakeDelvecLoader::load_from_file(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    *pdelvec = std::make_shared<DelVector>();
    // 2. find in delvec file
    TabletMetadataPtr metadata;
    if (_cached_metadata != nullptr && _cached_metadata->id() == tsid.tablet_id &&
        _cached_metadata->version() == version) {
        metadata = _cached_metadata;
    } else {
        // Always go through the path-based overload so we get the cached shared
        // TabletMetadataPB without an unconditional deep copy (the tablet_id
        // overload's std::make_shared<TabletMetadata>(*ptr) used to dominate
        // VerticalCompactionTask CPU on cold-start cluster traces).
        const std::string filepath =
                (_lake_io_opts.location_provider != nullptr)
                        ? _lake_io_opts.location_provider->tablet_metadata_location(tsid.tablet_id, version)
                        : _tablet_manager->tablet_metadata_location(tsid.tablet_id, version);
        // Bridge the read policy onto the metadata load. The two fill_* fields reproduce exactly what
        // the bool overload built from _fill_cache; skip_peer_cache is what that overload cannot
        // express, and dropping it here would let a CACHE SELECT's delvec metadata read fall back to
        // a peer while every other read on the same query avoids one.
        const CacheOptions cache_opts{.fill_meta_cache = _fill_cache,
                                      .fill_data_cache = _fill_cache,
                                      .skip_peer_cache = _lake_io_opts.skip_peer_cache};
        ASSIGN_OR_RETURN(metadata, _tablet_manager->get_tablet_metadata(filepath, cache_opts, 0, _lake_io_opts.fs));
    }

    RETURN_IF_ERROR(
            lake::get_del_vec(_tablet_manager, *metadata, tsid.segment_id, _fill_cache, _lake_io_opts, pdelvec->get()));
    return Status::OK();
}

} // namespace starrocks::lake
