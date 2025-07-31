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

#include <span>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks::lake {
class TabletManager;

struct PublisnTabletInfo {
    int64_t tablet_id_in_txn_log;
    int64_t tablet_id_in_metadata;
    bool tablet_splitting;

    PublisnTabletInfo(int64_t tablet_id_in_txn_log, int64_t tablet_id_in_metadata, bool tablet_splitting)
            : tablet_id_in_txn_log(tablet_id_in_txn_log),
              tablet_id_in_metadata(tablet_id_in_metadata),
              tablet_splitting(tablet_splitting) {}
};

TxnLogPtr convert_txn_log_for_dynamic_tablet(const TxnLogPtr& original_txn_log,
                                             const TabletMetadataPtr& base_tablet_metadata, bool tablet_splitting);

StatusOr<std::vector<TabletMetadataPtr>> publish_splitting_tablet(TabletManager* tablet_manager,
                                                                  const std::span<std::string>& distribution_columns,
                                                                  const SplittingTabletInfoPB& splitting_tablet,
                                                                  int64_t base_version, int64_t new_version,
                                                                  const TxnInfoPB& txn_info,
                                                                  bool skip_write_tablet_metadata);

StatusOr<std::vector<TabletMetadataPtr>> publish_merging_tablet(TabletManager* tablet_manager,
                                                                const std::span<std::string>& distribution_columns,
                                                                const MergingTabletInfoPB& merging_tablet,
                                                                int64_t base_version, int64_t new_version,
                                                                const TxnInfoPB& txn_info,
                                                                bool skip_write_tablet_metadata);

StatusOr<std::vector<TabletMetadataPtr>> publish_identical_tablet(TabletManager* tablet_manager,
                                                                  const IdenticalTabletInfoPB& identical_tablet,
                                                                  int64_t base_version, int64_t new_version,
                                                                  const TxnInfoPB& txn_info,
                                                                  bool skip_write_tablet_metadata);

} // namespace starrocks::lake
