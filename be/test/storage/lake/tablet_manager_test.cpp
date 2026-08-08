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

#include "storage/lake/tablet_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>

#include "base/bthreads/util.h"
#include "base/failpoint/fail_point.h"
#include "base/path/filesystem_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "common/config_lake_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/storage_define.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "platform/store_path.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/metacache.h"
#include "storage/lake/options.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/rowset/segment.h"
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

// NOTE: intend to put the following header to the end of the include section
// so that our `gutil/dynamic_annotations.h` takes precedence of the absl's.
// NOLINTNEXTLINE
#include "compute_env/staros/staros_worker.h"
#include "compute_env/staros/staros_worker_runtime.h"
#include "script/script.h"

namespace starrocks {

class LakeTabletManagerTest : public testing::Test {
public:
    LakeTabletManagerTest() : _test_dir() {};

    ~LakeTabletManagerTest() override = default;

    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = new starrocks::lake::TabletManager(_location_provider, _update_manager.get(), 1024 * 1024);
    }

    void TearDown() override {
        delete _tablet_manager;
        FileSystem::Default()->delete_dir_recursive(_test_dir);
    }

    starrocks::lake::TabletManager* _tablet_manager{nullptr};
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider{nullptr};
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
};

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, tablet_meta_write_and_read) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    string result;
    ASSERT_TRUE(
            execute_script(fmt::format("System.print(StorageEngine.get_lake_tablet_metadata_json({},2))", tablet_id),
                           result)
                    .ok());
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), tablet_id);
    EXPECT_EQ(res.value()->version(), 2);
    EXPECT_OK(_tablet_manager->delete_tablet_metadata(tablet_id, 2));
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.status().is_not_found());
}

TEST_F(LakeTabletManagerTest, tablet_meta_read_corrupted_and_recover) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    _tablet_manager->metacache()->prune();
    bool is_first_time = true;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFile::load::corruption");
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::load::corruption");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    // The metadata may be read with the legacy headerless format or the checksummed header format
    // depending on lake_enable_protobuf_file_checksum, so inject the first-read corruption on both.
    auto inject_first_read_corruption = [&](void* arg) {
        if (is_first_time) {
            *(Status*)arg = Status::Corruption("injected error");
            is_first_time = false;
        }
    };
    SyncPoint::GetInstance()->SetCallBack("ProtobufFile::load::corruption", inject_first_read_corruption);
    SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::load::corruption", inject_first_read_corruption);
    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->id(), tablet_id);
    EXPECT_EQ(res.value()->version(), 2);
}

// Self-heal: once the corrupted metadata file has had its local cache entry dropped, the
// immediately following re-read must bypass both the local disk cache and peer BEs, or it can be
// handed back the very bytes that were just dropped. tablet_meta_read_corrupted_and_recover above
// observes only the final Status; the open_options hook fires on *both* opens, so record them in
// order and check the policy changed between them.
TEST_F(LakeTabletManagerTest, tablet_meta_self_heal_reread_bypasses_disk_and_peer_cache) {
    struct OpenPolicy {
        bool skip_fill_local_cache;
        bool skip_disk_cache;
        bool skip_peer_cache;
    };

    auto read_with_first_open_corrupted = [&](bool fill_cache, std::vector<OpenPolicy>* opens) {
        starrocks::TabletMetadata metadata;
        auto tablet_id = next_id();
        metadata.set_id(tablet_id);
        metadata.set_version(2);
        ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));
        // put_tablet_metadata() warmed the metacache; prune so the read below is a genuinely cold
        // open that reaches ProtobufFileWithHeader::load rather than a metacache hit.
        _tablet_manager->metacache()->prune();

        bool first_read = true;
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::load::corruption");
            SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::open_options");
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::open_options", [&](void* arg) {
            const auto* o = static_cast<RandomAccessFileOptions*>(arg);
            opens->push_back({o->skip_fill_local_cache, o->skip_disk_cache, o->skip_peer_cache});
        });
        SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::load::corruption", [&](void* arg) {
            if (first_read) {
                first_read = false;
                *(Status*)arg = Status::Corruption("injected error");
            }
        });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });

        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2, fill_cache);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(tablet_id, res.value()->id());
    };

    std::vector<OpenPolicy> opens;
    read_with_first_open_corrupted(/*fill_cache=*/true, &opens);
    ASSERT_EQ(2u, opens.size()); // positive control: the first open plus the self-heal re-read
    EXPECT_FALSE(opens[0].skip_disk_cache);
    EXPECT_FALSE(opens[0].skip_peer_cache);
    EXPECT_TRUE(opens[1].skip_disk_cache);
    EXPECT_TRUE(opens[1].skip_peer_cache);
    // The bypass is about where the bytes are *read* from; it does not stop the fresh bytes from
    // being cached, so fill_cache=true still leaves skip_fill_local_cache off on the re-read.
    EXPECT_FALSE(opens[0].skip_fill_local_cache);
    EXPECT_FALSE(opens[1].skip_fill_local_cache);

    // fill_cache=false: the bypass is layered on top of the caller's fill-cache policy rather than
    // replacing it, so a metadata read that was already not populating the cache still self-heals.
    std::vector<OpenPolicy> no_fill_opens;
    read_with_first_open_corrupted(/*fill_cache=*/false, &no_fill_opens);
    ASSERT_EQ(2u, no_fill_opens.size());
    EXPECT_TRUE(no_fill_opens[0].skip_fill_local_cache);
    EXPECT_TRUE(no_fill_opens[1].skip_fill_local_cache);
    EXPECT_FALSE(no_fill_opens[0].skip_disk_cache);
    EXPECT_TRUE(no_fill_opens[1].skip_disk_cache);
    EXPECT_TRUE(no_fill_opens[1].skip_peer_cache);
}

// A cache write landing while the corrupted entry is being dropped must not change what the read
// returns. The load-bearing assertion is that the re-read is still issued with the disk cache
// bypassed, which is what keeps a repopulated on-disk entry -- invisible to a unit test, since
// there is no real disk cache here -- out of the result.
//
// The returned-value assertion is a weaker guard, and worth naming as such: it holds today by
// ordering rather than by the bypass, because this call passed the metacache lookup before the
// handler runs and the loaded value overwrites the entry on the way out. It pins that ordering
// against future change; it is not evidence of concurrency safety.
TEST_F(LakeTabletManagerTest, tablet_meta_self_heal_reread_unaffected_by_cache_write_during_drop) {
    constexpr int64_t kDurableCommitTime = 777;
    constexpr int64_t kStaleCommitTime = 111;

    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    metadata.set_commit_time(kDurableCommitTime);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));
    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    _tablet_manager->metacache()->prune();

    bool first_read = true;
    int bypassing_opens = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::load::corruption");
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::open_options");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::load::corruption", [&](void* arg) {
        if (first_read) {
            first_read = false;
            *(Status*)arg = Status::Corruption("injected error");
        }
    });
    SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::open_options", [&](void* arg) {
        if (static_cast<RandomAccessFileOptions*>(arg)->skip_disk_cache) {
            bypassing_opens++;
        }
    });
    // Stands in for whatever writes the entry back while the drop is in progress.
    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler", [&](void* arg) {
        auto stale = std::make_shared<starrocks::TabletMetadataPB>();
        stale->set_id(tablet_id);
        stale->set_version(2);
        stale->set_commit_time(kStaleCommitTime);
        _tablet_manager->metacache()->cache_tablet_metadata(path, std::move(stale));
        *(Status*)arg = Status::OK();
    });

    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(1, bypassing_opens); // positive control: the bypassing re-read actually happened
    EXPECT_EQ(kDurableCommitTime, res.value()->commit_time()); // ordering guard, see above
}

// A metadata corruption is a category-(a) site: the entry is invalidated and the file is
// immediately re-read, so it moves the `other` drop bucket and both self-heal re-read counters.
//
// All assertions are deltas around the read. These are process-global counters and other tests in
// this binary move them, so absolute values would be meaningless.
TEST_F(LakeTabletManagerTest, tablet_meta_corruption_counts_other_drop_and_reread) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));
    // put_tablet_metadata() warmed the metacache; prune so the read below is a genuinely cold open
    // that reaches the load path instead of being answered from the metacache.
    _tablet_manager->metacache()->prune();

    bool first_read = true;
    bool handler_ran = false;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFile::load::corruption");
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::load::corruption");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    // The metadata may be read with the legacy headerless format or the checksummed header format
    // depending on lake_enable_protobuf_file_checksum, so inject the first-read corruption on both.
    auto inject_first_read_corruption = [&](void* arg) {
        if (first_read) {
            first_read = false;
            *(Status*)arg = Status::Corruption("injected error");
        }
    };
    SyncPoint::GetInstance()->SetCallBack("ProtobufFile::load::corruption", inject_first_read_corruption);
    SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::load::corruption", inject_first_read_corruption);
    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler", [&](void* arg) {
        handler_ran = true;
        *(Status*)arg = Status::OK();
    });

    auto* metrics = StorageMetrics::instance();
    const int64_t other_before = metrics->invalidating_cache_drops(CacheDropSite::kOther).value();
    const int64_t gtid_before = metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value();
    const int64_t bundle_page_before = metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value();
    const int64_t attempts_before = metrics->self_heal_reread_attempts_total.value();
    const int64_t successes_before = metrics->self_heal_reread_success_total.value();

    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(tablet_id, res.value()->id());
    ASSERT_TRUE(handler_ran); // positive control: the drop really happened, the read did not just succeed

    EXPECT_EQ(other_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kOther).value());
    EXPECT_EQ(gtid_before, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    EXPECT_EQ(bundle_page_before, metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value());
    EXPECT_EQ(attempts_before + 1, metrics->self_heal_reread_attempts_total.value());
    EXPECT_EQ(successes_before + 1, metrics->self_heal_reread_success_total.value());
}

// The GTID mismatch in load_tablet_metadata() is a drop-only site: the stale entry is invalidated,
// the caller is told NotFound, and everything the caller tries next reads a different file, so there
// is no re-read of this path to observe. So it moves its own bucket and neither self-heal re-read
// counter. (The mismatch in get_single_tablet_metadata() is not drop-only -- see
// gtid_mismatch_rereads_bundle_bypassing_caches.)
TEST_F(LakeTabletManagerTest, tablet_meta_gtid_mismatch_counts_drop_without_reread) {
    constexpr int64_t kDurableGtid = 1000;
    constexpr int64_t kRequestedGtid = 1001;

    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    metadata.set_gtid(kDurableGtid);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));
    _tablet_manager->metacache()->prune();

    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    // Positive control: with the matching gtid the same call reads the file fine, so the NotFound
    // below is the gtid rejection and not a read that never found anything. fill_cache=false keeps
    // this probe out of the metacache, so the real read still goes to the file.
    auto probe = _tablet_manager->get_tablet_metadata(path, /*fill_cache=*/false, kDurableGtid);
    ASSERT_TRUE(probe.ok()) << probe.status();

    auto* metrics = StorageMetrics::instance();
    const int64_t gtid_before = metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value();
    const int64_t other_before = metrics->invalidating_cache_drops(CacheDropSite::kOther).value();
    const int64_t attempts_before = metrics->self_heal_reread_attempts_total.value();
    const int64_t successes_before = metrics->self_heal_reread_success_total.value();

    auto res = _tablet_manager->get_tablet_metadata(path, /*fill_cache=*/false, kRequestedGtid);
    ASSERT_TRUE(res.status().is_not_found()) << res.status();

    EXPECT_EQ(gtid_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    EXPECT_EQ(other_before, metrics->invalidating_cache_drops(CacheDropSite::kOther).value());
    EXPECT_EQ(attempts_before, metrics->self_heal_reread_attempts_total.value());
    EXPECT_EQ(successes_before, metrics->self_heal_reread_success_total.value());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, txnlog_write_and_read) {
    starrocks::TxnLog txnLog;
    auto tablet_id = next_id();
    txnLog.set_tablet_id(tablet_id);
    txnLog.set_txn_id(2);
    EXPECT_OK(_tablet_manager->put_txn_log(txnLog));
    auto res = _tablet_manager->get_txn_log(tablet_id, 2);
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value()->tablet_id(), tablet_id);
    EXPECT_EQ(res.value()->txn_id(), 2);
}

// Compatibility: tablet metadata / txn logs written by an older BE without the checksum
// header (legacy plain protobuf) must still be read correctly through the new load path.
// This verifies the LAKE_META_HEADER_MAGIC_NUMBER detection: a legacy file does not carry
// the magic, so the reader must fall back to plain protobuf instead of failing; a file
// written with the checksum on does carry the magic and is read + verified.
// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, read_legacy_format_written_without_checksum) {
    auto old_flag = config::lake_enable_protobuf_file_checksum;
    DeferOp guard([old_flag]() { config::lake_enable_protobuf_file_checksum = old_flag; });

    // ---- tablet metadata: write legacy (no header), read with checksum enabled ----
    auto tablet_id = next_id();
    config::lake_enable_protobuf_file_checksum = false;
    starrocks::TabletMetadata metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto* rowset = metadata.add_rowsets();
    rowset->set_id(2);
    rowset->set_overlapped(false);
    rowset->set_data_size(1024);
    rowset->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    config::lake_enable_protobuf_file_checksum = true;
    _tablet_manager->metacache()->prune(); // drop cached PB so the read hits the file
    auto meta_res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    ASSERT_OK(meta_res.status());
    EXPECT_EQ(meta_res.value()->id(), tablet_id);
    EXPECT_EQ(meta_res.value()->version(), 2);
    ASSERT_EQ(meta_res.value()->rowsets_size(), 1);
    EXPECT_EQ(meta_res.value()->rowsets(0).num_rows(), 5);

    // ---- txn log: write legacy (no header), read with checksum enabled ----
    config::lake_enable_protobuf_file_checksum = false;
    starrocks::TxnLog legacy_log;
    legacy_log.set_tablet_id(tablet_id);
    legacy_log.set_txn_id(99);
    EXPECT_OK(_tablet_manager->put_txn_log(legacy_log));

    config::lake_enable_protobuf_file_checksum = true;
    _tablet_manager->metacache()->prune();
    auto log_res = _tablet_manager->get_txn_log(tablet_id, 99);
    ASSERT_OK(log_res.status());
    EXPECT_EQ(log_res.value()->tablet_id(), tablet_id);
    EXPECT_EQ(log_res.value()->txn_id(), 99);

    // ---- sanity: a checksummed txn log (magic present) is also read back correctly ----
    auto tablet_id2 = next_id();
    starrocks::TxnLog checksummed_log;
    checksummed_log.set_tablet_id(tablet_id2);
    checksummed_log.set_txn_id(100);
    EXPECT_OK(_tablet_manager->put_txn_log(checksummed_log)); // checksum on
    _tablet_manager->metacache()->prune();
    auto log_res2 = _tablet_manager->get_txn_log(tablet_id2, 100);
    ASSERT_OK(log_res2.status());
    EXPECT_EQ(log_res2.value()->tablet_id(), tablet_id2);
    EXPECT_EQ(log_res2.value()->txn_id(), 100);
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());
    EXPECT_EQ(CompactionStrategyPB::DEFAULT, metadata->compaction_strategy());
}

TEST_F(LakeTabletManagerTest, create_tablet_enable_tablet_creation_optimization) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.__set_enable_tablet_creation_optimization(true);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_tablet_manager->tablet_initial_metadata_location(tablet_id)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());

    ASSIGN_OR_ABORT(auto meta_iter, _tablet_manager->list_tablet_metadata(tablet_id));
    EXPECT_TRUE(meta_iter.has_next());
    ASSIGN_OR_ABORT(metadata, meta_iter.next());
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());
    EXPECT_EQ(CompactionStrategyPB::DEFAULT, metadata->compaction_strategy());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_duplicate_column_id_or_name) {
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    req.tablet_schema.columns.resize(2);
    auto& c0 = req.tablet_schema.columns[0];
    c0.__set_is_key(true);
    c0.__set_is_allow_null(false);
    c0.__set_column_name("c0");
    c0.__set_aggregation_type(TAggregationType::NONE);
    c0.__set_col_unique_id(0);
    c0.__set_column_type(col_type);
    auto& c1 = req.tablet_schema.columns[1];
    c1 = c0;
    c1.__set_column_name("c1");
    auto st = _tablet_manager->create_tablet(req);
    ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, st.code());
    ASSERT_TRUE(MatchPattern(st.message(), "*Duplicate column id*")) << st.message();

    c1.__set_col_unique_id(1);
    c1.__set_column_name(c0.column_name);
    st = _tablet_manager->create_tablet(req);
    ASSERT_EQ(TStatusCode::INVALID_ARGUMENT, st.code());
    ASSERT_TRUE(MatchPattern(st.message(), "*Duplicate column name*")) << st.message();
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet_without_schema_file) {
    auto fs = FileSystem::Default();

    for (auto create_schema_file : {false, true}) {
        auto tablet_id = next_id();
        auto schema_id = next_id();

        TCreateTabletReq req;
        req.tablet_id = tablet_id;
        req.__set_version(1);
        req.__set_version_hash(0);
        req.tablet_schema.__set_id(schema_id);
        req.tablet_schema.__set_schema_hash(270068375);
        req.tablet_schema.__set_short_key_column_count(2);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        req.__set_create_schema_file(create_schema_file);
        EXPECT_OK(_tablet_manager->create_tablet(req));
        EXPECT_TRUE(_tablet_manager->get_tablet(tablet_id).ok());
        EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
        auto st = fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id));
        if (create_schema_file) {
            EXPECT_TRUE(st.ok()) << st;
        } else {
            EXPECT_TRUE(st.is_not_found()) << st;
        }
    }
}

TEST_F(LakeTabletManagerTest, create_tablet_with_compaction_strategy) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();
    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::LOCAL);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    req.__set_compaction_strategy(TCompactionStrategy::REAL_TIME);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::LOCAL, metadata->persistent_index_type());
    EXPECT_EQ(CompactionStrategyPB::REAL_TIME, metadata->compaction_strategy());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_range) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    // Set tablet range with lower and upper bounds
    TTabletRange range;
    TTuple lower_bound;
    TVariant lower_val1;
    TTypeDesc type_desc_int;
    type_desc_int.types.resize(1);
    type_desc_int.types[0].type = TTypeNodeType::SCALAR;
    type_desc_int.types[0].scalar_type.type = TPrimitiveType::INT;
    type_desc_int.types[0].__isset.scalar_type = true;
    type_desc_int.__isset.types = true;
    lower_val1.__set_type(type_desc_int);
    lower_val1.__set_value("100");
    lower_val1.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(lower_val1);

    TVariant lower_val2;
    TTypeDesc type_desc_varchar;
    type_desc_varchar.types.resize(1);
    type_desc_varchar.types[0].type = TTypeNodeType::SCALAR;
    type_desc_varchar.types[0].scalar_type.type = TPrimitiveType::VARCHAR;
    type_desc_varchar.types[0].scalar_type.len = 10;
    type_desc_varchar.types[0].__isset.scalar_type = true;
    type_desc_varchar.__isset.types = true;
    lower_val2.__set_type(type_desc_varchar);
    lower_val2.__set_value("abc");
    lower_val2.__set_variant_type(TVariantType::NORMAL_VALUE);
    lower_bound.values.push_back(lower_val2);

    TTuple upper_bound;
    TVariant upper_val1;
    upper_val1.__set_type(type_desc_int);
    upper_val1.__set_value("200");
    upper_val1.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(upper_val1);

    TVariant upper_val2;
    upper_val2.__set_type(type_desc_varchar);
    upper_val2.__set_value("xyz");
    upper_val2.__set_variant_type(TVariantType::NORMAL_VALUE);
    upper_bound.values.push_back(upper_val2);

    range.__set_lower_bound(lower_bound);
    range.__set_upper_bound(upper_bound);
    range.__set_lower_bound_included(true);
    range.__set_upper_bound_included(false);
    req.__set_range(range);

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());

    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());

    // Verify range is set correctly
    EXPECT_TRUE(metadata->has_range());
    const auto& pb_range = metadata->range();
    EXPECT_TRUE(pb_range.has_lower_bound());
    EXPECT_EQ(2, pb_range.lower_bound().values_size());
    EXPECT_EQ("100", pb_range.lower_bound().values(0).value());
    EXPECT_EQ("abc", pb_range.lower_bound().values(1).value());

    EXPECT_TRUE(pb_range.has_upper_bound());
    EXPECT_EQ(2, pb_range.upper_bound().values_size());
    EXPECT_EQ("200", pb_range.upper_bound().values(0).value());
    EXPECT_EQ("xyz", pb_range.upper_bound().values(1).value());

    EXPECT_TRUE(pb_range.has_lower_bound_included());
    EXPECT_TRUE(pb_range.lower_bound_included());
    EXPECT_TRUE(pb_range.has_upper_bound_included());
    EXPECT_FALSE(pb_range.upper_bound_included());
}

TEST_F(LakeTabletManagerTest, create_tablet_with_range_null_values) {
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    // Set tablet range with NULL values
    TTabletRange range;
    TTuple lower_bound;
    TVariant null_val;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::INT;
    type_desc.types[0].__isset.scalar_type = true;
    type_desc.__isset.types = true;
    null_val.__set_type(type_desc);
    null_val.__set_variant_type(TVariantType::NULL_VALUE);
    lower_bound.values.push_back(null_val);
    range.__set_lower_bound(lower_bound);
    req.__set_range(range);

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    // Verify range with NULL value
    EXPECT_TRUE(metadata->has_range());
    const auto& pb_range = metadata->range();
    EXPECT_TRUE(pb_range.has_lower_bound());
    EXPECT_EQ(1, pb_range.lower_bound().values_size());
    EXPECT_EQ(VariantTypePB::NULL_VALUE, pb_range.lower_bound().values(0).variant_type());
}

// MINIMUM/MAXIMUM variants should be rejected — FE must map them to NULL_VALUE.
TEST_F(LakeTabletManagerTest, create_tablet_with_range_rejects_min_max) {
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    TTabletRange range;
    TTuple lower_bound;
    TVariant min_val;
    TTypeDesc type_desc;
    type_desc.types.resize(1);
    type_desc.types[0].type = TTypeNodeType::SCALAR;
    type_desc.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    type_desc.types[0].__isset.scalar_type = true;
    type_desc.__isset.types = true;
    min_val.__set_type(type_desc);
    min_val.__set_variant_type(TVariantType::MINIMUM);
    lower_bound.values.push_back(min_val);
    range.__set_lower_bound(lower_bound);
    req.__set_range(range);

    auto status = _tablet_manager->create_tablet(req);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_invalid_argument());
}

TEST_F(LakeTabletManagerTest, create_tablet_without_range) {
    // Test backward compatibility: create tablet without range
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
    // Explicitly not setting range

    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));

    // Verify no range is set
    EXPECT_FALSE(metadata->has_range());
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, list_tablet_meta) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto rowset_meta_pb = metadata.add_rowsets();
    rowset_meta_pb->set_id(2);
    rowset_meta_pb->set_overlapped(false);
    rowset_meta_pb->set_data_size(1024);
    rowset_meta_pb->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(3);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_id(next_id());
    metadata.set_version(2);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    ASSIGN_OR_ABORT(auto metaIter, _tablet_manager->list_tablet_metadata(tablet_id));

    std::vector<std::string> objects;
    while (metaIter.has_next()) {
        ASSIGN_OR_ABORT(auto tabletmeta_ptr, metaIter.next());
        objects.emplace_back(fmt::format("{:016X}_{:016X}.meta", tabletmeta_ptr->id(), tabletmeta_ptr->version()));
    }

    EXPECT_EQ(objects.size(), 2);
    auto iter = std::find(objects.begin(), objects.end(), fmt::format("{:016X}_{:016X}.meta", tablet_id, 2));
    EXPECT_TRUE(iter != objects.end());
    iter = std::find(objects.begin(), objects.end(), fmt::format("{:016X}_{:016X}.meta", tablet_id, 3));
    EXPECT_TRUE(iter != objects.end());
}

// TODO: enable this test.
// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, DISABLED_put_get_tabletmetadata_witch_cache_evict) {
    int64_t tablet_id = 23456;
    std::vector<TabletMetadataPtr> vec;

    // we set meta cache capacity to 16K, and each meta here cost 232 bytes,putting 64 tablet meta will fill up the cache space.
    for (int i = 0; i < 64; ++i) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(2 + i);
        auto rowset_meta_pb = metadata->add_rowsets();
        rowset_meta_pb->set_id(2);
        rowset_meta_pb->set_overlapped(false);
        rowset_meta_pb->set_data_size(1024);
        rowset_meta_pb->set_num_rows(5);
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
        vec.emplace_back(metadata);
    }

    // get version 4 from cache
    {
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 4);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 4);
    }

    // put another 32 tablet meta to trigger cache eviction.
    for (int i = 0; i < 32; ++i) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(66 + i);
        auto rowset_meta_pb = metadata->add_rowsets();
        rowset_meta_pb->set_id(2);
        rowset_meta_pb->set_overlapped(false);
        rowset_meta_pb->set_data_size(1024);
        rowset_meta_pb->set_num_rows(5);
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
    }

    // test eviction result;
    {
        // version 4 expect not evicted
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 4);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 4);
        EXPECT_EQ(res.value().get(), vec[2].get());
    }
    {
        // version 6 expect evicted
        auto res = _tablet_manager->get_tablet_metadata(tablet_id, 6);
        EXPECT_TRUE(res.ok());
        EXPECT_EQ(res.value()->id(), tablet_id);
        EXPECT_EQ(res.value()->version(), 6);
        EXPECT_NE(res.value().get(), vec[4].get());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, tablet_schema_load) {
    starrocks::TabletMetadata metadata;
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(2);

    auto schema = metadata.mutable_schema();
    schema->set_id(10);
    schema->set_num_short_key_columns(1);
    schema->set_keys_type(DUP_KEYS);
    schema->set_num_rows_per_row_block(65535);
    auto c0 = schema->add_column();
    {
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema->add_column();
    {
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));

    const TabletSchema* ptr = nullptr;

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    {
        auto st = tablet.get_schema();
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(st.value()->id(), 10);
        EXPECT_EQ(st.value()->num_columns(), 2);
        EXPECT_EQ(st.value()->column(0).name(), "c0");
        EXPECT_EQ(st.value()->column(1).name(), "c1");
        ptr = st.value().get();
    }
    {
        auto st = tablet.get_schema();
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(st.value()->id(), 10);
        EXPECT_EQ(st.value()->num_columns(), 2);
        EXPECT_EQ(st.value()->column(0).name(), "c0");
        EXPECT_EQ(st.value()->column(1).name(), "c1");
        EXPECT_EQ(ptr, st.value().get());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_from_base_tablet) {
    // Create base tablet:
    //  - c0 BIGINT KEY
    //  - c1 INT
    //  - c2 FLOAT
    {
        TCreateTabletReq req;
        req.tablet_id = 65535;
        req.__set_version(1);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c1 = req.tablet_schema.columns.emplace_back();
        c1.column_name = "c1";
        c1.is_key = false;
        c1.column_type.type = TPrimitiveType::INT;
        c1.is_allow_null = false;
        c1.default_value = "10";

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65535, 1));
        auto schema = tablet.get_schema();
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->next_column_unique_id());
    }
    // Add a new column "c3" based on tablet 65535
    {
        TCreateTabletReq req;
        req.tablet_id = 65536;
        req.__set_version(1);
        req.__set_base_tablet_id(65535);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c3 = req.tablet_schema.columns.emplace_back();
        c3.column_name = "c3";
        c3.is_key = false;
        c3.column_type.type = TPrimitiveType::DOUBLE;
        c3.is_allow_null = false;

        auto& c1 = req.tablet_schema.columns.emplace_back();
        c1.column_name = "c1";
        c1.is_key = false;
        c1.column_type.type = TPrimitiveType::INT;
        c1.is_allow_null = false;
        c1.default_value = "10";

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65536, 1));
        auto schema = tablet.get_schema();
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->column(3).unique_id());
        ASSERT_EQ(4, schema->next_column_unique_id());

        ASSERT_EQ("c0", schema->column(0).name());
        ASSERT_EQ("c3", schema->column(1).name());
        ASSERT_EQ("c1", schema->column(2).name());
        ASSERT_EQ("c2", schema->column(3).name());
    }
    // Drop column "c1" based on tablet 65536
    {
        TCreateTabletReq req;
        req.tablet_id = 65537;
        req.__set_version(1);
        req.__set_base_tablet_id(65536);
        req.tablet_schema.__set_id(next_id());
        req.tablet_schema.__set_schema_hash(0);
        req.tablet_schema.__set_short_key_column_count(1);
        req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        auto& c0 = req.tablet_schema.columns.emplace_back();
        c0.column_name = "c0";
        c0.is_key = true;
        c0.column_type.type = TPrimitiveType::BIGINT;
        c0.is_allow_null = false;

        auto& c3 = req.tablet_schema.columns.emplace_back();
        c3.column_name = "c3";
        c3.is_key = false;
        c3.column_type.type = TPrimitiveType::DOUBLE;
        c3.is_allow_null = false;

        auto& c2 = req.tablet_schema.columns.emplace_back();
        c2.column_name = "c2";
        c2.is_key = false;
        c2.column_type.type = TPrimitiveType::FLOAT;
        c2.is_allow_null = false;
        EXPECT_OK(_tablet_manager->create_tablet(req));

        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(65537, 1));
        auto schema = tablet.get_schema();
        ASSERT_EQ(0, schema->column(0).unique_id());
        ASSERT_EQ(1, schema->column(1).unique_id());
        ASSERT_EQ(2, schema->column(2).unique_id());
        ASSERT_EQ(3, schema->next_column_unique_id());

        ASSERT_EQ("c0", schema->column(0).name());
        ASSERT_EQ("c3", schema->column(1).name());
        ASSERT_EQ("c2", schema->column(2).name());
    }
}

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, create_tablet_with_cloud_native_persistent_index) {
    auto fs = FileSystem::Default();
    auto tablet_id = next_id();
    auto schema_id = next_id();

    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.__set_enable_persistent_index(true);
    req.__set_persistent_index_type(TPersistentIndexType::CLOUD_NATIVE);
    req.tablet_schema.__set_id(schema_id);
    req.tablet_schema.__set_schema_hash(270068375);
    req.tablet_schema.__set_short_key_column_count(2);
    req.tablet_schema.__set_keys_type(TKeysType::PRIMARY_KEYS);
    EXPECT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    EXPECT_TRUE(fs->path_exists(_location_provider->tablet_metadata_location(tablet_id, 1)).ok());
    EXPECT_TRUE(fs->path_exists(_location_provider->schema_file_location(tablet_id, schema_id)).ok());
    ASSIGN_OR_ABORT(auto metadata, tablet.get_metadata(1));
    EXPECT_EQ(tablet_id, metadata->id());
    EXPECT_EQ(1, metadata->version());
    EXPECT_EQ(1, metadata->next_rowset_id());
    EXPECT_FALSE(metadata->has_commit_time());
    EXPECT_EQ(0, metadata->rowsets_size());
    EXPECT_EQ(0, metadata->cumulative_point());
    EXPECT_FALSE(metadata->has_delvec_meta());
    EXPECT_TRUE(metadata->enable_persistent_index());
    EXPECT_EQ(TPersistentIndexType::CLOUD_NATIVE, metadata->persistent_index_type());
}

TEST_F(LakeTabletManagerTest, put_bundle_tablet_metadata) {
    std::map<int64_t, TabletMetadataPB> metadatas;
    TabletSchemaPB schema_pb1;
    {
        schema_pb1.set_id(10);
        schema_pb1.set_num_short_key_columns(1);
        schema_pb1.set_keys_type(DUP_KEYS);
        schema_pb1.set_num_rows_per_row_block(65535);
        auto c0 = schema_pb1.add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }

    TabletSchemaPB schema_pb2;
    {
        schema_pb2.set_id(11);
        schema_pb2.set_num_short_key_columns(1);
        schema_pb2.set_keys_type(DUP_KEYS);
        schema_pb2.set_num_rows_per_row_block(65535);
        auto c1 = schema_pb2.add_column();
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }

    TabletSchemaPB schema_pb3;
    {
        schema_pb3.set_id(12);
        schema_pb3.set_num_short_key_columns(1);
        schema_pb3.set_keys_type(DUP_KEYS);
        schema_pb3.set_num_rows_per_row_block(65535);
        auto c2 = schema_pb3.add_column();
        c2->set_unique_id(2);
        c2->set_name("c2");
        c2->set_type("INT");
        c2->set_is_key(false);
        c2->set_is_nullable(false);
    }

    starrocks::TabletMetadataPB metadata1;
    {
        metadata1.set_id(1);
        metadata1.set_version(2);
        metadata1.mutable_schema()->CopyFrom(schema_pb1);
        auto& item1 = (*metadata1.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata1.mutable_historical_schemas())[11];
        item2.CopyFrom(schema_pb2);
        (*metadata1.mutable_rowset_to_schema())[3] = 11;
    }

    starrocks::TabletMetadataPB metadata2;
    {
        metadata2.set_id(2);
        metadata2.set_version(2);
        metadata2.mutable_schema()->CopyFrom(schema_pb2);
        auto& item1 = (*metadata2.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata2.mutable_historical_schemas())[12];
        item2.CopyFrom(schema_pb3);
        (*metadata2.mutable_rowset_to_schema())[3] = 10;
        (*metadata2.mutable_rowset_to_schema())[4] = 12;
    }

    metadatas.emplace(1, metadata1);
    metadatas.emplace(2, metadata2);
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    {
        auto res = _tablet_manager->get_tablet_metadata(1, 2);
        EXPECT_TRUE(res.ok()) << res.status().to_string();
        TabletMetadataPtr metadata = std::move(res).value();
        ASSERT_EQ(metadata->schema().id(), 10);
        ASSERT_EQ(metadata->historical_schemas_size(), 2);
    }

    {
        _tablet_manager->metacache()->prune();
        // inject corruption error
        SyncPoint::GetInstance()->EnableProcessing();
        bool is_first_time = true;
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::parse_bundle_tablet_metadata::corruption");
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::parse_bundle_tablet_metadata::corruption",
                                              [&](void* arg) {
                                                  if (is_first_time) {
                                                      *(bool*)arg = true;
                                                      is_first_time = false;
                                                  }
                                              });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });
        auto res = _tablet_manager->get_tablet_metadata(1, 2);
        EXPECT_TRUE(res.ok()) << res.status().to_string();
        TabletMetadataPtr metadata = std::move(res).value();
        ASSERT_EQ(metadata->schema().id(), 10);
        ASSERT_EQ(metadata->historical_schemas_size(), 2);
    }

    // multi thread read
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < 2; ++i) {
            threads.emplace_back(
                    [&](int i) {
                        auto res = _tablet_manager->get_tablet_metadata(i + 1, 2);
                        ASSERT_TRUE(res.ok());
                        TabletMetadataPtr metadata = std::move(res).value();
                        ASSERT_EQ(metadata->schema().id(), 10 + i);
                        ASSERT_EQ(metadata->historical_schemas_size(), 2 + i);
                        // check id
                        ASSERT_EQ(metadata->id(), i + 1);
                        // check version
                        ASSERT_EQ(metadata->version(), 2);
                    },
                    i);
        }
        for (auto& thread : threads) {
            thread.join();
        }
    }

    {
        // prune metacache
        _tablet_manager->prune_metacache();
        std::string fp_name = "tablet_schema_not_found_in_bundle_metadata";
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(fp_name);
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        fp->setMode(trigger_mode);
        auto res = _tablet_manager->get_tablet_metadata(2, 2);
        ASSERT_FALSE(res.ok());
        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp->setMode(trigger_mode);
        res = _tablet_manager->get_tablet_metadata(2, 2);
        ASSERT_TRUE(res.ok());
        TabletMetadataPtr metadata = std::move(res).value();
        ASSERT_EQ(metadata->schema().id(), 11);
        ASSERT_EQ(metadata->historical_schemas_size(), 3);
    }

    starrocks::TabletMetadata metadata4;
    {
        metadata4.set_id(3);
        metadata4.set_version(3);
        metadata4.mutable_schema()->CopyFrom(schema_pb1);
        auto& item1 = (*metadata4.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata4.mutable_historical_schemas())[12];
        item2.CopyFrom(schema_pb3);
    }
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata4));
    auto res = _tablet_manager->get_tablet_metadata(3, 3);
    ASSERT_TRUE(res.ok());

    // get initial tablet meta by path
    starrocks::TabletMetadata metadata5;
    {
        metadata5.set_id(4);
        metadata5.set_version(1);
        metadata5.mutable_schema()->CopyFrom(schema_pb1);
        auto& item1 = (*metadata5.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb1);
        auto& item2 = (*metadata5.mutable_historical_schemas())[12];
        item2.CopyFrom(schema_pb3);
    }
    // put initial tablet meta
    EXPECT_OK(_tablet_manager->put_tablet_metadata(std::make_shared<starrocks::TabletMetadata>(metadata5),
                                                   _tablet_manager->tablet_initial_metadata_location(metadata5.id())));
    ASSERT_TRUE(_tablet_manager->get_tablet_metadata(4, 1).ok());
    ASSERT_TRUE(_tablet_manager->get_tablet_metadata(_tablet_manager->tablet_metadata_location(4, 1)).ok());
}

TEST_F(LakeTabletManagerTest, get_inital_tablet_metadata) {
    auto tablet_id = next_id();

    // Create initial tablet metadata without setting tablet id
    starrocks::TabletMetadata initial_metadata;
    initial_metadata.set_version(1);
    initial_metadata.set_next_rowset_id(1);

    // Save it to initial metadata location
    EXPECT_OK(_tablet_manager->put_tablet_metadata(std::make_shared<starrocks::TabletMetadata>(initial_metadata),
                                                   _tablet_manager->tablet_initial_metadata_location(tablet_id)));

    // Get tablet metadata by tablet_id and version
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 1);
    ASSERT_TRUE(res.ok());

    // Verify that tablet_id is correctly set from the initial metadata
    EXPECT_EQ(res.value()->id(), tablet_id);
    EXPECT_EQ(res.value()->version(), 1);
}

TEST_F(LakeTabletManagerTest, cache_tablet_metadata) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    ASSERT_TRUE(_tablet_manager->cache_tablet_metadata(metadata).ok());
    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    ASSERT_TRUE(_tablet_manager->metacache()->lookup_tablet_metadata(path) != nullptr);
    ASSERT_TRUE(_tablet_manager->get_latest_cached_tablet_metadata(tablet_id) != nullptr);
}

TEST_F(LakeTabletManagerTest, get_tablet_metadata_cache_options) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);

    // 1. fill_meta_cache=true
    _tablet_manager->metacache()->prune();
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2, {true, true});
    EXPECT_TRUE(res.ok());
    EXPECT_TRUE(_tablet_manager->metacache()->lookup_tablet_metadata(path) != nullptr);

    // 2. fill_meta_cache=false
    _tablet_manager->metacache()->prune();
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2, {false, true});
    EXPECT_TRUE(res.ok());
    EXPECT_TRUE(_tablet_manager->metacache()->lookup_tablet_metadata(path) == nullptr);
}

// skip_meta_cache=true must distinguish "durably persisted" from "only in the
// metacache": a version whose metadata was cached (e.g. during an aggregate
// publish) but never durably written reads as NotFound.
TEST_F(LakeTabletManagerTest, get_tablet_metadata_skip_meta_cache_cache_only) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);

    // Cache-only: present in the metacache, no durable file.
    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    _tablet_manager->metacache()->cache_tablet_metadata(path, metadata);

    // A cache-hitting read sees it...
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2, lake::CacheOptions{});
    EXPECT_TRUE(res.ok());

    // ...but a skip_meta_cache read reports NotFound.
    lake::CacheOptions skip_cache_opts{.fill_meta_cache = true, .fill_data_cache = true, .skip_meta_cache = true};
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2, skip_cache_opts);
    EXPECT_TRUE(res.status().is_not_found()) << res.status();

    // Same contract on the single-tablet (bundle) read path.
    res = _tablet_manager->get_single_tablet_metadata(tablet_id, 2, skip_cache_opts);
    EXPECT_TRUE(res.status().is_not_found()) << res.status();
}

// With a durable copy present, skip_meta_cache reads it from storage (not the
// cached copy) and, with fill_meta_cache=true, refreshes the cache with it.
TEST_F(LakeTabletManagerTest, get_tablet_metadata_skip_meta_cache_reads_durable) {
    constexpr int64_t kDurableCommitTime = 100;
    constexpr int64_t kStaleCommitTime = 999999;

    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    metadata->set_commit_time(kDurableCommitTime);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    // Overwrite the cache entry with a deliberately-different stale copy.
    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    auto stale = std::make_shared<TabletMetadata>(*metadata);
    stale->set_commit_time(kStaleCommitTime);
    _tablet_manager->metacache()->cache_tablet_metadata(path, stale);

    // A cache-hitting read returns the stale copy.
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2, lake::CacheOptions{});
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(kStaleCommitTime, res.value()->commit_time());

    // skip_meta_cache returns the durable copy...
    res = _tablet_manager->get_tablet_metadata(
            tablet_id, 2,
            lake::CacheOptions{.fill_meta_cache = true, .fill_data_cache = true, .skip_meta_cache = true});
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(kDurableCommitTime, res.value()->commit_time());

    // ...and fill_meta_cache=true refreshed the cache with it.
    auto cached = _tablet_manager->metacache()->lookup_tablet_metadata(path);
    ASSERT_TRUE(cached != nullptr);
    EXPECT_EQ(kDurableCommitTime, cached->commit_time());
}

// CacheOptions::skip_peer_cache must reach the per-tablet metadata open
// (ProtobufFileWithHeader::load), since a bulk/background caller (e.g. CACHE SELECT, warmup) needs
// to bypass peer reads even before any LakeIOOptions exists.
TEST_F(LakeTabletManagerTest, get_tablet_metadata_propagates_skip_peer_cache) {
    auto metadata = std::make_shared<TabletMetadata>();
    auto tablet_id = next_id();
    metadata->set_id(tablet_id);
    metadata->set_version(2);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    int callback_count = 0;
    bool captured_skip_peer_cache = false;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::open_options");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::open_options", [&](void* arg) {
        callback_count++;
        captured_skip_peer_cache = static_cast<RandomAccessFileOptions*>(arg)->skip_peer_cache;
    });

    // put_tablet_metadata() above warmed the metacache; prune it so this read is a genuinely cold
    // open that reaches ProtobufFileWithHeader::load, not a metacache hit that never fires the hook.
    _tablet_manager->metacache()->prune();
    auto res = _tablet_manager->get_tablet_metadata(
            tablet_id, 2,
            lake::CacheOptions{.fill_meta_cache = true, .fill_data_cache = true, .skip_peer_cache = true});
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(1, callback_count); // positive control: the hook actually fired
    EXPECT_TRUE(captured_skip_peer_cache);

    // A default-constructed CacheOptions{} leaves the bit false.
    _tablet_manager->metacache()->prune();
    res = _tablet_manager->get_tablet_metadata(tablet_id, 2, lake::CacheOptions{});
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(2, callback_count);
    EXPECT_FALSE(captured_skip_peer_cache);
}

// CacheOptions::skip_peer_cache must also reach the bundled-metadata open in
// get_single_tablet_metadata -- the other terminal metadata open on the chain.
TEST_F(LakeTabletManagerTest, get_single_tablet_metadata_propagates_skip_peer_cache) {
    auto tablet_id = next_id();
    TabletSchemaPB schema_pb;
    schema_pb.set_id(10);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    starrocks::TabletMetadataPB metadata1;
    metadata1.set_id(tablet_id);
    metadata1.set_version(2);
    metadata1.mutable_schema()->CopyFrom(schema_pb);
    (*metadata1.mutable_historical_schemas())[10].CopyFrom(schema_pb);

    std::map<int64_t, TabletMetadataPB> metadatas;
    metadatas.emplace(tablet_id, metadata1);
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    int callback_count = 0;
    bool captured_skip_peer_cache = false;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::metadata_open_options");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::metadata_open_options", [&](void* arg) {
        callback_count++;
        captured_skip_peer_cache = static_cast<RandomAccessFileOptions*>(arg)->skip_peer_cache;
    });

    _tablet_manager->metacache()->prune();
    auto res = _tablet_manager->get_single_tablet_metadata(
            tablet_id, 2,
            lake::CacheOptions{.fill_meta_cache = true, .fill_data_cache = true, .skip_peer_cache = true});
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(1, callback_count); // positive control: the hook actually fired
    EXPECT_TRUE(captured_skip_peer_cache);

    // A default-constructed CacheOptions{} leaves the bit false.
    _tablet_manager->metacache()->prune();
    res = _tablet_manager->get_single_tablet_metadata(tablet_id, 2, lake::CacheOptions{});
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(2, callback_count);
    EXPECT_FALSE(captured_skip_peer_cache);
}

// Self-heal, bundled-metadata variant: the re-read after a dropped corrupted bundle must carry the
// bypass on its own options object. The first read's options are shared with whatever other callers
// the singleflight coalesced, so the bypass has to be scoped to this one retry.
TEST_F(LakeTabletManagerTest, bundle_meta_self_heal_reread_bypasses_disk_and_peer_cache) {
    auto tablet_id = next_id();
    TabletSchemaPB schema_pb;
    schema_pb.set_id(10);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    starrocks::TabletMetadataPB metadata1;
    metadata1.set_id(tablet_id);
    metadata1.set_version(2);
    metadata1.mutable_schema()->CopyFrom(schema_pb);
    (*metadata1.mutable_historical_schemas())[10].CopyFrom(schema_pb);

    std::map<int64_t, TabletMetadataPB> metadatas;
    metadatas.emplace(tablet_id, metadata1);
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    int first_opens = 0;
    int retry_opens = 0;
    const void* first_opts_addr = nullptr;
    const void* retry_opts_addr = nullptr;
    bool first_skip_disk_cache = true;
    bool retry_skip_disk_cache = false;
    bool retry_skip_peer_cache = false;
    bool inject_corruption = true;

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::parse_bundle_tablet_metadata::corruption");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::metadata_open_options");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::metadata_retry_open_options");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::parse_bundle_tablet_metadata::corruption", [&](void* arg) {
        if (inject_corruption) {
            inject_corruption = false;
            *(bool*)arg = true;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::metadata_open_options", [&](void* arg) {
        first_opens++;
        first_opts_addr = arg;
        first_skip_disk_cache = static_cast<RandomAccessFileOptions*>(arg)->skip_disk_cache;
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::metadata_retry_open_options", [&](void* arg) {
        retry_opens++;
        retry_opts_addr = arg;
        const auto* o = static_cast<RandomAccessFileOptions*>(arg);
        retry_skip_disk_cache = o->skip_disk_cache;
        retry_skip_peer_cache = o->skip_peer_cache;
    });

    auto* metrics = StorageMetrics::instance();
    const int64_t other_before = metrics->invalidating_cache_drops(CacheDropSite::kOther).value();
    const int64_t bundle_page_before = metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value();
    const int64_t attempts_before = metrics->self_heal_reread_attempts_total.value();
    const int64_t successes_before = metrics->self_heal_reread_success_total.value();

    // put_bundle_tablet_metadata() warmed the metacache; prune so the read below really opens the
    // bundle file instead of returning the cached per-tablet metadata.
    _tablet_manager->metacache()->prune();
    auto res = _tablet_manager->get_single_tablet_metadata(tablet_id, 2);
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_EQ(tablet_id, res.value()->id());

    // Positive controls: both opens happened, exactly once each.
    ASSERT_EQ(1, first_opens);
    ASSERT_EQ(1, retry_opens);
    EXPECT_FALSE(first_skip_disk_cache);
    EXPECT_TRUE(retry_skip_disk_cache);
    EXPECT_TRUE(retry_skip_peer_cache);
    // ...and the retry did not simply reuse (or mutate) the first read's options object.
    EXPECT_NE(first_opts_addr, retry_opts_addr);

    // A whole bundle file that fails to parse is the same kind of failure as a corrupted
    // single-tablet metadata file, so it is bucketed `other` and pairs with a re-read.
    // `bundle_page` stays put: that bucket is only for the two per-tablet page failures, which
    // drop without re-reading. Deltas, because these counters are process-global.
    EXPECT_EQ(other_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kOther).value());
    EXPECT_EQ(bundle_page_before, metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value());
    EXPECT_EQ(attempts_before + 1, metrics->self_heal_reread_attempts_total.value());
    EXPECT_EQ(successes_before + 1, metrics->self_heal_reread_success_total.value());
}

// The mixed-layout remainder of a GTID rejection. get_single_tablet_metadata() resolves the bundle
// itself (see gtid_mismatch_rereads_bundle_bypassing_caches); when authoritative storage confirms
// the bundle really is at a different gtid, it also invalidates the per-tablet file's cache entry,
// because this version may exist in that layout too and get_tablet_metadata() reads it next. That
// fallback has to bypass the disk and peer caches -- otherwise a peer BE holding the same stale copy
// (or an entry a concurrent reader repopulated) can hand it back and the mismatch repeats.
//
// The bypass has to hold whether or not the drop itself succeeded, so the drop's outcome is forced
// and both cases are covered. The failed-drop case is not an edge case: FileSystem::drop_local_cache
// answers NotFound when there was nothing cached (fs.h), and nothing being cached locally is exactly
// the case where a peer BE served the stale copy in the first place.
//
// The last case is the other side of the contract: a NotFound that rejected nothing must leave the
// caches alone. That one is routine, not exceptional -- once a partition has been published in
// aggregate mode, every version it did not bundle takes the same fallback.
TEST_F(LakeTabletManagerTest, gtid_mismatch_mixed_layout_fallback_bypasses_caches) {
    constexpr int64_t kStaleGtid = 1000;
    constexpr int64_t kDurableGtid = 1001;
    constexpr int64_t kSchemaId = 10;

    TabletSchemaPB schema_pb;
    schema_pb.set_id(kSchemaId);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto make_metadata = [&](int64_t tablet_id, int64_t gtid) {
        starrocks::TabletMetadataPB metadata;
        metadata.set_id(tablet_id);
        metadata.set_version(2);
        metadata.set_gtid(gtid);
        metadata.mutable_schema()->CopyFrom(schema_pb);
        (*metadata.mutable_historical_schemas())[kSchemaId].CopyFrom(schema_pb);
        return metadata;
    };

    // |bundled_id| is in the bundle with a stale gtid; |unbundled_id| is not in the bundle at all.
    // Both have an authoritative per-tablet metadata file.
    auto bundled_id = next_id();
    auto unbundled_id = next_id();
    std::map<int64_t, TabletMetadataPB> bundle;
    bundle.emplace(bundled_id, make_metadata(bundled_id, kStaleGtid));
    // Writing the bundle is also what marks the partition as using aggregated metadata, which is
    // what routes both reads below through get_single_tablet_metadata() first.
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(bundle));

    ASSERT_OK(_tablet_manager->put_tablet_metadata(make_metadata(bundled_id, kDurableGtid)));
    ASSERT_OK(_tablet_manager->put_tablet_metadata(make_metadata(unbundled_id, kDurableGtid)));

    int opens = 0;
    bool skip_disk_cache = false;
    bool skip_peer_cache = false;
    int drop_attempts = 0;
    // The drop's outcome is forced rather than inherited from the environment. PosixFileSystem does
    // not override FileSystem::drop_local_cache, so here it always answers NotFound; taking that as
    // given would silently cover only one of the two cases below, and would leave the successful
    // drop -- the one the production path normally takes -- untested.
    Status forced_drop_status = Status::OK();

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::open_options");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::gtid_mismatch_drop_status");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    // Only the per-tablet metadata file is read through ProtobufFileWithHeader; the bundle file is
    // opened directly. So this hook observes the fallback read and nothing else.
    SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::open_options", [&](void* arg) {
        opens++;
        const auto* o = static_cast<RandomAccessFileOptions*>(arg);
        skip_disk_cache = o->skip_disk_cache;
        skip_peer_cache = o->skip_peer_cache;
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::gtid_mismatch_drop_status", [&](void* arg) {
        drop_attempts++;
        *static_cast<Status*>(arg) = forced_drop_status;
    });

    auto* metrics = StorageMetrics::instance();
    const int64_t gtid_before = metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value();

    // The writes above warmed the metacache; evict just the entry being read so each read really
    // opens the files. prune() would also evict the aggregated-partition marker and send the reads
    // down the other branch, where there is no fallback to observe.
    auto bundled_path = _tablet_manager->tablet_metadata_location(bundled_id, 2);
    auto unbundled_path = _tablet_manager->tablet_metadata_location(unbundled_id, 2);
    auto read = [&](const std::string& path) {
        _tablet_manager->metacache()->erase(path);
        return _tablet_manager->get_tablet_metadata(path, lake::CacheOptions{}, kDurableGtid);
    };

    // 1. The stale entry was cached locally and the drop removed it.
    auto res = read(bundled_path);
    ASSERT_TRUE(res.ok()) << res.status();
    // Positive controls: the read converged on the authoritative file rather than the bundle's stale
    // copy, the gtid rejection and its drop really happened, and the fallback really opened the
    // per-tablet file -- so the flags below are that open's and not their initial values.
    EXPECT_EQ(kDurableGtid, res.value()->gtid());
    EXPECT_EQ(gtid_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    ASSERT_EQ(1, drop_attempts);
    ASSERT_EQ(1, opens);
    EXPECT_TRUE(skip_disk_cache);
    EXPECT_TRUE(skip_peer_cache);

    // 2. Nothing was cached locally, so there was nothing to drop. The re-read still has to bypass,
    //    and the peer bypass is the whole point here: a local cache that missed is why a peer BE
    //    supplied the stale copy to begin with.
    forced_drop_status = Status::NotFound("no local cache entry");
    res = read(bundled_path);
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(kDurableGtid, res.value()->gtid());
    EXPECT_EQ(gtid_before + 2, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    ASSERT_EQ(2, drop_attempts);
    ASSERT_EQ(2, opens);
    EXPECT_TRUE(skip_disk_cache);
    EXPECT_TRUE(skip_peer_cache);

    // 3. Same fallback, reached because the tablet is absent from the bundle rather than rejected on
    //    its gtid. Nothing was rejected, so the caches stay in play -- including for a caller that
    //    did pass an expected gtid. The unchanged counters are what prove no rejection happened.
    res = read(unbundled_path);
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(kDurableGtid, res.value()->gtid());
    EXPECT_EQ(gtid_before + 2, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    EXPECT_EQ(2, drop_attempts);
    ASSERT_EQ(3, opens);
    EXPECT_FALSE(skip_disk_cache);
    EXPECT_FALSE(skip_peer_cache);
}

// The bytes a GTID check rejects come out of the bundle file, so the bundle is what has to be
// re-read from authoritative storage. Re-reading the per-tablet file instead would resolve nothing
// in the one configuration that produces bundle hits at all: an aggregate publish writes only the
// bundle (lake_service.cpp derives skip_write_tablet_metadata from enable_aggregate_publish, and
// transactions.cpp then skips put_tablet_metadata), so there is no per-tablet file to re-read.
// This test builds exactly that shape -- a bundle and no per-tablet file.
TEST_F(LakeTabletManagerTest, gtid_mismatch_rereads_bundle_bypassing_caches) {
    constexpr int64_t kStaleGtid = 2000;
    constexpr int64_t kDurableGtid = 2001;
    constexpr int64_t kSchemaId = 10;

    TabletSchemaPB schema_pb;
    schema_pb.set_id(kSchemaId);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto tablet_id = next_id();
    auto write_bundle = [&](int64_t gtid) {
        starrocks::TabletMetadataPB metadata;
        metadata.set_id(tablet_id);
        metadata.set_version(2);
        metadata.set_gtid(gtid);
        metadata.mutable_schema()->CopyFrom(schema_pb);
        (*metadata.mutable_historical_schemas())[kSchemaId].CopyFrom(schema_pb);
        std::map<int64_t, TabletMetadataPB> bundle;
        bundle.emplace(tablet_id, std::move(metadata));
        return _tablet_manager->put_bundle_tablet_metadata(bundle);
    };
    // Only the bundle is written -- deliberately no put_tablet_metadata, so there is no per-tablet
    // file for anything to fall back to.
    ASSERT_OK(write_bundle(kStaleGtid));

    int first_opens = 0;
    int retry_opens = 0;
    const void* first_opts_addr = nullptr;
    const void* retry_opts_addr = nullptr;
    bool first_skip_disk_cache = true;
    bool retry_skip_disk_cache = false;
    bool retry_skip_peer_cache = false;
    bool heal_on_retry = false;

    int stale_bundle_drops = 0;
    int drop_outcomes = 0;
    bool fail_the_drop = false;
    std::string dropped_path;

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::metadata_open_options");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::gtid_retry_open_options");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::gtid_stale_bundle_drop");
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::gtid_stale_bundle_drop_status");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::gtid_stale_bundle_drop", [&](void* arg) {
        stale_bundle_drops++;
        dropped_path = *static_cast<std::string*>(arg);
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::gtid_stale_bundle_drop_status", [&](void* arg) {
        drop_outcomes++;
        if (fail_the_drop) {
            *static_cast<Status*>(arg) = Status::IOError("injected drop failure");
        }
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::metadata_open_options", [&](void* arg) {
        first_opens++;
        first_opts_addr = arg;
        first_skip_disk_cache = static_cast<RandomAccessFileOptions*>(arg)->skip_disk_cache;
    });
    SyncPoint::GetInstance()->SetCallBack("TabletManager::gtid_retry_open_options", [&](void* arg) {
        retry_opens++;
        retry_opts_addr = arg;
        const auto* o = static_cast<RandomAccessFileOptions*>(arg);
        retry_skip_disk_cache = o->skip_disk_cache;
        retry_skip_peer_cache = o->skip_peer_cache;
        if (heal_on_retry) {
            // Model authoritative storage holding the current bundle while the first read was served
            // the stale one from the local cache or a peer: rewrite the file just before the
            // bypassing re-read opens it. The re-read is outside the singleflight, so writing here
            // is safe.
            EXPECT_OK(write_bundle(kDurableGtid));
        }
    });

    auto* metrics = StorageMetrics::instance();
    const int64_t gtid_before = metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value();
    const int64_t attempts_before = metrics->self_heal_reread_attempts_total.value();
    const int64_t successes_before = metrics->self_heal_reread_success_total.value();
    auto path = _tablet_manager->tablet_metadata_location(tablet_id, 2);
    auto read = [&]() {
        _tablet_manager->metacache()->erase(path);
        return _tablet_manager->get_tablet_metadata(path, lake::CacheOptions{}, kDurableGtid);
    };

    // 1. Authoritative storage agrees the bundle is at that gtid, so the rejection stands. There is
    //    no per-tablet file, so the whole read reports NotFound rather than converging.
    auto res = read();
    EXPECT_TRUE(res.status().is_not_found()) << res.status();
    // Positive controls: the re-read really happened -- it only runs on a mismatch -- and it carried
    // both bypasses on an options object of its own, not by mutating the first read's, which the
    // singleflight may share with coalesced callers.
    ASSERT_EQ(1, retry_opens);
    EXPECT_TRUE(retry_skip_disk_cache);
    EXPECT_TRUE(retry_skip_peer_cache);
    ASSERT_EQ(1, first_opens);
    EXPECT_FALSE(first_skip_disk_cache);
    EXPECT_NE(first_opts_addr, retry_opts_addr);
    EXPECT_EQ(gtid_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    // The re-read was issued but did not recover anything, so only the attempt counts. Deltas,
    // because these counters are process-global.
    EXPECT_EQ(attempts_before + 1, metrics->self_heal_reread_attempts_total.value());
    EXPECT_EQ(successes_before, metrics->self_heal_reread_success_total.value());
    // The bundle agreed with authoritative storage, so its own entry is sound and must be left
    // alone. The drop recorded above is the per-tablet file's, for the mixed-layout fallback.
    EXPECT_EQ(0, stale_bundle_drops);

    // 2. Authoritative storage has the up-to-date bundle, so the bypassing re-read resolves the
    //    mismatch and the read converges -- with no per-tablet file involved anywhere.
    heal_on_retry = true;
    res = read();
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(kDurableGtid, res.value()->gtid());
    EXPECT_EQ(tablet_id, res.value()->id());
    ASSERT_EQ(2, retry_opens);
    EXPECT_TRUE(retry_skip_disk_cache);
    EXPECT_TRUE(retry_skip_peer_cache);
    EXPECT_EQ(attempts_before + 2, metrics->self_heal_reread_attempts_total.value());
    EXPECT_EQ(successes_before + 1, metrics->self_heal_reread_success_total.value());
    // The re-read disagreeing with the first read is proof the copy that was served -- from the
    // local disk cache or a peer -- is stale, so the bundle's own entry has to go. Without this the
    // next reader of any tablet in the same bundle is handed the same stale bytes, and one that
    // passes no expected gtid has nothing to reject them with. The path is asserted because
    // dropping the per-tablet file here instead would leave the offending entry in place.
    ASSERT_EQ(1, stale_bundle_drops);
    EXPECT_EQ(_tablet_manager->bundle_tablet_metadata_location(tablet_id, 2), dropped_path);
    // The terminal branch was never reached, so this drop is the healed branch's, not its.
    EXPECT_EQ(gtid_before + 2, metrics->invalidating_cache_drops(CacheDropSite::kGtidMismatch).value());
    ASSERT_EQ(1, drop_outcomes);

    // 3. The invalidation fails outright. The read has already converged by that point, so it must
    //    still return the authoritative metadata: failing a healed read over a cache eviction would
    //    be a regression. Swallowing the error is deliberate, and this is what pins it.
    ASSERT_OK(write_bundle(kStaleGtid)); // make the first read stale again
    fail_the_drop = true;
    res = read();
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(kDurableGtid, res.value()->gtid());
    ASSERT_EQ(3, retry_opens);
    ASSERT_EQ(2, stale_bundle_drops);
    ASSERT_EQ(2, drop_outcomes); // positive control: the injected failure was actually reached
}

TEST_F(LakeTabletManagerTest, parse_bundle_tablet_metadata_with_zero_size) {
    // Create a corrupted bundle metadata file with bundle_metadata_size = 0
    std::string serialized_string;
    serialized_string.resize(sizeof(uint64_t));
    // Set bundle_metadata_size to 0 in the footer
    encode_fixed64_le((uint8_t*)serialized_string.data(), 0);

    auto res = starrocks::lake::TabletManager::parse_bundle_tablet_metadata("test_path", serialized_string);
    EXPECT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_corruption());
}

// The bundle file is the complete metadata of one version and is written with truncate semantics,
// so persisting a map that is short a tablet corrupts that version permanently: FE makes it
// visible, and every later publish must read exactly that version as its base, so no retry can
// ever repair it. Refuse the write instead; the version stays unpublished and the publish is
// retryable.
TEST_F(LakeTabletManagerTest, put_bundle_tablet_metadata_rejects_incomplete_bundle) {
    TabletSchemaPB schema_pb;
    schema_pb.set_id(10);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    const int64_t t1 = next_id();
    const int64_t t2 = next_id();
    const int64_t t3 = next_id();
    const std::set<int64_t> expected{t1, t2, t3};

    auto make_meta = [&](int64_t tablet_id) {
        TabletMetadataPB meta;
        meta.set_id(tablet_id);
        meta.set_version(2);
        meta.mutable_schema()->CopyFrom(schema_pb);
        return meta;
    };
    auto fs = FileSystem::Default();
    // All three ids live in the same partition directory, so any of them names the same bundle file.
    auto bundle_path = _tablet_manager->bundle_tablet_metadata_location(t1, 2);

    // Short by one tablet: rejected, and nothing is left on disk.
    {
        std::map<int64_t, TabletMetadataPB> metadatas;
        metadatas.emplace(t1, make_meta(t1));
        metadatas.emplace(t2, make_meta(t2));
        auto st = _tablet_manager->put_bundle_tablet_metadata(metadatas, expected);
        ASSERT_FALSE(st.ok());
        EXPECT_TRUE(MatchPattern(st.message(), fmt::format("*missing 1 of 3 tablets*{}*", t3))) << st.message();
        EXPECT_TRUE(fs->path_exists(bundle_path).is_not_found());
    }

    // An empty expected set skips the check: callers that cannot state the tablet set up front still
    // get their bundle written.
    {
        std::map<int64_t, TabletMetadataPB> metadatas;
        metadatas.emplace(t1, make_meta(t1));
        ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));
        EXPECT_TRUE(fs->path_exists(bundle_path).ok());
        ASSERT_OK(fs->delete_file(bundle_path));
        _tablet_manager->metacache()->prune();
    }

    // Complete: written, and every expected tablet is readable at that version.
    {
        std::map<int64_t, TabletMetadataPB> metadatas;
        for (int64_t tablet_id : expected) {
            metadatas.emplace(tablet_id, make_meta(tablet_id));
        }
        ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas, expected));
        EXPECT_TRUE(fs->path_exists(bundle_path).ok());
        for (int64_t tablet_id : expected) {
            ASSIGN_OR_ABORT(auto metadata, _tablet_manager->get_single_tablet_metadata(tablet_id, 2));
            EXPECT_EQ(tablet_id, metadata->id());
            EXPECT_EQ(2, metadata->version());
        }
    }
}

// Regression for the aggregate-publish data-loss bug: an old worker sends a rowset whose segment_metas
// lack filename, with the real names only in deprecated_segments. put_bundle_tablet_metadata must
// persist metadata whose segment filename survives (reproduces old-worker -> new-aggregator).
TEST_F(LakeTabletManagerTest, put_bundle_preserves_legacy_segment_filenames) {
    auto tablet_id = next_id();
    std::map<int64_t, TabletMetadataPB> metadatas;
    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    {
        auto* schema = metadata.mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    // Legacy-shaped rowset: segment_metas carries only sort-key fields (no filename); the real name
    // lives only in deprecated_segments.
    auto* rs = metadata.add_rowsets();
    rs->set_id(2);
    rs->set_overlapped(false);
    rs->set_num_rows(5);
    rs->add_segment_metas()->set_num_rows(5);
    rs->add_deprecated_segments("real_seg.dat");
    metadatas.emplace(tablet_id, metadata);

    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto got = std::move(res).value();
    ASSERT_EQ(1, got->rowsets_size());
    ASSERT_EQ(1, got->rowsets(0).segment_metas_size());
    EXPECT_EQ("real_seg.dat", got->rowsets(0).segment_metas(0).filename());
}

// Layer C (after_load extend) on the aggregate receive path: a sparse legacy rowset
// (segment_metas_size() < deprecated_segments_size()) must keep ALL segment names. Without the
// after_load in put_bundle_tablet_metadata, the no-extend before_save would truncate the tail.
TEST_F(LakeTabletManagerTest, put_bundle_extends_then_preserves_mismatched_legacy_counts) {
    auto tablet_id = next_id();
    std::map<int64_t, TabletMetadataPB> metadatas;
    TabletMetadataPB metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    {
        auto* schema = metadata.mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto* rs = metadata.add_rowsets();
    rs->set_id(2);
    rs->set_overlapped(false);
    rs->set_num_rows(10);
    rs->add_segment_metas()->set_num_rows(10); // only 1 segment_metas...
    rs->add_deprecated_segments("s0.dat");     // ...but 2 real segment names
    rs->add_deprecated_segments("s1.dat");
    metadatas.emplace(tablet_id, metadata);

    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto got = std::move(res).value();
    ASSERT_EQ(1, got->rowsets_size());
    ASSERT_EQ(2, got->rowsets(0).segment_metas_size());
    EXPECT_EQ("s0.dat", got->rowsets(0).segment_metas(0).filename());
    EXPECT_EQ("s1.dat", got->rowsets(0).segment_metas(1).filename());
}

TEST_F(LakeTabletManagerTest, get_single_tablet_metadata_parse_failure) {
    // Pin the write format rather than inheriting it: the corruption below targets the footer's
    // crc32 field, which only exists in the checksummed layout.
    auto old_checksum_flag = config::lake_enable_protobuf_file_checksum;
    DeferOp restore_checksum_flag(
            [old_checksum_flag]() { config::lake_enable_protobuf_file_checksum = old_checksum_flag; });
    config::lake_enable_protobuf_file_checksum = true;

    // First, create a valid bundle metadata to get the file path
    auto tablet_id = next_id();
    std::map<int64_t, TabletMetadataPB> metadatas;
    TabletSchemaPB schema_pb;
    {
        schema_pb.set_id(10);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_rows_per_row_block(65535);
        auto c0 = schema_pb.add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }

    starrocks::TabletMetadataPB metadata1;
    {
        metadata1.set_id(tablet_id);
        metadata1.set_version(2);
        metadata1.mutable_schema()->CopyFrom(schema_pb);
        auto& item1 = (*metadata1.mutable_historical_schemas())[10];
        item1.CopyFrom(schema_pb);
    }

    metadatas.emplace(tablet_id, metadata1);
    ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadatas));

    // Read the bundle file and corrupt it
    auto bundle_path = _tablet_manager->bundle_tablet_metadata_location(tablet_id, 2);
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(bundle_path));
    ASSIGN_OR_ABORT(auto content, read_file->read_all());

    // Corrupt the bundle header so the whole file fails to parse.
    //
    // The footer is [bundle_meta][crc32(4)][size64(8)], so size-12 is the first byte of the stored
    // crc32 and flipping it always fails the header check. It has to be addressed from the end:
    // this bundle holds one tablet, whose page is written with the schema stripped, so the page is
    // 4 bytes while `tablet_id` fits in a one-byte varint and 5 bytes once it does not. `next_id()`
    // is a process-global counter, so an offset measured from the *start* of the file selects the
    // bundle header or a tablet page depending on how many tests ran first -- which is two
    // different code paths, two different drop buckets, and a test that passes alone and fails in
    // the whole binary.
    ASSERT_GT(content.size(), sizeof(uint64_t) + sizeof(uint32_t));
    content[content.size() - sizeof(uint64_t) - sizeof(uint32_t)] ^= 0xFF;

    // Write the corrupted content back
    ASSERT_OK(fs->delete_file(bundle_path));
    ASSIGN_OR_ABORT(auto write_file, fs->new_writable_file(bundle_path));
    ASSERT_OK(write_file->append(content));
    ASSERT_OK(write_file->close());

    // Clear cache to force reload from disk
    _tablet_manager->metacache()->prune();

    // Use sync point to mock corrupted_tablet_meta_handler
    SyncPoint::GetInstance()->EnableProcessing();
    bool handler_called = false;
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler", [&](void* arg) {
        handler_called = true;
        // Return OK to allow the code to continue, but the parse will still fail
        *(Status*)arg = Status::OK();
    });

    auto* metrics = StorageMetrics::instance();
    const int64_t other_before = metrics->invalidating_cache_drops(CacheDropSite::kOther).value();
    const int64_t bundle_page_before = metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value();
    const int64_t attempts_before = metrics->self_heal_reread_attempts_total.value();
    const int64_t successes_before = metrics->self_heal_reread_success_total.value();

    // Try to get metadata - should fail with corruption error
    auto res = _tablet_manager->get_tablet_metadata(tablet_id, 2);

    // Should fail due to parse error
    EXPECT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_corruption());
    EXPECT_TRUE(handler_called) << "corrupted_tablet_meta_handler should have been called";

    // A bundle header that fails its crc is category-(a): the bundle is dropped as `other` and
    // re-read. This is the corruption side of the pairing `self_heal_reread_success_total` exists to
    // expose -- a re-read that then *fails* -- so pin it here: one attempt, no success. The re-read
    // pulls the same bytes off disk, so it cannot succeed. (The GTID side of the same pairing is
    // covered by gtid_mismatch_rereads_bundle_bypassing_caches.) `bundle_page` is
    // asserted unchanged to keep the two paths apart -- corrupting a single tablet page, leaving
    // the header intact, is what the checksum test below does.
    EXPECT_EQ(other_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kOther).value());
    EXPECT_EQ(bundle_page_before, metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value());
    EXPECT_EQ(attempts_before + 1, metrics->self_heal_reread_attempts_total.value());
    EXPECT_EQ(successes_before, metrics->self_heal_reread_success_total.value());
}

// Bundle tablet metadata checksum: the footer carries a crc32 over the bundle header plus a
// magic so the read path can tell the new checksummed layout from the legacy one. Verifies:
//   - new format (flag on): footer magic present, header crc + per-page checksums populated, reads OK;
//   - old format (flag off): no footer magic, read path detects legacy layout and reads OK with flag on;
//   - corrupting the bundle header is caught by the footer crc;
//   - corrupting a tablet page is caught by the per-page checksum.
TEST_F(LakeTabletManagerTest, put_bundle_tablet_metadata_checksum) {
    auto old_flag = config::lake_enable_protobuf_file_checksum;
    DeferOp guard([old_flag]() { config::lake_enable_protobuf_file_checksum = old_flag; });

    auto build_meta = [](int64_t tid) {
        TabletSchemaPB schema_pb;
        schema_pb.set_id(10);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_rows_per_row_block(65535);
        auto* c0 = schema_pb.add_column();
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        TabletMetadataPB m;
        m.set_id(tid);
        m.set_version(2);
        m.mutable_schema()->CopyFrom(schema_pb);
        (*m.mutable_historical_schemas())[10].CopyFrom(schema_pb);
        return m;
    };

    auto fs = FileSystem::Default();
    auto write_bundle = [&](int64_t tid) {
        std::map<int64_t, TabletMetadataPB> metadatas;
        metadatas.emplace(tid, build_meta(tid));
        return _tablet_manager->put_bundle_tablet_metadata(metadatas);
    };
    // The checksummed layout is marked by the high bit of the trailing 8-byte size field.
    auto footer_has_checksum = [&](const std::string& p) -> bool {
        auto rf = *fs->new_random_access_file(p);
        auto content = *rf->read_all();
        uint64_t raw = decode_fixed64_le((const uint8_t*)(content.data() + content.size() - sizeof(uint64_t)));
        return (raw & LAKE_BUNDLE_META_CHECKSUM_FLAG) != 0;
    };
    auto overwrite = [&](const std::string& p, const std::string& content) {
        ASSERT_OK(fs->delete_file(p));
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(p));
        ASSERT_OK(wf->append(content));
        ASSERT_OK(wf->close());
    };

    // ---- NEW format (flag on): footer magic present, header crc + per-page checksums written ----
    config::lake_enable_protobuf_file_checksum = true;
    auto new_id = next_id();
    ASSERT_OK(write_bundle(new_id));
    auto new_path = _tablet_manager->bundle_tablet_metadata_location(new_id, 2);
    EXPECT_TRUE(footer_has_checksum(new_path));
    {
        ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file(new_path));
        ASSIGN_OR_ABORT(auto content, rf->read_all());
        ASSIGN_OR_ABORT(auto bundle, starrocks::lake::TabletManager::parse_bundle_tablet_metadata(new_path, content));
        EXPECT_EQ(bundle->tablet_meta_page_checksum_size(), 1);
        EXPECT_EQ(bundle->tablet_meta_page_checksum().count(new_id), 1);
    }
    _tablet_manager->metacache()->prune();
    {
        auto res = _tablet_manager->get_tablet_metadata(new_id, 2);
        ASSERT_OK(res.status());
        EXPECT_EQ(res.value()->id(), new_id);
        EXPECT_EQ(res.value()->schema().id(), 10);
    }

    // ---- OLD format (flag off): no footer magic; the read path detects the legacy layout ----
    config::lake_enable_protobuf_file_checksum = false;
    auto old_id = next_id();
    ASSERT_OK(write_bundle(old_id));
    auto old_path = _tablet_manager->bundle_tablet_metadata_location(old_id, 2);
    EXPECT_FALSE(footer_has_checksum(old_path));
    {
        ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file(old_path));
        ASSIGN_OR_ABORT(auto content, rf->read_all());
        ASSIGN_OR_ABORT(auto bundle, starrocks::lake::TabletManager::parse_bundle_tablet_metadata(old_path, content));
        EXPECT_EQ(bundle->tablet_meta_page_checksum_size(), 0);
    }
    // Read the legacy-format bundle with the flag ON: magic absent -> legacy layout -> read OK.
    config::lake_enable_protobuf_file_checksum = true;
    _tablet_manager->metacache()->prune();
    {
        auto res = _tablet_manager->get_tablet_metadata(old_id, 2);
        ASSERT_OK(res.status());
        EXPECT_EQ(res.value()->id(), old_id);
        EXPECT_EQ(res.value()->schema().id(), 10);
    }

    // ---- corrupting the checksummed bundle is caught by the footer crc ----
    {
        ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file(new_path));
        ASSIGN_OR_ABORT(auto content, rf->read_all());
        ASSERT_GT(content.size(), sizeof(uint64_t) + sizeof(uint32_t));
        // Footer is [bundle_meta][crc32][size64|flag]. Flip a byte in the stored crc32 field (the
        // 4 bytes immediately before the trailing 8-byte size) so it no longer matches the bundle
        // metadata. This is a fixed position independent of the bundle metadata size.
        size_t crc_off = content.size() - sizeof(uint64_t) - sizeof(uint32_t);
        content[crc_off] ^= 0xFF;
        // Direct parse must reject the tampered bundle. The reported error may be the checksum
        // mismatch or a downstream parse failure depending on where the corruption lands relative
        // to the (small) bundle metadata; the contract verified here is that it is rejected.
        auto parsed = starrocks::lake::TabletManager::parse_bundle_tablet_metadata(new_path, content);
        EXPECT_TRUE(parsed.status().is_corruption()) << parsed.status();
        overwrite(new_path, content);
    }
    _tablet_manager->metacache()->prune();
    {
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp d([]() {
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });
        auto res = _tablet_manager->get_tablet_metadata(new_id, 2);
        EXPECT_FALSE(res.ok());
        EXPECT_TRUE(res.status().is_corruption()) << res.status();
    }

    // ---- corrupting a tablet page (new format) is caught by the per-page checksum ----
    config::lake_enable_protobuf_file_checksum = true;
    auto page_id = next_id();
    ASSERT_OK(write_bundle(page_id));
    auto page_path = _tablet_manager->bundle_tablet_metadata_location(page_id, 2);
    {
        ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file(page_path));
        ASSIGN_OR_ABORT(auto content, rf->read_all());
        content[0] ^= 0xFF; // flip a byte in the first tablet page (footer/header intact)
        overwrite(page_path, content);
    }
    _tablet_manager->metacache()->prune();
    {
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp d([]() {
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::corrupted_tablet_meta_handler");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::corrupted_tablet_meta_handler",
                                              [](void* arg) { *(Status*)arg = Status::OK(); });

        // Deltas taken here rather than at the top of the test: the footer-crc block above already
        // moved these counters, so only the page corruption below may move them again.
        auto* metrics = StorageMetrics::instance();
        const int64_t bundle_page_before = metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value();
        const int64_t other_before = metrics->invalidating_cache_drops(CacheDropSite::kOther).value();
        const int64_t attempts_before = metrics->self_heal_reread_attempts_total.value();

        auto res = _tablet_manager->get_tablet_metadata(page_id, 2);
        EXPECT_FALSE(res.ok());
        EXPECT_TRUE(res.status().is_corruption()) << res.status();
        EXPECT_THAT(res.status().to_string(), ::testing::HasSubstr("checksum"));

        // This is the only positive pin on the `bundle_page` bucket. A single flipped byte leaves
        // the bundle header intact, so the failure is the per-page checksum and the drop is
        // drop-only: no re-read follows. The `other` assertion is what disambiguates it from the
        // footer-crc failure above, which lands in `other` with a re-read -- the substring check
        // cannot tell them apart, because both messages say "checksum".
        EXPECT_EQ(bundle_page_before + 1, metrics->invalidating_cache_drops(CacheDropSite::kBundlePage).value());
        EXPECT_EQ(other_before, metrics->invalidating_cache_drops(CacheDropSite::kOther).value());
        EXPECT_EQ(attempts_before, metrics->self_heal_reread_attempts_total.value());
    }
}

namespace {
class PartitionedLocationProvider : public lake::LocationProvider {
public:
    PartitionedLocationProvider(std::string root_dir, int num_partition)
            : _root_dir(std::move(root_dir)), _num_partition(num_partition) {
        for (int i = 0; i < _num_partition; i++) {
            auto dir = lake::join_path(_root_dir, std::to_string(i));
            CHECK_OK(fs::create_directories(lake::join_path(dir, lake::kMetadataDirectoryName)));
        }
    }

    std::string root_location(int64_t tablet_id) const override {
        return lake::join_path(_root_dir, std::to_string(tablet_id % _num_partition));
    }

private:
    std::string _root_dir;
    const int _num_partition;
};

TCreateTabletReq build_create_tablet_request(int64_t tablet_id, int64_t index_id) {
    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.tablet_schema.__set_id(index_id);
    req.tablet_schema.__set_schema_hash(0);
    req.tablet_schema.__set_short_key_column_count(1);
    req.tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

    auto& c0 = req.tablet_schema.columns.emplace_back();
    c0.column_name = "c0";
    c0.is_key = true;
    c0.column_type.type = TPrimitiveType::BIGINT;
    c0.is_allow_null = false;

    return req;
}

} // namespace

// NOLINTNEXTLINE
TEST_F(LakeTabletManagerTest, test_multi_partition_schema_file) {
    const static int kNumPartition = 4;
    const static int64_t kIndexId = 123454321;
    auto lp = std::make_shared<PartitionedLocationProvider>(_test_dir, kNumPartition);
    _tablet_manager->TEST_set_location_provider(lp);
    for (int i = 0; i < 10; i++) {
        auto req = build_create_tablet_request(next_id(), kIndexId);
        ASSERT_OK(_tablet_manager->create_tablet(req));
    }
    for (int i = 0; i < kNumPartition; i++) {
        auto partition_dir = lake::join_path(_test_dir, std::to_string(i));
        auto schema_file_path = lake::join_path(partition_dir, lake::schema_filename(kIndexId));
        EXPECT_TRUE(fs::path_exist(schema_file_path)) << schema_file_path;
    }
}

TEST_F(LakeTabletManagerTest, test_get_schema_file_concurrently) {
    auto tablet_id = next_id();
    auto schema_id = next_id();
    auto req = build_create_tablet_request(tablet_id, schema_id);
    ASSERT_OK(_tablet_manager->create_tablet(req));
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto schema, tablet.get_schema_by_id(schema_id));

    auto fn_read_schema = [&]() {
        for (int i = 0; i < 50; i++) {
            ASSIGN_OR_ABORT(auto real_schema, tablet.get_schema_by_id(schema_id));
            EXPECT_EQ(*schema, *real_schema);
            _tablet_manager->metacache()->prune();
        }
    };

    auto pthreads = std::vector<std::thread>{};
    pthreads.reserve(10);
    for (int i = 0; i < 10; i++) {
        pthreads.emplace_back(fn_read_schema);
    }

    auto bthreads = std::vector<bthread_t>{};
    bthreads.reserve(10);
    for (int i = 0; i < 10; i++) {
        ASSIGN_OR_ABORT(auto bid, bthreads::start_bthread(fn_read_schema));
        bthreads.emplace_back(bid);
    }

    for (auto&& t : pthreads) {
        t.join();
    }
    for (auto&& t : bthreads) {
        bthread_join(t, nullptr);
    }
}

// Bundle-format segments let multiple rowsets point at the same physical file at disjoint
// byte ranges. Keying the segment cache on path alone would return the first-loaded slice to
// every later caller, silently replacing their data. The key must include the bundle offset.
TEST_F(LakeTabletManagerTest, segment_cache_key_distinguishes_bundle_slices) {
    FileInfo plain{.path = "oss://bucket/data/abc.dat"};
    FileInfo slice0{.path = "oss://bucket/data/abc.dat"};
    FileInfo slice1{.path = "oss://bucket/data/abc.dat"};
    slice0.bundle_file_offset = 0;
    slice1.bundle_file_offset = 10704;

    const auto plain_key = plain.cache_key();
    const auto slice0_key = slice0.cache_key();
    const auto slice1_key = slice1.cache_key();

    // offset absent or zero collapses to the legacy path-only key so the cache layout for
    // non-bundled segments is unchanged.
    EXPECT_EQ(plain.path, plain_key);
    EXPECT_EQ(plain_key, slice0_key);

    // A non-zero bundle offset must fork off its own key; otherwise slice1 would collide with
    // slice0 and receive slice0's Segment on cache hit.
    EXPECT_NE(plain_key, slice1_key);
    EXPECT_THAT(slice1_key, testing::HasSubstr("#10704"));
}

TEST_F(LakeTabletManagerTest, load_segment_returns_distinct_slices_for_same_path) {
    // Construct a real file that encodes two back-to-back segments (a common shape produced
    // by cross-boundary UPSERTs written through BundleWritableFile). We cannot call the full
    // load_segment+open() path here without a SegmentWriter scaffold — Segment::open parses the
    // bundle slice's footer — so the cheapest correctness check is that load_segment routes
    // two different bundle offsets to distinct metacache entries. That is the specific
    // invariant the cache-key fix protects.
    const std::string path = _location_provider->segment_location(1, "segment_slice_test.dat");
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(path));
    const std::string payload_a(1024, 'A');
    const std::string payload_b(2048, 'B');
    {
        WritableFileOptions opts;
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(opts, path));
        ASSERT_OK(of->append(payload_a));
        ASSERT_OK(of->append(payload_b));
        ASSERT_OK(of->close());
    }

    FileInfo info_a{.path = path};
    info_a.size = payload_a.size();
    info_a.bundle_file_offset = 0;

    FileInfo info_b{.path = path};
    info_b.size = payload_b.size();
    info_b.bundle_file_offset = static_cast<int64_t>(payload_a.size());

    const auto key_a = info_a.cache_key();
    const auto key_b = info_b.cache_key();
    ASSERT_NE(key_a, key_b);

    // Seed the metacache with a marker for key_a, prove a lookup by key_b does NOT find it.
    // This mirrors the failure mode pre-fix: lookup by path-only would collide.
    auto marker = std::make_shared<Segment>(fs, info_a, /*segment_id*/ 0, nullptr, _tablet_manager);
    auto cached = _tablet_manager->metacache()->cache_segment_if_absent(key_a, marker);
    ASSERT_NE(cached, nullptr);
    EXPECT_EQ(_tablet_manager->metacache()->lookup_segment(key_a).get(), marker.get());
    EXPECT_EQ(_tablet_manager->metacache()->lookup_segment(key_b), nullptr);

    ASSERT_OK(fs->delete_file(path));
}

#ifdef USE_STAROS
class MockStarOSWorker : public StarOSWorker {
public:
    MOCK_METHOD((absl::StatusOr<staros::starlet::ShardInfo>), _fetch_shard_info_from_remote,
                (staros::starlet::ShardId id));
};

TEST_F(LakeTabletManagerTest, tablet_schema_load_from_remote) {
    auto tablet_id = next_id();
    int64_t schema_id = 10086;

    TabletSchemaPB schema_pb;
    schema_pb.set_id(10);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_rows_per_row_block(65535);
    auto c0 = schema_pb.add_column();
    {
        c0->set_unique_id(0);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema_pb.add_column();
    {
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
    }
    // prepare and set schema info in global_schema_cache
    auto schema_ptr = TabletSchema::create(schema_pb);
    _tablet_manager->TEST_set_global_schema_cache(schema_id, schema_ptr);

    // prepare fake shard_info returned by mocked `_fetch_shard_info_from_remote()`
    staros::starlet::ShardInfo shard_info;
    shard_info.id = tablet_id;
    shard_info.properties.emplace("indexId", std::to_string(schema_id));

    // Preserve original worker value, and reset it to our mocked worker.
    std::shared_ptr<StarOSWorker> origin_worker = get_staros_worker();
    set_staros_worker_for_test(std::make_shared<MockStarOSWorker>());
    DeferOp op([origin_worker] { set_staros_worker_for_test(origin_worker); });

    // set mock function excepted call
    MockStarOSWorker* worker = dynamic_cast<MockStarOSWorker*>(get_staros_worker().get());
    EXPECT_CALL(*worker, _fetch_shard_info_from_remote(tablet_id)).WillOnce(::testing::Return(shard_info));

    // fire the testing
    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
    auto st = tablet.get_schema();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(st.value()->id(), 10);
    EXPECT_EQ(st.value()->num_columns(), 2);
    EXPECT_EQ(st.value()->column(0).name(), "c0");
    EXPECT_EQ(st.value()->column(1).name(), "c1");
}

TEST_F(LakeTabletManagerTest, test_in_writing_data_size) {
    ASSERT_EQ(_tablet_manager->in_writing_data_size(1), 0);
    _tablet_manager->add_in_writing_data_size(1, 100);

    _tablet_manager->add_in_writing_data_size(1, 100);
    _tablet_manager->clean_in_writing_data_size();
    ASSERT_EQ(_tablet_manager->in_writing_data_size(1), 200);

    // Preserve original worker value, and reset it to our mocked worker.
    std::shared_ptr<StarOSWorker> origin_worker = get_staros_worker();
    set_staros_worker_for_test(std::make_shared<MockStarOSWorker>());
    DeferOp op([origin_worker] { set_staros_worker_for_test(origin_worker); });

    _tablet_manager->clean_in_writing_data_size();
    ASSERT_EQ(_tablet_manager->in_writing_data_size(1), 0);
}

TEST_F(LakeTabletManagerTest, test_in_writing_data_size_with_cached_metadata) {
    auto tablet_id = next_id();

    // Put a tablet metadata with known data_size into the cache
    starrocks::TabletMetadata metadata;
    metadata.set_id(tablet_id);
    metadata.set_version(2);
    auto* rowset1 = metadata.add_rowsets();
    rowset1->set_id(1);
    rowset1->set_data_size(500);
    rowset1->set_num_rows(10);
    auto* rowset2 = metadata.add_rowsets();
    rowset2->set_id(2);
    rowset2->set_data_size(300);
    rowset2->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    // First call to add_in_writing_data_size should pick up the cached
    // metadata data_size (500 + 300 = 800) as the base, rather than
    // triggering list_tablet_metadata (object storage LIST).
    auto result = _tablet_manager->add_in_writing_data_size(tablet_id, 100);
    EXPECT_EQ(result, 900); // base_size(800) + size(100)

    // Subsequent call should just accumulate
    result = _tablet_manager->add_in_writing_data_size(tablet_id, 200);
    EXPECT_EQ(result, 1100); // 900 + 200

    // in_writing_data_size should return the same accumulated value
    EXPECT_EQ(_tablet_manager->in_writing_data_size(tablet_id), 1100);
}

TEST_F(LakeTabletManagerTest, test_in_writing_data_size_without_cached_metadata) {
    auto tablet_id = next_id();
    // No metadata exists for this tablet_id.
    auto result = _tablet_manager->add_in_writing_data_size(tablet_id, 100);
    EXPECT_EQ(result, 100);
}

TEST_F(LakeTabletManagerTest, test_in_writing_data_size_cache_miss_fallback_list_configurable) {
    auto make_meta = [&](int64_t tablet_id) {
        starrocks::TabletMetadata metadata;
        metadata.set_id(tablet_id);
        metadata.set_version(2);
        auto* rowset1 = metadata.add_rowsets();
        rowset1->set_id(1);
        rowset1->set_data_size(500);
        rowset1->set_num_rows(10);
        auto* rowset2 = metadata.add_rowsets();
        rowset2->set_id(2);
        rowset2->set_data_size(300);
        rowset2->set_num_rows(5);
        return metadata;
    };

    auto put_meta_without_latest_cache = [&](const TabletMetadata& metadata) {
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp defer([]() {
            SyncPoint::GetInstance()->ClearCallBack("TabletManager::skip_cache_latest_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->SetCallBack("TabletManager::skip_cache_latest_metadata",
                                              [](void* arg) { *(bool*)arg = true; });
        EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));
        _tablet_manager->metacache()->prune();
    };

    bool old = config::allow_list_object_for_random_bucketing_on_cache_miss;
    DeferOp config_guard([old] { config::allow_list_object_for_random_bucketing_on_cache_miss = old; });

    // cache miss + LIST fallback enabled -> list metadata and recover base_size from meta files.
    {
        auto tablet_id = next_id();
        auto metadata = make_meta(tablet_id);
        put_meta_without_latest_cache(metadata);
        config::allow_list_object_for_random_bucketing_on_cache_miss = true;
        auto result = _tablet_manager->add_in_writing_data_size(tablet_id, 100);
        EXPECT_EQ(result, 900);
    }

    // cache miss + LIST fallback disabled -> skip LIST, base_size stays 0.
    {
        auto tablet_id = next_id();
        auto metadata = make_meta(tablet_id);
        put_meta_without_latest_cache(metadata);
        config::allow_list_object_for_random_bucketing_on_cache_miss = false;
        auto result = _tablet_manager->add_in_writing_data_size(tablet_id, 100);
        EXPECT_EQ(result, 100);
    }
}

TEST_F(LakeTabletManagerTest, test_get_output_rorwset_schema) {
    std::shared_ptr<TabletMetadata> tablet_metadata = lake::generate_simple_tablet_metadata(DUP_KEYS);
    for (int i = 0; i < 5; i++) {
        auto rs = tablet_metadata->add_rowsets();
        rs->set_id(next_id());
    }

    // set historical schema
    auto schema_id1 = next_id();
    auto& schema_pb1 = (*tablet_metadata->mutable_historical_schemas())[schema_id1];
    schema_pb1.set_id(schema_id1);
    schema_pb1.set_schema_version(0);

    auto schema_id2 = next_id();
    auto& schema_pb2 = (*tablet_metadata->mutable_historical_schemas())[schema_id2];
    schema_pb2.set_id(schema_id2);
    schema_pb2.set_schema_version(1);

    auto schema_id3 = tablet_metadata->schema().id();
    auto& schema_pb3 = (*tablet_metadata->mutable_historical_schemas())[schema_id3];
    schema_pb3.set_id(schema_id3);
    schema_pb3.set_schema_version(2);

    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(0).id()] = schema_id3;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(1).id()] = schema_id1;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(2).id()] = schema_id3;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(3).id()] = schema_id2;
    (*tablet_metadata->mutable_rowset_to_schema())[tablet_metadata->rowsets(4).id()] = schema_id3;
    lake::VersionedTablet tablet(_tablet_manager, tablet_metadata);

    {
        for (int i = 0; i < 5; i++) {
            std::vector<uint32_t> input_rowsets;
            input_rowsets.emplace_back(tablet_metadata->rowsets(i).id());
            auto res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
            ASSERT_TRUE(res.ok());
            auto schema_id = tablet_metadata->rowset_to_schema().at(tablet_metadata->rowsets(i).id());
            ASSERT_EQ(res.value()->id(), schema_id);
        }
    }

    auto rs1 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 0, 0 /* compaction_segment_limit */);
    auto rs2 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 1, 0 /* compaction_segment_limit */);
    auto rs3 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 2, 0 /* compaction_segment_limit */);
    auto rs4 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 3, 0 /* compaction_segment_limit */);
    auto rs5 = std::make_shared<lake::Rowset>(_tablet_manager, tablet_metadata, 4, 0 /* compaction_segment_limit */);

    {
        std::vector<uint32_t> input_rowsets;
        input_rowsets.emplace_back(tablet_metadata->rowsets(0).id());
        input_rowsets.emplace_back(tablet_metadata->rowsets(1).id());
        auto res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(res.value()->id(), schema_id3);

        input_rowsets.emplace_back(tablet_metadata->rowsets(2).id());
        res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(res.value()->id(), schema_id3);

        input_rowsets.clear();
        input_rowsets.emplace_back(tablet_metadata->rowsets(3).id());
        input_rowsets.emplace_back(tablet_metadata->rowsets(1).id());
        res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(res.value()->id(), schema_id2);
    }

    {
        tablet_metadata->mutable_rowset_to_schema()->clear();
        for (int i = 0; i < 5; i++) {
            std::vector<uint32_t> input_rowsets;
            input_rowsets.emplace_back(tablet_metadata->rowsets(i).id());
            auto res = _tablet_manager->get_output_rowset_schema(input_rowsets, tablet_metadata.get());
            ASSERT_TRUE(res.ok());
            ASSERT_EQ(res.value()->id(), tablet_metadata->schema().id());
        }
    }
}

TEST_F(LakeTabletManagerTest, capture_tablet_and_rowsets) {
    auto old_capture_tablet_and_rowsets_config = config::experimental_enable_lake_capture_tablet_and_rowsets;
    DeferOp defer([old_capture_tablet_and_rowsets_config]() {
        config::experimental_enable_lake_capture_tablet_and_rowsets = old_capture_tablet_and_rowsets_config;
    });
    config::experimental_enable_lake_capture_tablet_and_rowsets = true;

    starrocks::TabletMetadata metadata;
    auto schema = metadata.mutable_schema();
    schema->set_id(1);
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(1);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(2);
    auto rowset_meta_pb2 = metadata.add_rowsets();
    rowset_meta_pb2->set_id(2);
    rowset_meta_pb2->set_overlapped(false);
    rowset_meta_pb2->set_data_size(1024);
    rowset_meta_pb2->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(3);
    auto rowset_meta_pb3 = metadata.add_rowsets();
    rowset_meta_pb3->set_id(3);
    rowset_meta_pb3->set_overlapped(false);
    rowset_meta_pb3->set_data_size(1024);
    rowset_meta_pb3->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 0, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(2, rowsets.size());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 1, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(2, rowsets.size());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 2, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(2, rowsets.size());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 3, 3);
        EXPECT_TRUE(res.ok());
        auto& [tablet, rowsets] = res.value();
        ASSERT_EQ(1, rowsets.size());
    }
}

TEST_F(LakeTabletManagerTest, capture_tablet_and_rowsets_capture_delta_versions_disabled) {
    auto old_capture_tablet_and_rowsets_config = config::experimental_enable_lake_capture_tablet_and_rowsets;
    DeferOp defer([old_capture_tablet_and_rowsets_config]() {
        config::experimental_enable_lake_capture_tablet_and_rowsets = old_capture_tablet_and_rowsets_config;
    });
    config::experimental_enable_lake_capture_tablet_and_rowsets = false;

    starrocks::TabletMetadata metadata;
    auto schema = metadata.mutable_schema();
    schema->set_id(1);
    auto tablet_id = next_id();
    metadata.set_id(tablet_id);
    metadata.set_version(1);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(2);
    auto rowset_meta_pb2 = metadata.add_rowsets();
    rowset_meta_pb2->set_id(2);
    rowset_meta_pb2->set_overlapped(false);
    rowset_meta_pb2->set_data_size(1024);
    rowset_meta_pb2->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    metadata.set_version(3);
    auto rowset_meta_pb3 = metadata.add_rowsets();
    rowset_meta_pb3->set_id(3);
    rowset_meta_pb3->set_overlapped(false);
    rowset_meta_pb3->set_data_size(1024);
    rowset_meta_pb3->set_num_rows(5);
    EXPECT_OK(_tablet_manager->put_tablet_metadata(metadata));

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 1, 3);
        EXPECT_TRUE(res.status().is_not_supported());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 2, 3);
        EXPECT_TRUE(res.status().is_not_supported());
    }

    {
        auto res = _tablet_manager->capture_tablet_and_rowsets(tablet_id, 0, 3);
        EXPECT_TRUE(res.status().is_not_supported());
    }
}

// Verify that pick_local_anchor_tablet_id prefers a tablet id that this worker owns.
// In file-bundling mode the aggregator derives bundle/txn-log paths from a single
// tablet id of the batch; using a remote tablet id forces the staros worker to fetch
// shard info via RPC, so the helper must skip non-local ids and pick a local one.
TEST_F(LakeTabletManagerTest, pick_local_anchor_tablet_id_skips_non_local) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("is_tablet_in_worker:2");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // Mock: tablets 100 and 101 are NOT on this worker; 102 is.
    SyncPoint::GetInstance()->SetCallBack("is_tablet_in_worker:2", [](void* arg) {
        auto* p = static_cast<std::pair<int64_t, bool*>*>(arg);
        if (p->first == 100 || p->first == 101) {
            *(p->second) = false;
        }
    });

    std::vector<int64_t> candidates{100, 101, 102, 103};
    EXPECT_EQ(102, _tablet_manager->pick_local_anchor_tablet_id(candidates));
}

TEST_F(LakeTabletManagerTest, pick_local_anchor_tablet_id_falls_back_to_first_when_none_local) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("is_tablet_in_worker:2");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // Mock: no tablet is on this worker.
    SyncPoint::GetInstance()->SetCallBack("is_tablet_in_worker:2", [](void* arg) {
        auto* p = static_cast<std::pair<int64_t, bool*>*>(arg);
        *(p->second) = false;
    });

    std::vector<int64_t> candidates{200, 201, 202};
    // No local tablet available → the helper must still return a usable id (the first).
    EXPECT_EQ(200, _tablet_manager->pick_local_anchor_tablet_id(candidates));
}

TEST_F(LakeTabletManagerTest, pick_local_anchor_tablet_id_returns_first_when_all_local) {
    // Without any sync-point override, is_tablet_in_worker returns true by default in
    // unit tests (the StarOS worker is null), so the first candidate is picked immediately.
    std::vector<int64_t> candidates{300, 301, 302};
    EXPECT_EQ(300, _tablet_manager->pick_local_anchor_tablet_id(candidates));
}

#endif // USE_STAROS

} // namespace starrocks
