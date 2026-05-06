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
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

// Internal helpers for legacy shared PK sstable fast-path v2. The v2 design
// (see ~/workspace/doc/legacy_sstable_fastpath_v2.md) infers split families
// across merge_contexts, then uses the canonical child's rssid offset to
// project shared-ancestor rowsets and their referencing legacy sstables
// onto matching final rssids in the merged tablet — eliminating the partial-
// compaction fallback class that v1 hits ~50% of the time on real workloads.
//
// This commit adds the inference primitives only (commit 1 of the v2 PR
// plan). They are not consumed yet; commits 4-5 wire them through
// merge_rowsets / map_rssid / fast-path. Behavior change: zero.
namespace detail {

// Full physical identity for a rowset, used by family inference's rowset-
// identity edge to decide whether two rowsets in different merge_contexts
// are the same physical rowset (i.e. inherited from the same SPLIT). Must
// be an exact match — partial signals (first segment only) can falsely
// union unrelated rowsets across families.
struct RowsetPhysicalKey {
    int64_t version = 0;
    std::vector<std::string> segments;
    std::vector<int64_t> bundle_file_offsets;
    std::vector<bool> shared_segments_flags;
    std::vector<int32_t> segment_idx_layout;

    bool operator==(const RowsetPhysicalKey& other) const = default;
};

struct RowsetPhysicalKeyHash {
    size_t operator()(const RowsetPhysicalKey& key) const noexcept;
};

// Construct a RowsetPhysicalKey by copying every identifying field from a
// rowset's metadata.
RowsetPhysicalKey make_rowset_physical_key(const RowsetMetadataPB& rowset_meta);

// Returns true iff the rowset is a candidate for the rowset-identity edge:
// it has at least one segment, every segment is marked shared (inherited
// from SPLIT, not produced by post-split DML or compaction), and the
// shared_segments vector covers every segment position. Delete-only
// rowsets (no segments) and child-local rowsets are excluded.
bool is_shared_ancestor_rowset(const RowsetMetadataPB& rowset_meta);

// Pure-data input for split family inference. Decoupled from
// TabletMergeContext (which lives in tablet_merger.cpp's anonymous
// namespace and cannot cross the TU boundary), so tests can construct
// inputs directly without spinning up a full merge context.
struct SplitFamilyInferenceInput {
    TabletMetadataPtr metadata;
    // Already-computed rssid_offset for this child (Phase 1 of merge_tablet
    // sets ctx[i].rssid_offset before any merge_rowsets work; the inference
    // helper just records it on the canonical entry of each family).
    int64_t rssid_offset = 0;
};

// Inferred split families across a set of merge contexts. Each input
// (= one child of the merge) belongs to AT MOST one family (or kNoFamily
// for orphan / standalone children).
struct InferredSplitFamilies {
    static constexpr uint32_t kNoFamily = std::numeric_limits<uint32_t>::max();

    struct Family {
        // member child_indexes in ascending order. The smallest member is
        // the canonical child for this family.
        std::vector<uint32_t> member_child_indexes;
        // == member_child_indexes.front(). Stored explicitly so callers
        // don't have to peek into the vector.
        uint32_t canonical_child_index = 0;
        // == inputs[canonical_child_index].rssid_offset at the moment of
        // inference. Recorded once so subsequent consumers don't re-fetch
        // it from the merge contexts.
        int64_t canonical_rssid_offset = 0;
    };

    // child_index → family_id (kNoFamily for orphan).
    std::vector<uint32_t> child_to_family;
    // Indexed by family_id. Emitted in ascending canonical_child_index
    // order (so the iteration is deterministic and matches the dedup
    // order in merge_sstables).
    std::vector<Family> families;
};

// Infer split families from a vector of merge inputs. Edges:
//   (1) two children share a legacy `shared && !has_shared_rssid` sstable
//       filename (catches the case where rowset duplication is incomplete
//       across children but the shared sstable file is still common);
//   (2) two children carry a rowset that satisfies is_shared_ancestor_
//       rowset() AND has identical RowsetPhysicalKey. The shared-ancestor
//       filter excludes delete-only and child-local rowsets that could
//       otherwise produce false unions when their physical keys happen
//       to match.
//
// Children with no edges to any other child get kNoFamily. Family ids are
// assigned in ascending canonical_child_index order; canonical_child_index
// is always the smallest child_index in the family.
StatusOr<InferredSplitFamilies> infer_split_families(const std::vector<SplitFamilyInferenceInput>& inputs);

// Composite key for RssidProjectionPlan::explicit_rssid_map. The named
// fields prevent positional swaps and self-document each side of the
// pair (the prior std::pair<uint32_t, uint32_t> shape forced callers
// to read .first.first / .first.second pattern with no spelling).
struct SourceRssidKey {
    uint32_t child_index = 0;
    uint32_t source_rssid = 0;

    bool operator==(const SourceRssidKey& other) const = default;
};

// Folds the two uint32 halves of SourceRssidKey into a single uint64 so
// the hash uses both fields independently.
struct SourceRssidKeyHash {
    size_t operator()(const SourceRssidKey& key) const noexcept {
        return std::hash<uint64_t>{}((static_cast<uint64_t>(key.child_index) << 32) | key.source_rssid);
    }
};

// Concrete projection plan: for each (child_index, source_rssid) the plan
// records the final rssid the merge will assign in the merged tablet's id
// space. Consumed by ctx.map_rssid() (commit 4) and the v2 fast-path
// (commit 5). For now, this commit only BUILDS the plan; no caller
// consumes it.
struct RssidProjectionPlan {
    // (child_index, source_rssid) → final_rssid for shared-ancestor rowsets
    // in safe families. Populated for both the rowset.id() key (covers
    // add_rowset's first map_rssid call and rowset-level metadata such as
    // delvec keys) and every get_rssid(rowset, segment_position) key
    // (covers data-entry remap with sparse segment_idx layouts).
    std::unordered_map<SourceRssidKey, uint32_t, SourceRssidKeyHash> explicit_rssid_map;

    // family_id → accumulated rssid_offset to write into emitted legacy
    // sstable PBs by the v2 fast-path. Recorded only for safe families.
    std::unordered_map<uint32_t, int64_t> family_legacy_sstable_offset;

    // Occupancy table for collision detection during plan build. Each
    // entry records which physical rowset claimed a given final rssid AND
    // which family that rowset belonged to (or kNoFamily if it came from
    // an orphan ctx). The plan's consumers do NOT read this map at runtime;
    // it is exposed for tests and diagnostics.
    struct Occupancy {
        RowsetPhysicalKey key;
        uint32_t family_id; // == InferredSplitFamilies::kNoFamily for orphan
    };
    std::unordered_map<uint32_t, Occupancy> occupied_rssids;

    // family_ids that hit a collision during plan build. The fast-path
    // (commit 5) treats every sstable in an unsafe family as a fallback
    // rebuild; merge_rowsets (commit 4) skips the explicit projection for
    // member rowsets and lets v1 first-emitter natural assignment take over.
    std::unordered_set<uint32_t> unsafe_families;
};

// Build the projection plan from the merge inputs and the inferred
// families. Algorithm (per ~/workspace/doc/legacy_sstable_fastpath_v2.md
// §"build_rssid_projection_plan"):
//
//   Step 1: For every (ctx, rowset, key_position) — where key_position is
//           rowset.id() OR each get_rssid(rowset, segment_position):
//             offset = (rowset is shared-ancestor in a family)
//                       ? family.canonical_rssid_offset
//                       : ctx.rssid_offset
//             record_occupancy(key_position + offset, RowsetPhysicalKey(rowset), family_id)
//
//           record_occupancy compares against any prior claim on the same
//           final rssid:
//             - empty           → claim it
//             - same physical   → safe dedup, no-op
//             - different       → mark BOTH involved families unsafe (the
//                                 current family and the prior occupant's
//                                 family, if any)
//
//   Step 2: For every safe family, populate explicit_rssid_map for every
//           shared-ancestor rowset's rowset.id() AND every segment position.
//           Record family_legacy_sstable_offset[family_id] for the fast-
//           path's PB emit step (commit 5).
//
// Boundary protection: if any final rssid arithmetic overflows uint32_t,
// the affected family is marked unsafe.
StatusOr<RssidProjectionPlan> build_rssid_projection_plan(const std::vector<SplitFamilyInferenceInput>& inputs,
                                                          const InferredSplitFamilies& families);

} // namespace detail

} // namespace starrocks::lake
