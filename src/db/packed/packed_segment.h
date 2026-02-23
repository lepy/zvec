// Copyright 2025-present the zvec project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <zvec/db/schema.h>
#include <zvec/db/status.h>
#include "db/index/column/vector_column/combined_vector_column_indexer.h"
#include "db/index/column/vector_column/vector_column_indexer.h"
#include "db/index/common/delete_store.h"
#include "db/index/common/index_filter.h"
#include "db/index/common/meta.h"
#include "db/index/segment/segment.h"
#include "db/index/storage/base_forward_store.h"
#include "db/packed/packed_forward_store.h"

namespace zvec {

// Read-only Segment implementation backed by sections from a .zvecpack file.
// Forward stores are PackedForwardStore (Arrow IPC from mmap).
// Vector indexes are loaded via MMapSliceSegment -> IndexSegmentStorage.
class PackedSegment : public Segment,
                      public std::enable_shared_from_this<PackedSegment> {
 public:
  using Ptr = std::shared_ptr<PackedSegment>;

  // Section data: pointer + size into the mmap'd file.
  using SectionMap =
      std::map<std::string, std::pair<const uint8_t *, size_t>>;

  // Open a read-only segment from pre-parsed sections.
  static Result<Segment::Ptr> Open(
      const CollectionSchema &schema, const SegmentMeta &meta,
      const DeleteStore::Ptr &delete_store, const SectionMap &sections,
      std::shared_ptr<void> mmap_lifetime);

  ~PackedSegment() = default;

  // Read-only accessors
  SegmentID id() const override;
  SegmentMeta::Ptr meta() const override;
  uint64_t doc_count(const IndexFilter::Ptr filter = nullptr) override;

  // Fetch / scan for sql engine
  TablePtr fetch(const std::vector<std::string> &columns,
                 const std::vector<int> &indices) const override;
  ExecBatchPtr fetch(const std::vector<std::string> &columns,
                     int index) const override;
  RecordBatchReaderPtr scan(
      const std::vector<std::string> &columns) const override;

  // Vector indexer access
  CombinedVectorColumnIndexer::Ptr get_combined_vector_indexer(
      const std::string &field_name) const override;
  CombinedVectorColumnIndexer::Ptr get_quant_combined_vector_indexer(
      const std::string &field_name) const override;
  std::vector<VectorColumnIndexer::Ptr> get_vector_indexer(
      const std::string &field_name) const override;
  std::vector<VectorColumnIndexer::Ptr> get_quant_vector_indexer(
      const std::string &field_name) const override;
  InvertedColumnIndexer::Ptr get_scalar_indexer(
      const std::string &field_name) const override;
  const IndexFilter::Ptr get_filter() override;

  Doc::Ptr Fetch(uint64_t g_doc_id) override;

  // Write operations - all return read-only error
  Status Insert(Doc &doc) override;
  Status Upsert(Doc &doc) override;
  Status Update(Doc &doc) override;
  Status Delete(const std::string &pk) override;
  Status Delete(uint64_t g_doc_id) override;

  Status add_column(FieldSchema::Ptr column_schema,
                    const std::string &expression,
                    const AddColumnOptions &options) override;
  Status alter_column(const std::string &column_name,
                      const FieldSchema::Ptr &new_column_schema,
                      const AlterColumnOptions &options) override;
  Status drop_column(const std::string &column_name) override;

  Status create_all_vector_index(
      int concurrency, SegmentMeta::Ptr *new_segment_meta,
      std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          *vector_indexers,
      std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          *quant_vector_indexers) override;
  Status create_vector_index(
      const std::string &column, const IndexParams::Ptr &index_params,
      int concurrency, SegmentMeta::Ptr *new_segment_meta,
      std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          *vector_indexers,
      std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          *quant_vector_indexers) override;
  Status drop_vector_index(
      const std::string &column, SegmentMeta::Ptr *new_segment_meta,
      std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          *vector_indexers) override;
  Status reload_vector_index(
      const CollectionSchema &schema, const SegmentMeta::Ptr &segment_meta,
      const std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          &vector_indexers,
      const std::unordered_map<std::string, VectorColumnIndexer::Ptr>
          &quant_vector_indexers) override;
  bool vector_index_ready(const std::string &column,
                          const IndexParams::Ptr &index_params) const override;
  bool all_vector_index_ready() const override;
  Status create_scalar_index(const std::vector<std::string> &columns,
                             const IndexParams::Ptr &index_params,
                             SegmentMeta::Ptr *new_segment_meta,
                             InvertedIndexer::Ptr *new_scalar_indexer) override;
  Status drop_scalar_index(const std::vector<std::string> &columns,
                           SegmentMeta::Ptr *new_segment_meta,
                           InvertedIndexer::Ptr *new_scalar_indexer) override;
  Status reload_scalar_index(
      const CollectionSchema &schema, const SegmentMeta::Ptr &segment_meta,
      const InvertedIndexer::Ptr &scalar_indexer) override;

  Status flush() override;
  Status dump() override;
  Status destroy() override;

 private:
  PackedSegment() = default;

  Status load_forward_stores(const SectionMap &sections);
  Status load_vector_indexers(const SectionMap &sections);
  Status load_doc_ids();
  void build_block_offsets();

  Result<uint64_t> get_global_doc_id(uint32_t local_id) const;

  int find_store_index(int segment_doc_id,
                       const std::string &col_name) const;

  static Status read_only_error() {
    return Status::InvalidArgument("PackedCollection is read-only");
  }

  // Filter that translates local IDs to global and checks delete store
  class PackedIndexFilter : public IndexFilter {
   public:
    PackedIndexFilter(const DeleteStore::Ptr &delete_store,
                      std::weak_ptr<const PackedSegment> segment)
        : delete_store_(delete_store), segment_(segment) {}
    bool is_filtered(uint64_t id) const override;

   private:
    DeleteStore::Ptr delete_store_;
    std::weak_ptr<const PackedSegment> segment_;
  };

  CollectionSchema::Ptr collection_schema_;
  SegmentMeta::Ptr segment_meta_;
  DeleteStore::Ptr delete_store_;
  std::shared_ptr<void> mmap_lifetime_;

  // Forward stores, one per SCALAR block
  std::vector<BaseForwardStore::Ptr> forward_stores_;

  // Block offsets: cumulative doc counts for routing doc_id to store
  std::vector<int> block_offsets_;
  std::vector<BlockMeta> scalar_block_metas_;

  // Vector indexers per field name
  std::unordered_map<std::string, std::vector<VectorColumnIndexer::Ptr>>
      vector_indexers_;
  std::unordered_map<std::string, std::vector<VectorColumnIndexer::Ptr>>
      quant_vector_indexers_;

  // Vector block metas per field name
  std::unordered_map<std::string, std::vector<BlockMeta>>
      vector_block_metas_;
  std::unordered_map<std::string, std::vector<BlockMeta>>
      quant_vector_block_metas_;

  // local_id -> global_doc_id
  std::vector<uint64_t> doc_ids_;

  IndexFilter::Ptr filter_;
};

}  // namespace zvec
