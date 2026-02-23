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

#include "packed_segment.h"
#include <algorithm>
#include <arrow/table.h>
#include <zvec/ailego/logger/logger.h>
#include <zvec/core/framework/index_segment_storage.h>
#include <zvec/core/interface/index_factory.h>
#include <zvec/db/index_params.h>
#include "db/common/constants.h"
#include "db/index/column/vector_column/vector_column_indexer.h"
#include "db/index/storage/store_helper.h"
#include "db/packed/packed_index_storage.h"

namespace zvec {

///////////////////////////////////////////////////////////////////////////////
// PackedIndexFilter
///////////////////////////////////////////////////////////////////////////////

bool PackedSegment::PackedIndexFilter::is_filtered(uint64_t id) const {
  auto seg = segment_.lock();
  if (!seg) return false;
  auto result = seg->get_global_doc_id(id);
  if (!result.has_value()) return false;
  uint64_t doc_id = result.value();
  if (delete_store_ && delete_store_->is_deleted(doc_id)) {
    return true;
  }
  return false;
}

///////////////////////////////////////////////////////////////////////////////
// PackedSegment::Open
///////////////////////////////////////////////////////////////////////////////

Result<Segment::Ptr> PackedSegment::Open(
    const CollectionSchema &schema, const SegmentMeta &meta,
    const DeleteStore::Ptr &delete_store, const SectionMap &sections,
    std::shared_ptr<void> mmap_lifetime) {
  auto segment = std::shared_ptr<PackedSegment>(new PackedSegment());
  segment->collection_schema_ =
      std::make_shared<CollectionSchema>(schema);
  segment->segment_meta_ = std::make_shared<SegmentMeta>(meta);
  segment->delete_store_ = delete_store;
  segment->mmap_lifetime_ = std::move(mmap_lifetime);

  // Load forward stores from SCALAR block sections
  auto s = segment->load_forward_stores(sections);
  if (!s.ok()) return tl::make_unexpected(s);

  // Build block offsets for routing doc_ids to stores
  segment->build_block_offsets();

  // Load vector indexers from VECTOR_INDEX block sections
  s = segment->load_vector_indexers(sections);
  if (!s.ok()) return tl::make_unexpected(s);

  // Extract doc_ids_ from forward stores (GLOBAL_DOC_ID column)
  s = segment->load_doc_ids();
  if (!s.ok()) return tl::make_unexpected(s);

  // Create filter
  segment->filter_ = std::make_shared<PackedIndexFilter>(
      delete_store, std::weak_ptr<const PackedSegment>(segment));

  return std::static_pointer_cast<Segment>(segment);
}

///////////////////////////////////////////////////////////////////////////////
// Forward store loading
///////////////////////////////////////////////////////////////////////////////

Status PackedSegment::load_forward_stores(const SectionMap &sections) {
  auto seg_id = segment_meta_->id();

  for (const auto &block : segment_meta_->persisted_blocks()) {
    if (block.type() != BlockType::SCALAR) continue;

    scalar_block_metas_.push_back(block);

    // Section name: "Forward.{seg_id}.{block_id}"
    std::string section_name = "Forward." + std::to_string(seg_id) + "." +
                               std::to_string(block.id());
    auto it = sections.find(section_name);
    if (it == sections.end()) {
      return Status::InternalError("Missing forward section: " + section_name);
    }

    auto store = std::make_shared<PackedForwardStore>(
        it->second.first, it->second.second);
    auto s = store->Open();
    if (!s.ok()) {
      return Status::InternalError("Failed to open forward store for " +
                                   section_name + ": " + s.message());
    }
    forward_stores_.push_back(std::move(store));
  }

  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
// Vector indexer loading
///////////////////////////////////////////////////////////////////////////////

Status PackedSegment::load_vector_indexers(const SectionMap &sections) {
  auto seg_id = segment_meta_->id();

  for (const auto &block : segment_meta_->persisted_blocks()) {
    bool is_quantized = (block.type() == BlockType::VECTOR_INDEX_QUANTIZE);
    if (block.type() != BlockType::VECTOR_INDEX && !is_quantized) continue;

    // Vector blocks have exactly one column name
    if (block.columns().empty()) continue;
    const std::string &col_name = block.columns()[0];

    // Section name: "VecIdx.{seg}.{col}.{block}" or "QVecIdx.{seg}.{col}.{block}"
    std::string prefix = is_quantized ? "QVecIdx" : "VecIdx";
    std::string section_name = prefix + "." + std::to_string(seg_id) + "." +
                               col_name + "." + std::to_string(block.id());
    auto it = sections.find(section_name);
    if (it == sections.end()) {
      LOG_WARN("Missing vector index section: %s", section_name.c_str());
      continue;
    }

    // Create MMapSliceSegment wrapping the section data
    auto slice_segment = std::make_shared<MMapSliceSegment>(
        it->second.first, it->second.second, mmap_lifetime_);

    // Create IndexSegmentStorage for nested unpacking
    auto segment_storage =
        std::make_shared<core::IndexSegmentStorage>(slice_segment);
    if (0 != segment_storage->open("", true)) {
      return Status::InternalError(
          "Failed to open IndexSegmentStorage for " + section_name);
    }

    // Get field schema for creating the indexer
    auto *field = collection_schema_->get_field(col_name);
    if (field == nullptr) {
      return Status::InternalError("Field not found: " + col_name);
    }

    // Create VectorColumnIndexer and open with the storage
    auto indexer = std::make_shared<VectorColumnIndexer>(
        section_name, *field);
    auto s = indexer->Open(
        std::static_pointer_cast<core::IndexStorage>(segment_storage));
    if (!s.ok()) {
      return Status::InternalError(
          "Failed to open vector indexer for " + section_name + ": " +
          s.message());
    }

    if (is_quantized) {
      quant_vector_indexers_[col_name].push_back(indexer);
      quant_vector_block_metas_[col_name].push_back(block);
    } else {
      vector_indexers_[col_name].push_back(indexer);
      vector_block_metas_[col_name].push_back(block);
    }
  }

  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
// Doc ID loading
///////////////////////////////////////////////////////////////////////////////

Status PackedSegment::load_doc_ids() {
  doc_ids_.clear();
  for (size_t i = 0; i < forward_stores_.size(); ++i) {
    auto table = forward_stores_[i]->fetch(
        {GLOBAL_DOC_ID}, {});  // empty indices -> need scan instead

    // Use scan to read all doc IDs
    auto reader = forward_stores_[i]->scan({GLOBAL_DOC_ID});
    if (!reader) {
      return Status::InternalError("Failed to scan GLOBAL_DOC_ID from store");
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
      auto status = reader->ReadNext(&batch);
      if (!status.ok()) {
        return Status::InternalError(
            "Failed to read GLOBAL_DOC_ID batch: " + status.ToString());
      }
      if (!batch) break;

      auto col = batch->column(0);
      auto uint64_array =
          std::dynamic_pointer_cast<arrow::UInt64Array>(col);
      if (!uint64_array) {
        return Status::InternalError("GLOBAL_DOC_ID column is not UInt64");
      }
      for (int64_t j = 0; j < uint64_array->length(); ++j) {
        doc_ids_.push_back(uint64_array->Value(j));
      }
    }
  }
  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
// Block offset building
///////////////////////////////////////////////////////////////////////////////

void PackedSegment::build_block_offsets() {
  block_offsets_.clear();
  int offset = 0;
  for (const auto &block : scalar_block_metas_) {
    block_offsets_.push_back(offset);
    offset += block.doc_count();
  }
}

///////////////////////////////////////////////////////////////////////////////
// Helper: get_global_doc_id
///////////////////////////////////////////////////////////////////////////////

Result<uint64_t> PackedSegment::get_global_doc_id(uint32_t local_id) const {
  if (local_id >= doc_ids_.size()) {
    return tl::make_unexpected(
        Status::InvalidArgument("local_id out of range"));
  }
  return doc_ids_[local_id];
}

///////////////////////////////////////////////////////////////////////////////
// Helper: find_store_index
///////////////////////////////////////////////////////////////////////////////

int PackedSegment::find_store_index(int segment_doc_id,
                                    const std::string &col_name) const {
  for (int i = static_cast<int>(scalar_block_metas_.size()) - 1; i >= 0; --i) {
    if (segment_doc_id >= block_offsets_[i] &&
        scalar_block_metas_[i].contain_column(col_name)) {
      return i;
    }
  }
  return -1;
}

///////////////////////////////////////////////////////////////////////////////
// Read-only accessors
///////////////////////////////////////////////////////////////////////////////

SegmentID PackedSegment::id() const { return segment_meta_->id(); }

SegmentMeta::Ptr PackedSegment::meta() const { return segment_meta_; }

uint64_t PackedSegment::doc_count(const IndexFilter::Ptr filter) {
  uint64_t total = segment_meta_->doc_count();
  if (filter == nullptr && delete_store_) {
    uint64_t min_id = segment_meta_->min_doc_id();
    uint64_t max_id = segment_meta_->max_doc_id();
    total -= delete_store_->range_count(min_id, max_id + 1);
  }
  return total;
}

///////////////////////////////////////////////////////////////////////////////
// Fetch (Table)
///////////////////////////////////////////////////////////////////////////////

TablePtr PackedSegment::fetch(const std::vector<std::string> &columns,
                              const std::vector<int> &indices) const {
  if (columns.empty()) return nullptr;

  // Build result schema
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto &col : columns) {
    if (col == LOCAL_ROW_ID) {
      fields.push_back(arrow::field(LOCAL_ROW_ID, arrow::uint64()));
    } else if (col == GLOBAL_DOC_ID) {
      fields.push_back(arrow::field(GLOBAL_DOC_ID, arrow::uint64()));
    } else if (col == USER_ID) {
      fields.push_back(arrow::field(USER_ID, arrow::utf8()));
    } else {
      auto *field = collection_schema_->get_field(col);
      if (!field) return nullptr;
      std::shared_ptr<arrow::Field> arrow_field;
      auto status = ConvertFieldSchemaToArrowField(field, &arrow_field);
      if (!status.ok()) return nullptr;
      fields.push_back(std::move(arrow_field));
    }
  }
  auto result_schema = std::make_shared<arrow::Schema>(fields);

  if (indices.empty()) {
    arrow::ArrayVector empty_arrays;
    for (const auto &field : fields) {
      empty_arrays.push_back(arrow::MakeEmptyArray(field->type()).ValueOrDie());
    }
    return arrow::Table::Make(result_schema, empty_arrays, 0);
  }

  // Group requests by store: store_index -> {col -> [(output_row, local_row)]}
  std::map<int, std::map<std::string, std::vector<std::pair<int, int>>>>
      store_requests;

  std::vector<std::pair<int, uint64_t>> local_doc_id_values;

  for (int output_row = 0; output_row < static_cast<int>(indices.size());
       ++output_row) {
    int doc_id = indices[output_row];
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
      const std::string &col = columns[col_idx];
      if (col == LOCAL_ROW_ID) {
        local_doc_id_values.emplace_back(output_row, doc_id);
        continue;
      }
      int store_idx = find_store_index(doc_id, col);
      if (store_idx >= 0) {
        int local_row = doc_id - block_offsets_[store_idx];
        store_requests[store_idx][col].emplace_back(output_row, local_row);
      } else {
        LOG_ERROR("Document ID %d not found in packed segment %d", doc_id,
                  meta()->id());
        return nullptr;
      }
    }
  }

  // Fetch from stores and collect scalars
  std::vector<std::vector<std::pair<int, std::shared_ptr<arrow::Scalar>>>>
      column_results(columns.size());

  for (const auto &[store_idx, col_to_rows] : store_requests) {
    std::vector<std::string> fetch_cols;
    fetch_cols.reserve(col_to_rows.size());
    for (const auto &kv : col_to_rows) {
      fetch_cols.push_back(kv.first);
    }

    std::vector<int> fetch_rows;
    std::vector<std::pair<int, int>> output_to_result;
    for (const auto &[out_row, local_row] :
         col_to_rows.at(fetch_cols[0])) {
      fetch_rows.push_back(local_row);
      output_to_result.emplace_back(out_row,
                                    static_cast<int>(fetch_rows.size() - 1));
    }

    auto block_table = forward_stores_[store_idx]->fetch(fetch_cols, fetch_rows);
    if (!block_table || block_table->num_rows() == 0) continue;

    for (size_t i = 0; i < fetch_cols.size(); ++i) {
      const std::string &col = fetch_cols[i];
      auto col_it = std::find(columns.begin(), columns.end(), col);
      if (col_it == columns.end()) continue;
      size_t col_index = std::distance(columns.begin(), col_it);

      auto chunks = block_table->column(i)->chunks();
      auto flat_res =
          arrow::Concatenate(chunks, arrow::default_memory_pool());
      if (!flat_res.ok()) return nullptr;
      auto flat_array = flat_res.ValueOrDie();

      for (size_t j = 0; j < fetch_rows.size(); ++j) {
        auto scalar_result = flat_array->GetScalar(j);
        if (!scalar_result.ok()) continue;
        int out_row = output_to_result[j].first;
        column_results[col_index].emplace_back(
            out_row, std::move(scalar_result.ValueOrDie()));
      }
    }
  }

  // Build result arrays
  std::vector<std::shared_ptr<arrow::Array>> result_arrays(columns.size());
  bool need_local_doc_id = false;
  size_t local_doc_id_col_index = 0;

  for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
    if (columns[col_idx] == LOCAL_ROW_ID) {
      need_local_doc_id = true;
      local_doc_id_col_index = col_idx;
      continue;
    }

    auto &result_vec = column_results[col_idx];
    std::sort(result_vec.begin(), result_vec.end());

    std::vector<std::shared_ptr<arrow::Scalar>> ordered_scalars;
    for (int i = 0; i < static_cast<int>(indices.size()); ++i) {
      auto it = std::find_if(
          result_vec.begin(), result_vec.end(),
          [i](const std::pair<int, std::shared_ptr<arrow::Scalar>> &p) {
            return p.first == i;
          });
      if (it != result_vec.end()) {
        ordered_scalars.push_back(it->second);
      } else {
        auto field = result_schema->GetFieldByName(columns[col_idx]);
        ordered_scalars.push_back(
            arrow::MakeNullScalar(field ? field->type() : arrow::null()));
      }
    }

    auto status = ConvertScalarVectorToArrayByType(ordered_scalars,
                                                   &result_arrays[col_idx]);
    if (!status.ok()) return nullptr;
  }

  if (need_local_doc_id) {
    std::sort(local_doc_id_values.begin(), local_doc_id_values.end());
    arrow::UInt64Builder builder;
    for (const auto &[row, id] : local_doc_id_values) {
      auto s = builder.Append(id);
      if (!s.ok()) return nullptr;
    }
    std::shared_ptr<arrow::Array> arr;
    auto s = builder.Finish(&arr);
    if (!s.ok()) return nullptr;
    result_arrays[local_doc_id_col_index] = std::move(arr);
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> result_columns;
  result_columns.reserve(result_arrays.size());
  for (const auto &arr : result_arrays) {
    result_columns.push_back(std::make_shared<arrow::ChunkedArray>(arr));
  }

  return arrow::Table::Make(result_schema, result_columns,
                            static_cast<int64_t>(indices.size()));
}

///////////////////////////////////////////////////////////////////////////////
// Fetch (ExecBatch) - single doc
///////////////////////////////////////////////////////////////////////////////

ExecBatchPtr PackedSegment::fetch(const std::vector<std::string> &columns,
                                  int doc_id) const {
  if (columns.empty()) return nullptr;

  int store_idx = find_store_index(doc_id, columns[0]);
  if (store_idx >= 0) {
    int local_row = doc_id - block_offsets_[store_idx];
    return forward_stores_[store_idx]->fetch(columns, local_row);
  }

  // Fallback: use Table fetch
  auto table = fetch(columns, std::vector<int>{doc_id});
  if (!table || table->num_rows() == 0) return nullptr;

  std::vector<arrow::Datum> datums;
  for (const auto &col : table->columns()) {
    datums.emplace_back(col->chunk(0)->GetScalar(0).ValueOrDie());
  }
  auto exec_batch_result =
      arrow::compute::ExecBatch::Make(datums, table->num_rows());
  if (exec_batch_result.ok()) {
    return std::make_shared<arrow::compute::ExecBatch>(
        exec_batch_result.ValueOrDie());
  }
  return nullptr;
}

///////////////////////////////////////////////////////////////////////////////
// Scan
///////////////////////////////////////////////////////////////////////////////

RecordBatchReaderPtr PackedSegment::scan(
    const std::vector<std::string> &columns) const {
  if (columns.empty()) return nullptr;

  // Collect readers from all forward stores that have the requested columns
  std::vector<std::shared_ptr<arrow::RecordBatchReader>> readers;
  for (size_t i = 0; i < forward_stores_.size(); ++i) {
    std::vector<std::string> interested;
    for (const auto &col : columns) {
      if (col == LOCAL_ROW_ID) continue;
      if (scalar_block_metas_[i].contain_column(col)) {
        interested.push_back(col);
      }
    }
    if (interested.empty()) continue;
    auto reader = forward_stores_[i]->scan(interested);
    if (reader) readers.push_back(std::move(reader));
  }

  if (readers.empty()) return nullptr;
  if (readers.size() == 1) return readers[0];

  // Collect all batches from all readers and create a single reader
  auto schema = readers[0]->schema();
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  for (auto &reader : readers) {
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
      auto st = reader->ReadNext(&batch);
      if (!st.ok() || !batch) break;
      all_batches.push_back(std::move(batch));
    }
  }
  auto result = arrow::RecordBatchReader::Make(all_batches, schema);
  if (result.ok()) return result.ValueOrDie();
  return nullptr;
}

///////////////////////////////////////////////////////////////////////////////
// Fetch doc by global doc ID
///////////////////////////////////////////////////////////////////////////////

Doc::Ptr PackedSegment::Fetch(uint64_t g_doc_id) {
  // Find local doc_id
  auto it = std::find(doc_ids_.begin(), doc_ids_.end(), g_doc_id);
  if (it == doc_ids_.end()) return nullptr;

  int local_id = static_cast<int>(std::distance(doc_ids_.begin(), it));

  // Fetch all forward columns + USER_ID
  std::vector<std::string> columns = {USER_ID};
  auto forward_fields = collection_schema_->forward_field_names();
  columns.insert(columns.end(), forward_fields.begin(), forward_fields.end());

  std::vector<int> indices = {local_id};
  auto table = fetch(columns, indices);
  if (!table || table->num_rows() == 0) return nullptr;

  // Build Doc from table, converting Arrow scalars to native types
  auto doc = std::make_shared<Doc>();
  for (int i = 0; i < table->num_columns(); ++i) {
    auto col = table->column(i);
    if (col->length() == 0) continue;
    auto scalar_result = col->chunk(0)->GetScalar(0);
    if (!scalar_result.ok()) continue;
    auto scalar = scalar_result.ValueOrDie();
    if (!scalar || !scalar->is_valid) continue;

    auto field_name = table->schema()->field(i)->name();
    auto type_id = scalar->type->id();

    switch (type_id) {
      case arrow::Type::STRING: {
        auto s = std::dynamic_pointer_cast<arrow::StringScalar>(scalar);
        if (s) doc->set(field_name, std::string(s->view()));
        break;
      }
      case arrow::Type::INT32: {
        auto s = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::INT64: {
        auto s = std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::UINT32: {
        auto s = std::dynamic_pointer_cast<arrow::UInt32Scalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::UINT64: {
        auto s = std::dynamic_pointer_cast<arrow::UInt64Scalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::FLOAT: {
        auto s = std::dynamic_pointer_cast<arrow::FloatScalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::DOUBLE: {
        auto s = std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::BOOL: {
        auto s = std::dynamic_pointer_cast<arrow::BooleanScalar>(scalar);
        if (s) doc->set(field_name, s->value);
        break;
      }
      case arrow::Type::BINARY: {
        auto s = std::dynamic_pointer_cast<arrow::BinaryScalar>(scalar);
        if (s) doc->set(field_name, std::string(s->view()));
        break;
      }
      case arrow::Type::LIST: {
        auto list_scalar =
            std::dynamic_pointer_cast<arrow::ListScalar>(scalar);
        if (list_scalar && list_scalar->value) {
          auto list_type = std::dynamic_pointer_cast<arrow::ListType>(
              scalar->type);
          if (list_type) {
            auto vt = list_type->value_type()->id();
            switch (vt) {
              case arrow::Type::BOOL: {
                auto arr = std::dynamic_pointer_cast<arrow::BooleanArray>(
                    list_scalar->value);
                if (arr) {
                  std::vector<bool> v;
                  v.reserve(arr->length());
                  for (int64_t j = 0; j < arr->length(); ++j)
                    if (arr->IsValid(j)) v.push_back(arr->Value(j));
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::INT32: {
                auto arr = std::dynamic_pointer_cast<arrow::Int32Array>(
                    list_scalar->value);
                if (arr) {
                  std::vector<int32_t> v(arr->raw_values(),
                                         arr->raw_values() + arr->length());
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::INT64: {
                auto arr = std::dynamic_pointer_cast<arrow::Int64Array>(
                    list_scalar->value);
                if (arr) {
                  std::vector<int64_t> v(arr->raw_values(),
                                         arr->raw_values() + arr->length());
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::UINT32: {
                auto arr = std::dynamic_pointer_cast<arrow::UInt32Array>(
                    list_scalar->value);
                if (arr) {
                  std::vector<uint32_t> v(arr->raw_values(),
                                          arr->raw_values() + arr->length());
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::UINT64: {
                auto arr = std::dynamic_pointer_cast<arrow::UInt64Array>(
                    list_scalar->value);
                if (arr) {
                  std::vector<uint64_t> v(arr->raw_values(),
                                          arr->raw_values() + arr->length());
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::FLOAT: {
                auto arr = std::dynamic_pointer_cast<arrow::FloatArray>(
                    list_scalar->value);
                if (arr) {
                  std::vector<float> v(arr->raw_values(),
                                       arr->raw_values() + arr->length());
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::DOUBLE: {
                auto arr = std::dynamic_pointer_cast<arrow::DoubleArray>(
                    list_scalar->value);
                if (arr) {
                  std::vector<double> v(arr->raw_values(),
                                        arr->raw_values() + arr->length());
                  doc->set(field_name, v);
                }
                break;
              }
              case arrow::Type::STRING: {
                auto arr = std::dynamic_pointer_cast<arrow::StringArray>(
                    list_scalar->value);
                if (arr) {
                  std::vector<std::string> v;
                  v.reserve(arr->length());
                  for (int64_t j = 0; j < arr->length(); ++j)
                    if (arr->IsValid(j)) v.push_back(arr->GetString(j));
                  doc->set(field_name, v);
                }
                break;
              }
              default:
                break;
            }
          }
        }
        break;
      }
      default:
        LOG_WARN("Unsupported Arrow type %d for field %s in packed fetch",
                 static_cast<int>(type_id), field_name.c_str());
        break;
    }
  }
  return doc;
}

///////////////////////////////////////////////////////////////////////////////
// Vector indexer access
///////////////////////////////////////////////////////////////////////////////

CombinedVectorColumnIndexer::Ptr PackedSegment::get_combined_vector_indexer(
    const std::string &field_name) const {
  std::vector<VectorColumnIndexer::Ptr> indexers;
  auto iter = vector_indexers_.find(field_name);
  if (iter != vector_indexers_.end()) {
    indexers = iter->second;
  }
  if (indexers.empty()) return nullptr;

  auto *field = collection_schema_->get_field(field_name);
  if (!field) return nullptr;

  auto vector_index_params =
      std::dynamic_pointer_cast<VectorIndexParams>(field->index_params());
  MetricType metric_type = vector_index_params->metric_type();

  auto blocks_it = vector_block_metas_.find(field_name);
  std::vector<BlockMeta> blocks;
  if (blocks_it != vector_block_metas_.end()) {
    blocks = blocks_it->second;
  }

  return std::make_shared<CombinedVectorColumnIndexer>(
      indexers, indexers, *field, *segment_meta_, std::move(blocks),
      metric_type);
}

CombinedVectorColumnIndexer::Ptr
PackedSegment::get_quant_combined_vector_indexer(
    const std::string &field_name) const {
  std::vector<VectorColumnIndexer::Ptr> indexers;
  auto iter = quant_vector_indexers_.find(field_name);
  if (iter != quant_vector_indexers_.end()) {
    indexers = iter->second;
  }
  if (indexers.empty()) return nullptr;

  // normal_indexers for fetch fallback
  std::vector<VectorColumnIndexer::Ptr> normal_indexers;
  auto n_iter = vector_indexers_.find(field_name);
  if (n_iter != vector_indexers_.end()) {
    normal_indexers = n_iter->second;
  }

  auto *field = collection_schema_->get_field(field_name);
  if (!field) return nullptr;

  auto vector_index_params =
      std::dynamic_pointer_cast<VectorIndexParams>(field->index_params());
  MetricType metric_type = vector_index_params->metric_type();

  auto blocks_it = quant_vector_block_metas_.find(field_name);
  std::vector<BlockMeta> blocks;
  if (blocks_it != quant_vector_block_metas_.end()) {
    blocks = blocks_it->second;
  }

  return std::make_shared<CombinedVectorColumnIndexer>(
      indexers, normal_indexers, *field, *segment_meta_, std::move(blocks),
      metric_type, true);
}

std::vector<VectorColumnIndexer::Ptr> PackedSegment::get_vector_indexer(
    const std::string &field_name) const {
  auto iter = vector_indexers_.find(field_name);
  if (iter != vector_indexers_.end()) return iter->second;
  return {};
}

std::vector<VectorColumnIndexer::Ptr> PackedSegment::get_quant_vector_indexer(
    const std::string &field_name) const {
  auto iter = quant_vector_indexers_.find(field_name);
  if (iter != quant_vector_indexers_.end()) return iter->second;
  return {};
}

InvertedColumnIndexer::Ptr PackedSegment::get_scalar_indexer(
    const std::string & /*field_name*/) const {
  // Scalar indices not supported in packed format (V2)
  return nullptr;
}

const IndexFilter::Ptr PackedSegment::get_filter() { return filter_; }

///////////////////////////////////////////////////////////////////////////////
// Write operations - read-only errors
///////////////////////////////////////////////////////////////////////////////

Status PackedSegment::Insert(Doc &) { return read_only_error(); }
Status PackedSegment::Upsert(Doc &) { return read_only_error(); }
Status PackedSegment::Update(Doc &) { return read_only_error(); }
Status PackedSegment::Delete(const std::string &) { return read_only_error(); }
Status PackedSegment::Delete(uint64_t) { return read_only_error(); }

Status PackedSegment::add_column(FieldSchema::Ptr, const std::string &,
                                 const AddColumnOptions &) {
  return read_only_error();
}

Status PackedSegment::alter_column(const std::string &,
                                   const FieldSchema::Ptr &,
                                   const AlterColumnOptions &) {
  return read_only_error();
}

Status PackedSegment::drop_column(const std::string &) {
  return read_only_error();
}

Status PackedSegment::create_all_vector_index(
    int, SegmentMeta::Ptr *,
    std::unordered_map<std::string, VectorColumnIndexer::Ptr> *,
    std::unordered_map<std::string, VectorColumnIndexer::Ptr> *) {
  return read_only_error();
}

Status PackedSegment::create_vector_index(
    const std::string &, const IndexParams::Ptr &, int, SegmentMeta::Ptr *,
    std::unordered_map<std::string, VectorColumnIndexer::Ptr> *,
    std::unordered_map<std::string, VectorColumnIndexer::Ptr> *) {
  return read_only_error();
}

Status PackedSegment::drop_vector_index(
    const std::string &, SegmentMeta::Ptr *,
    std::unordered_map<std::string, VectorColumnIndexer::Ptr> *) {
  return read_only_error();
}

Status PackedSegment::reload_vector_index(
    const CollectionSchema &, const SegmentMeta::Ptr &,
    const std::unordered_map<std::string, VectorColumnIndexer::Ptr> &,
    const std::unordered_map<std::string, VectorColumnIndexer::Ptr> &) {
  return read_only_error();
}

bool PackedSegment::vector_index_ready(const std::string &column,
                                       const IndexParams::Ptr &) const {
  return vector_indexers_.count(column) > 0;
}

bool PackedSegment::all_vector_index_ready() const {
  auto vector_fields = collection_schema_->vector_fields();
  for (const auto &field : vector_fields) {
    if (vector_indexers_.count(field->name()) == 0) return false;
  }
  return true;
}

Status PackedSegment::create_scalar_index(const std::vector<std::string> &,
                                          const IndexParams::Ptr &,
                                          SegmentMeta::Ptr *,
                                          InvertedIndexer::Ptr *) {
  return read_only_error();
}

Status PackedSegment::drop_scalar_index(const std::vector<std::string> &,
                                        SegmentMeta::Ptr *,
                                        InvertedIndexer::Ptr *) {
  return read_only_error();
}

Status PackedSegment::reload_scalar_index(const CollectionSchema &,
                                          const SegmentMeta::Ptr &,
                                          const InvertedIndexer::Ptr &) {
  return read_only_error();
}

Status PackedSegment::flush() { return Status::OK(); }
Status PackedSegment::dump() { return Status::OK(); }
Status PackedSegment::destroy() { return read_only_error(); }

}  // namespace zvec
