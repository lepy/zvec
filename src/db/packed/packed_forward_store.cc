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

#include "packed_forward_store.h"
#include <zvec/ailego/logger/logger.h>
#include "db/common/constants.h"
#include "db/index/storage/store_helper.h"


namespace zvec {


PackedForwardStore::PackedForwardStore(const void *data, size_t size)
    : data_(data), size_(size) {}


Status PackedForwardStore::Open() {
  // Wrap mmap'd data as Arrow buffer (zero-copy)
  auto buffer = arrow::Buffer::Wrap(static_cast<const uint8_t *>(data_), size_);
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  auto result = arrow::ipc::RecordBatchFileReader::Open(buffer_reader);
  if (!result.ok()) {
    LOG_ERROR("Failed to open IPC from buffer: %s",
              result.status().ToString().c_str());
    return Status::InternalError(result.status().ToString());
  }
  ipc_file_reader_ = std::move(result).ValueOrDie();

  auto table_result = ipc_file_reader_->ToTable();
  if (!table_result.ok()) {
    LOG_ERROR("Failed to read IPC table from buffer: %s",
              table_result.status().ToString().c_str());
    return Status::InternalError(table_result.status().ToString());
  }
  table_ = std::move(table_result).ValueOrDie();

  if (table_->num_columns() == 0) {
    LOG_ERROR("IPC buffer has no columns");
    return Status::InternalError("IPC buffer has no columns");
  }

  auto chunked_array = table_->column(0);
  for (int i = 0; i < chunked_array->num_chunks(); ++i) {
    auto chunk = chunked_array->chunk(i);
    if (chunk->length() == 0) {
      LOG_ERROR("Encountered empty chunk at index %d", i);
      return Status::InternalError("Encountered empty IPC chunk");
    }
    chunk_index_map_.emplace_back(num_rows_, num_rows_ + chunk->length() - 1);
    num_rows_ += chunk->length();

    if (fixed_batch_size_ == -1) {
      fixed_batch_size_ = chunk->length();
    } else if (fixed_batch_size_ != chunk->length()) {
      if (i != chunked_array->num_chunks() - 1) {
        is_fixed_batch_size_ = false;
      }
    }
  }

  physic_schema_ = ipc_file_reader_->schema();
  LOG_INFO("PackedForwardStore opened: %lld rows, %d cols, %d chunks",
           static_cast<long long>(num_rows_), physic_schema_->num_fields(),
           chunked_array->num_chunks());

  return Status::OK();
}


bool PackedForwardStore::validate(
    const std::vector<std::string> &columns) const {
  if (columns.empty()) {
    LOG_ERROR("Empty columns");
    return false;
  }
  for (auto &column : columns) {
    if (column == LOCAL_ROW_ID) continue;
    if (physic_schema_->GetFieldIndex(column) == -1) {
      LOG_ERROR("Validate failed. unknown column: %s", column.c_str());
      return false;
    }
  }
  return true;
}


bool PackedForwardStore::FindTargetChunk(int target_index, int num_chunks,
                                         int *target_chunk_index,
                                         int64_t *offset_in_chunk) {
  if (target_index < 0 || target_index >= num_rows_) return false;

  if (is_fixed_batch_size_ && fixed_batch_size_ > 0) {
    int chunk_index = target_index / fixed_batch_size_;
    if (chunk_index < 0 || chunk_index >= num_chunks) return false;
    *target_chunk_index = chunk_index;
    *offset_in_chunk = target_index % fixed_batch_size_;
    return true;
  } else {
    int left = 0;
    int right = num_chunks - 1;
    while (left <= right) {
      int mid = left + (right - left) / 2;
      const auto &range = chunk_index_map_[mid];
      if (target_index >= range.first && target_index <= range.second) {
        *target_chunk_index = mid;
        *offset_in_chunk = target_index - range.first;
        return true;
      } else if (target_index < range.first) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
  }
  return false;
}


TablePtr PackedForwardStore::fetch(const std::vector<std::string> &columns,
                                   const std::vector<int> &indices) {
  if (!validate(columns)) return nullptr;

  if (indices.empty()) {
    auto fields = SelectFields(physic_schema_, columns);
    arrow::ArrayVector empty_arrays;
    for (const auto &field : fields) {
      empty_arrays.push_back(arrow::MakeEmptyArray(field->type()).ValueOrDie());
    }
    return arrow::Table::Make(std::make_shared<arrow::Schema>(fields),
                              empty_arrays, 0);
  }

  auto chunked_array = table_->column(0);
  std::vector<std::pair<int64_t, int64_t>> indices_in_table;
  for (const auto &target_index : indices) {
    int target_chunk_index = -1;
    int64_t offset_in_chunk = -1;
    if (FindTargetChunk(target_index, chunked_array->num_chunks(),
                        &target_chunk_index, &offset_in_chunk)) {
      indices_in_table.emplace_back(target_chunk_index, offset_in_chunk);
    } else {
      LOG_ERROR("Failed to find target chunk for index %d", target_index);
      return nullptr;
    }
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> result_columns;
  std::vector<std::shared_ptr<arrow::Field>> result_fields;

  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] == LOCAL_ROW_ID) {
      arrow::UInt64Builder builder;
      std::vector<uint64_t> u64_indices(indices.begin(), indices.end());
      auto status = builder.AppendValues(u64_indices);
      if (!status.ok()) return nullptr;
      std::shared_ptr<arrow::Array> array;
      status = builder.Finish(&array);
      if (!status.ok()) return nullptr;
      result_columns.push_back(std::make_shared<arrow::ChunkedArray>(array));
      result_fields.push_back(
          arrow::field(LOCAL_ROW_ID, arrow::uint64(), false));
    } else {
      std::shared_ptr<arrow::Array> array;
      auto col_array = table_->GetColumnByName(columns[i]);
      auto status =
          BuildArrayFromIndicesWithType(col_array, indices_in_table, &array);
      if (!status.ok()) {
        LOG_ERROR("BuildArrayFromIndices failed: %s",
                  status.ToString().c_str());
        return nullptr;
      }
      result_columns.push_back(std::make_shared<arrow::ChunkedArray>(array));
      result_fields.push_back(physic_schema_->GetFieldByName(columns[i]));
    }
  }

  auto result_schema = std::make_shared<arrow::Schema>(result_fields);
  return arrow::Table::Make(result_schema, result_columns, indices.size());
}


ExecBatchPtr PackedForwardStore::fetch(const std::vector<std::string> &columns,
                                       int index) {
  if (!validate(columns)) return nullptr;
  if (index < 0 || index >= num_rows_) {
    LOG_ERROR("Invalid global row: %d", index);
    return nullptr;
  }

  std::vector<arrow::Datum> scalars;
  scalars.reserve(columns.size());
  for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
    int field_index = table_->schema()->GetFieldIndex(columns[col_idx]);
    auto chunked_array = table_->column(field_index);
    auto scalar_result = chunked_array->GetScalar(index);
    if (scalar_result.ok()) {
      scalars.push_back(scalar_result.ValueOrDie());
    } else {
      LOG_ERROR("Get scalar failed for column %zu, row %d: %s", col_idx, index,
                scalar_result.status().ToString().c_str());
      return nullptr;
    }
  }
  return std::make_shared<arrow::ExecBatch>(std::move(scalars), 1);
}


RecordBatchReaderPtr PackedForwardStore::scan(
    const std::vector<std::string> &columns) {
  if (!validate(columns)) return nullptr;

  std::vector<int> col_indices;
  for (auto &column : columns) {
    int idx = physic_schema_->GetFieldIndex(column);
    if (idx == -1) continue;
    col_indices.push_back(idx);
  }

  auto result = table_->SelectColumns(col_indices);
  if (!result.ok()) {
    LOG_ERROR("Failed to select columns: %s",
              result.status().message().c_str());
    return nullptr;
  }
  auto sub_table = std::move(result).ValueOrDie();
  return std::make_shared<arrow::TableBatchReader>(sub_table);
}


}  // namespace zvec
