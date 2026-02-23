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

#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <zvec/db/status.h>
#include "db/index/storage/base_forward_store.h"

namespace zvec {

// Forward store that reads Arrow IPC data from an in-memory buffer (zero-copy).
class PackedForwardStore : public BaseForwardStore {
 public:
  using Ptr = std::shared_ptr<PackedForwardStore>;

  PackedForwardStore(const void *data, size_t size);

  Status Open() override;

  TablePtr fetch(const std::vector<std::string> &columns,
                 const std::vector<int> &indices) override;

  ExecBatchPtr fetch(const std::vector<std::string> &columns,
                     int index) override;

  RecordBatchReaderPtr scan(const std::vector<std::string> &columns) override;

  const std::shared_ptr<arrow::Schema> physic_schema() const override {
    return physic_schema_;
  }

  TablePtr get_table() override { return table_; }

 private:
  bool validate(const std::vector<std::string> &columns) const;
  bool FindTargetChunk(int target_index, int num_chunks,
                       int *target_chunk_index, int64_t *offset_in_chunk);

  const void *data_;
  size_t size_;
  std::shared_ptr<arrow::Schema> physic_schema_;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_file_reader_;
  std::shared_ptr<arrow::Table> table_;
  int64_t num_rows_{0};
  std::vector<std::pair<int64_t, int64_t>> chunk_index_map_;
  bool is_fixed_batch_size_{true};
  int64_t fixed_batch_size_{-1};
};

}  // namespace zvec
