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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <zvec/db/status.h>
#include "db/index/common/id_map.h"

namespace zvec {

// Format: [uint64_t count][{uint16_t key_len, char key[], uint64_t doc_id}...]
class PackedIDMap {
 public:
  using Ptr = std::shared_ptr<PackedIDMap>;

  PackedIDMap() = default;

  Status load(const void *data, size_t size);

  bool has(const std::string &key, uint64_t *doc_id = nullptr) const;

  Status multi_get(const std::vector<std::string> &keys,
                   std::vector<uint64_t> *doc_ids) const;

  size_t count() const { return map_.size(); }

  static Status Serialize(IDMap *source, std::string *out);

 private:
  std::unordered_map<std::string, uint64_t> map_;
};

}  // namespace zvec
