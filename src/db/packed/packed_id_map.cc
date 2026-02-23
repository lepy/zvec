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

#include "packed_id_map.h"
#include <cstring>
#include <zvec/ailego/logger/logger.h>
#include "db/common/constants.h"


namespace zvec {


Status PackedIDMap::load(const void *data, size_t size) {
  map_.clear();
  const uint8_t *ptr = static_cast<const uint8_t *>(data);
  const uint8_t *end = ptr + size;

  if (size < sizeof(uint64_t)) {
    return Status::InvalidArgument("PackedIDMap buffer too small");
  }

  uint64_t count;
  memcpy(&count, ptr, sizeof(uint64_t));
  ptr += sizeof(uint64_t);

  map_.reserve(count);

  for (uint64_t i = 0; i < count; ++i) {
    if (ptr + sizeof(uint16_t) > end) {
      return Status::InvalidArgument("PackedIDMap: unexpected end of buffer");
    }
    uint16_t key_len;
    memcpy(&key_len, ptr, sizeof(uint16_t));
    ptr += sizeof(uint16_t);

    if (ptr + key_len + sizeof(uint64_t) > end) {
      return Status::InvalidArgument("PackedIDMap: unexpected end of buffer");
    }
    std::string key(reinterpret_cast<const char *>(ptr), key_len);
    ptr += key_len;

    uint64_t doc_id;
    memcpy(&doc_id, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    map_.emplace(std::move(key), doc_id);
  }

  LOG_INFO("PackedIDMap loaded %zu entries", map_.size());
  return Status::OK();
}


bool PackedIDMap::has(const std::string &key, uint64_t *doc_id) const {
  auto it = map_.find(key);
  if (it != map_.end()) {
    if (doc_id) {
      *doc_id = it->second;
    }
    return true;
  }
  if (doc_id) {
    *doc_id = INVALID_DOC_ID;
  }
  return false;
}


Status PackedIDMap::multi_get(const std::vector<std::string> &keys,
                              std::vector<uint64_t> *doc_ids) const {
  if (keys.empty()) {
    doc_ids->clear();
    return Status::InvalidArgument("Empty keys");
  }

  doc_ids->resize(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    auto it = map_.find(keys[i]);
    if (it != map_.end()) {
      (*doc_ids)[i] = it->second;
    } else {
      (*doc_ids)[i] = INVALID_DOC_ID;
    }
  }
  return Status::OK();
}


Status PackedIDMap::Serialize(IDMap *source, std::string *out) {
  // First pass: count entries and compute size
  uint64_t count = 0;
  size_t total_size = sizeof(uint64_t);  // count header

  auto s = source->iterate(
      [&count, &total_size](const std::string &key, uint64_t /*doc_id*/) {
        count++;
        total_size += sizeof(uint16_t) + key.size() + sizeof(uint64_t);
        return true;
      });
  if (!s.ok()) return s;

  out->resize(total_size);
  uint8_t *ptr = reinterpret_cast<uint8_t *>(out->data());

  memcpy(ptr, &count, sizeof(uint64_t));
  ptr += sizeof(uint64_t);

  // Second pass: write entries
  s = source->iterate(
      [&ptr](const std::string &key, uint64_t doc_id) {
        uint16_t key_len = static_cast<uint16_t>(key.size());
        memcpy(ptr, &key_len, sizeof(uint16_t));
        ptr += sizeof(uint16_t);
        memcpy(ptr, key.data(), key_len);
        ptr += key_len;
        memcpy(ptr, &doc_id, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        return true;
      });

  return s;
}


}  // namespace zvec
