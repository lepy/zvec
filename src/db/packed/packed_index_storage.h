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

#include <cstring>
#include <memory>
#include <zvec/core/framework/index_storage.h>

namespace zvec {

// Read-only IndexStorage::Segment backed by a region in an mmap'd file.
// The lifetime holder ensures the mmap stays alive while this segment exists.
class MMapSliceSegment : public core::IndexStorage::Segment,
                         public std::enable_shared_from_this<MMapSliceSegment> {
 public:
  using Pointer = std::shared_ptr<MMapSliceSegment>;

  MMapSliceSegment(const uint8_t *data, size_t size,
                   std::shared_ptr<void> lifetime)
      : data_(data), size_(size), lifetime_(std::move(lifetime)) {}

  size_t data_size() const override { return size_; }
  uint32_t data_crc() const override { return 0; }
  size_t padding_size() const override { return 0; }
  size_t capacity() const override { return size_; }

  size_t fetch(size_t offset, void *buf, size_t len) const override {
    if (offset + len > size_) {
      len = (offset < size_) ? size_ - offset : 0;
    }
    if (len > 0) {
      memcpy(buf, data_ + offset, len);
    }
    return len;
  }

  size_t read(size_t offset, const void **out, size_t len) override {
    if (offset + len > size_) {
      len = (offset < size_) ? size_ - offset : 0;
    }
    *out = data_ + offset;
    return len;
  }

  size_t read(size_t offset, core::IndexStorage::MemoryBlock &data,
              size_t len) override {
    if (offset + len > size_) {
      len = (offset < size_) ? size_ - offset : 0;
    }
    data.reset(const_cast<void *>(static_cast<const void *>(data_ + offset)));
    return len;
  }

  bool read(core::IndexStorage::SegmentData *iovec, size_t count) override {
    for (size_t i = 0; i < count; ++i) {
      if (iovec[i].offset + iovec[i].length > size_) return false;
      iovec[i].data = data_ + iovec[i].offset;
    }
    return true;
  }

  // Read-only: write and resize are no-ops
  size_t write(size_t, const void *, size_t) override { return 0; }
  size_t resize(size_t) override { return 0; }
  void update_data_crc(uint32_t) override {}

  core::IndexStorage::Segment::Pointer clone() override {
    return shared_from_this();
  }

 private:
  const uint8_t *data_;
  size_t size_;
  std::shared_ptr<void> lifetime_;  // Prevents mmap deallocation
};

}  // namespace zvec
