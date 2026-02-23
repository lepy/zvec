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
#include <zvec/ailego/io/mmap_file.h>
#include <zvec/core/framework/index_unpacker.h>
#include <zvec/db/collection.h>
#include <zvec/db/schema.h>
#include <zvec/db/status.h>
#include "db/index/common/delete_store.h"
#include "db/index/segment/segment.h"
#include "db/packed/packed_id_map.h"
#include "db/sqlengine/sqlengine.h"

namespace zvec {

// Read-only Collection backed by a single .zvecpack file.
// The file is mmap'd and all data is accessed zero-copy.
class PackedCollection : public Collection {
 public:
  using Ptr = std::shared_ptr<PackedCollection>;

  static Result<Collection::Ptr> Open(const std::string &path);

  ~PackedCollection() override;

  // Read-only query API
  Result<DocPtrList> Query(const VectorQuery &query) const override;
  Result<GroupResults> GroupByQuery(
      const GroupByVectorQuery &query) const override;
  Result<DocPtrMap> Fetch(const std::vector<std::string> &pks) const override;

  // Metadata
  Result<std::string> Path() const override;
  Result<CollectionStats> Stats() const override;
  Result<CollectionSchema> Schema() const override;
  Result<CollectionOptions> Options() const override;

  // Write operations - all return read-only error
  Status Destroy() override;
  Status Flush() override;
  Status CreateIndex(const std::string &column_name,
                     const IndexParams::Ptr &index_params,
                     const CreateIndexOptions &options) override;
  Status DropIndex(const std::string &column_name) override;
  Status Optimize(const OptimizeOptions &options) override;
  Status AddColumn(const FieldSchema::Ptr &column_schema,
                   const std::string &expression,
                   const AddColumnOptions &options) override;
  Status DropColumn(const std::string &column_name) override;
  Status AlterColumn(const std::string &column_name,
                     const std::string &rename,
                     const FieldSchema::Ptr &new_column_schema,
                     const AlterColumnOptions &options) override;
  Result<WriteResults> Insert(std::vector<Doc> &docs) override;
  Result<WriteResults> Upsert(std::vector<Doc> &docs) override;
  Result<WriteResults> Update(std::vector<Doc> &docs) override;
  Result<WriteResults> Delete(const std::vector<std::string> &pks) override;
  Status DeleteByFilter(const std::string &filter) override;

 private:
  PackedCollection() = default;

  Status open_impl(const std::string &path);
  Status load_manifest(const core::IndexUnpacker &unpacker);
  Status load_idmap(const core::IndexUnpacker &unpacker);
  Status load_delete_store(const core::IndexUnpacker &unpacker);
  Status load_segments(const core::IndexUnpacker &unpacker);

  Segment::Ptr find_segment_by_doc_id(uint64_t doc_id) const;

  static Status read_only_error() {
    return Status::InvalidArgument("PackedCollection is read-only");
  }

  std::string path_;
  std::shared_ptr<ailego::MMapFile> mmap_file_;
  CollectionSchema::Ptr schema_;
  PackedIDMap::Ptr id_map_;
  DeleteStore::Ptr delete_store_;
  std::vector<Segment::Ptr> segments_;
  sqlengine::SQLEngine::Ptr sql_engine_;
};

}  // namespace zvec
