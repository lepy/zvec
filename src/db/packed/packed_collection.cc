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

#include "packed_collection.h"
#include <proto/zvec.pb.h>
#include <zvec/ailego/logger/logger.h>
#include "db/common/profiler.h"
#include "db/index/common/proto_converter.h"
#include "db/packed/packed_segment.h"

namespace zvec {

///////////////////////////////////////////////////////////////////////////////
// PackedCollection::Open
///////////////////////////////////////////////////////////////////////////////

Result<Collection::Ptr> PackedCollection::Open(const std::string &path) {
  auto collection = std::shared_ptr<PackedCollection>(new PackedCollection());
  auto s = collection->open_impl(path);
  if (!s.ok()) return tl::make_unexpected(s);
  return std::static_pointer_cast<Collection>(collection);
}

PackedCollection::~PackedCollection() = default;

///////////////////////////////////////////////////////////////////////////////
// open_impl
///////////////////////////////////////////////////////////////////////////////

Status PackedCollection::open_impl(const std::string &path) {
  path_ = path;

  // mmap the file
  mmap_file_ = std::make_shared<ailego::MMapFile>();
  if (!mmap_file_->open(path, /*rdonly=*/true, /*shared=*/true)) {
    return Status::InternalError("Failed to mmap file: " + path);
  }

  // Unpack the top-level container
  const uint8_t *base =
      static_cast<const uint8_t *>(mmap_file_->region());
  size_t total_size = mmap_file_->size();

  auto read_data = [base](size_t offset, const void **data, size_t len) -> size_t {
    *data = base + offset;
    return len;
  };

  core::IndexUnpacker unpacker;
  if (!unpacker.unpack(read_data, total_size, false)) {
    return Status::InternalError("Failed to unpack .zvecpack file");
  }

  // Load components
  auto s = load_manifest(unpacker);
  if (!s.ok()) return s;

  s = load_idmap(unpacker);
  if (!s.ok()) return s;

  s = load_delete_store(unpacker);
  if (!s.ok()) return s;

  s = load_segments(unpacker);
  if (!s.ok()) return s;

  // Init SQL engine
  auto profiler = std::make_shared<Profiler>();
  sql_engine_ = sqlengine::SQLEngine::create(profiler);

  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
// load_manifest
///////////////////////////////////////////////////////////////////////////////

Status PackedCollection::load_manifest(const core::IndexUnpacker &unpacker) {
  const auto &segments = unpacker.segments();
  auto it = segments.find("PackManifest");
  if (it == segments.end()) {
    return Status::InternalError("Missing PackManifest section");
  }

  const uint8_t *base =
      static_cast<const uint8_t *>(mmap_file_->region());
  const uint8_t *data = base + it->second.data_offset();
  size_t size = it->second.data_size();

  proto::PackedManifest manifest_pb;
  if (!manifest_pb.ParseFromArray(data, static_cast<int>(size))) {
    return Status::InternalError("Failed to parse PackedManifest");
  }

  // Convert schema
  schema_ = ProtoConverter::FromPb(manifest_pb.schema());
  if (!schema_) {
    return Status::InternalError("Failed to convert schema from proto");
  }

  // Set default index params for vector fields
  for (auto &field : schema_->fields()) {
    if (field->is_vector_field() && field->index_params() == nullptr) {
      field->set_index_params(DefaultVectorIndexParams);
    }
  }

  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
// load_idmap
///////////////////////////////////////////////////////////////////////////////

Status PackedCollection::load_idmap(const core::IndexUnpacker &unpacker) {
  const auto &segments = unpacker.segments();
  auto it = segments.find("IDMap");
  if (it == segments.end()) {
    return Status::InternalError("Missing IDMap section");
  }

  const uint8_t *base =
      static_cast<const uint8_t *>(mmap_file_->region());
  const uint8_t *data = base + it->second.data_offset();
  size_t size = it->second.data_size();

  id_map_ = std::make_shared<PackedIDMap>();
  return id_map_->load(data, size);
}

///////////////////////////////////////////////////////////////////////////////
// load_delete_store
///////////////////////////////////////////////////////////////////////////////

Status PackedCollection::load_delete_store(
    const core::IndexUnpacker &unpacker) {
  const auto &segments = unpacker.segments();
  auto it = segments.find("DeleteBitmap");

  delete_store_ = std::make_shared<DeleteStore>("packed");

  if (it == segments.end()) {
    // No delete bitmap -> no deletions
    return Status::OK();
  }

  const uint8_t *base =
      static_cast<const uint8_t *>(mmap_file_->region());
  const uint8_t *data = base + it->second.data_offset();
  size_t size = it->second.data_size();

  return delete_store_->load_from_buffer(data, size);
}

///////////////////////////////////////////////////////////////////////////////
// load_segments
///////////////////////////////////////////////////////////////////////////////

Status PackedCollection::load_segments(const core::IndexUnpacker &unpacker) {
  const auto &all_sections = unpacker.segments();
  const uint8_t *base =
      static_cast<const uint8_t *>(mmap_file_->region());

  // Re-parse the manifest to get segment metas
  auto it = all_sections.find("PackManifest");
  if (it == all_sections.end()) {
    return Status::InternalError("Missing PackManifest section");
  }

  proto::PackedManifest manifest_pb;
  if (!manifest_pb.ParseFromArray(
          base + it->second.data_offset(),
          static_cast<int>(it->second.data_size()))) {
    return Status::InternalError("Failed to parse PackedManifest");
  }

  // Build section map for each segment
  for (int i = 0; i < manifest_pb.persisted_segment_metas_size(); ++i) {
    auto seg_meta_ptr = ProtoConverter::FromPb(
        manifest_pb.persisted_segment_metas(i));
    if (!seg_meta_ptr) {
      return Status::InternalError("Failed to convert segment meta from proto");
    }

    auto seg_id = seg_meta_ptr->id();

    // Build section map: extract sections relevant to this segment
    PackedSegment::SectionMap section_map;
    std::string seg_prefix =
        "Forward." + std::to_string(seg_id) + ".";
    std::string vec_prefix =
        "VecIdx." + std::to_string(seg_id) + ".";
    std::string qvec_prefix =
        "QVecIdx." + std::to_string(seg_id) + ".";

    for (const auto &[name, meta] : all_sections) {
      if (name.rfind(seg_prefix, 0) == 0 ||
          name.rfind(vec_prefix, 0) == 0 ||
          name.rfind(qvec_prefix, 0) == 0) {
        section_map[name] = std::make_pair(
            base + meta.data_offset(), meta.data_size());
      }
    }

    // shared_ptr<void> lifetime that prevents mmap deallocation
    std::shared_ptr<void> lifetime = mmap_file_;

    auto segment = PackedSegment::Open(
        *schema_, *seg_meta_ptr, delete_store_, section_map, lifetime);
    if (!segment.has_value()) {
      return segment.error();
    }
    segments_.push_back(segment.value());
  }

  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////
// find_segment_by_doc_id (binary search, segments sorted by min_doc_id)
///////////////////////////////////////////////////////////////////////////////

Segment::Ptr PackedCollection::find_segment_by_doc_id(
    uint64_t doc_id) const {
  size_t left = 0;
  size_t right = segments_.size();
  while (left < right) {
    size_t mid = left + (right - left) / 2;
    uint64_t min_id = segments_[mid]->meta()->min_doc_id();
    uint64_t max_id = segments_[mid]->meta()->max_doc_id();
    if (doc_id < min_id) {
      right = mid;
    } else if (doc_id > max_id) {
      left = mid + 1;
    } else {
      return segments_[mid];
    }
  }
  return nullptr;
}

///////////////////////////////////////////////////////////////////////////////
// Query API
///////////////////////////////////////////////////////////////////////////////

Result<DocPtrList> PackedCollection::Query(const VectorQuery &query) const {
  auto s = query.validate(schema_->get_vector_field(query.field_name_));
  if (!s.ok()) return tl::make_unexpected(s);

  if (segments_.empty()) return DocPtrList();

  return sql_engine_->execute(schema_, query, segments_);
}

Result<GroupResults> PackedCollection::GroupByQuery(
    const GroupByVectorQuery &query) const {
  if (segments_.empty()) return GroupResults();

  return sql_engine_->execute_group_by(schema_, query, segments_);
}

Result<DocPtrMap> PackedCollection::Fetch(
    const std::vector<std::string> &pks) const {
  DocPtrMap results;

  for (const auto &pk : pks) {
    uint64_t doc_id;
    bool has = id_map_->has(pk, &doc_id);
    if (!has) {
      results.insert({pk, nullptr});
      continue;
    }
    if (delete_store_ && delete_store_->is_deleted(doc_id)) {
      results.insert({pk, nullptr});
      continue;
    }
    auto segment = find_segment_by_doc_id(doc_id);
    if (!segment) {
      LOG_WARN("doc_id: %zu segment not found", (size_t)doc_id);
      results.insert({pk, nullptr});
      continue;
    }
    results.insert({pk, segment->Fetch(doc_id)});
  }

  return results;
}

///////////////////////////////////////////////////////////////////////////////
// Metadata
///////////////////////////////////////////////////////////////////////////////

Result<std::string> PackedCollection::Path() const { return path_; }

Result<CollectionStats> PackedCollection::Stats() const {
  CollectionStats stats;
  auto vector_fields = schema_->vector_fields();

  if (segments_.empty()) {
    stats.doc_count = 0;
    for (auto &field : vector_fields) {
      stats.index_completeness[field->name()] = 1;
    }
    return stats;
  }

  for (auto &segment : segments_) {
    stats.doc_count += segment->doc_count(
        delete_store_ ? delete_store_->make_filter() : nullptr);
  }

  for (auto &field : vector_fields) {
    if (stats.doc_count == 0) {
      stats.index_completeness[field->name()] = 1;
      continue;
    }
    uint32_t indexed_doc_count = 0;
    for (auto &segment : segments_) {
      if (segment->meta()->vector_indexed(field->name())) {
        indexed_doc_count += segment->doc_count(
            delete_store_ ? delete_store_->make_filter() : nullptr);
      }
    }
    stats.index_completeness[field->name()] =
        indexed_doc_count * 1.0 / stats.doc_count;
  }

  return stats;
}

Result<CollectionSchema> PackedCollection::Schema() const {
  return *schema_;
}

Result<CollectionOptions> PackedCollection::Options() const {
  CollectionOptions options;
  options.read_only_ = true;
  return options;
}

///////////////////////////////////////////////////////////////////////////////
// Write operations - all read-only errors
///////////////////////////////////////////////////////////////////////////////

Status PackedCollection::Destroy() { return read_only_error(); }
Status PackedCollection::Flush() { return Status::OK(); }

Status PackedCollection::CreateIndex(const std::string &, const IndexParams::Ptr &,
                                     const CreateIndexOptions &) {
  return read_only_error();
}

Status PackedCollection::DropIndex(const std::string &) {
  return read_only_error();
}

Status PackedCollection::Optimize(const OptimizeOptions &) {
  return read_only_error();
}

Status PackedCollection::AddColumn(const FieldSchema::Ptr &,
                                   const std::string &,
                                   const AddColumnOptions &) {
  return read_only_error();
}

Status PackedCollection::DropColumn(const std::string &) {
  return read_only_error();
}

Status PackedCollection::AlterColumn(const std::string &, const std::string &,
                                     const FieldSchema::Ptr &,
                                     const AlterColumnOptions &) {
  return read_only_error();
}

Result<WriteResults> PackedCollection::Insert(std::vector<Doc> &) {
  return tl::make_unexpected(read_only_error());
}

Result<WriteResults> PackedCollection::Upsert(std::vector<Doc> &) {
  return tl::make_unexpected(read_only_error());
}

Result<WriteResults> PackedCollection::Update(std::vector<Doc> &) {
  return tl::make_unexpected(read_only_error());
}

Result<WriteResults> PackedCollection::Delete(
    const std::vector<std::string> &) {
  return tl::make_unexpected(read_only_error());
}

Status PackedCollection::DeleteByFilter(const std::string &) {
  return read_only_error();
}

}  // namespace zvec
