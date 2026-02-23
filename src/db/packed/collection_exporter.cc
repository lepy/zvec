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

#include "collection_exporter.h"
#include <chrono>
#include <proto/zvec.pb.h>
#include <zvec/ailego/hash/crc32c.h>
#include <zvec/ailego/io/file.h>
#include <zvec/ailego/logger/logger.h>
#include <zvec/ailego/utility/string_helper.h>
#include <zvec/core/framework/index_factory.h>
#include <zvec/core/framework/index_packer.h>
#include <zvec/db/collection.h>
#include "db/common/file_helper.h"
#include "db/common/typedef.h"
#include "db/index/common/id_map.h"
#include "db/index/common/proto_converter.h"
#include "db/index/common/version_manager.h"
#include "db/packed/packed_id_map.h"

namespace zvec {

// Helper: write a section to the packer and register its metadata.
static Status write_section(core::IndexDumper *dumper,
                            const std::string &name, const void *data,
                            size_t size) {
  // Pad to 32-byte alignment
  size_t padding = ((size + 31u) & ~31u) - size;
  size_t written = dumper->write(data, size);
  if (written != size) {
    return Status::InternalError("Failed to write section: " + name);
  }
  if (padding > 0) {
    std::string pad_buf(padding, '\0');
    written = dumper->write(pad_buf.data(), padding);
    if (written != padding) {
      return Status::InternalError("Failed to write padding for: " + name);
    }
  }
  uint32_t crc = ailego::Crc32c::Hash(data, size, 0);
  dumper->append(name, size, padding, crc);
  return Status::OK();
}

// Helper: read an entire file into a string buffer.
static Status read_file(const std::string &path, std::string *out) {
  ailego::File file;
  if (!file.open(path.c_str(), true, false)) {
    return Status::InternalError("Failed to open file: " + path);
  }
  size_t file_size = file.size();
  out->resize(file_size);
  if (file.read(out->data(), file_size) != file_size) {
    return Status::InternalError("Failed to read file: " + path);
  }
  return Status::OK();
}

Status CollectionExporter::Export(const std::string &collection_path,
                                  const std::string &output_path) {
  // 1. Open source collection in read-only mode
  CollectionOptions options;
  options.read_only_ = true;
  auto coll_result = Collection::Open(collection_path, options);
  if (!coll_result.has_value()) {
    return coll_result.error();
  }

  // 2. Extract version info (schema + segment metas)
  auto version_manager = VersionManager::Recovery(collection_path);
  if (!version_manager.has_value()) {
    return version_manager.error();
  }
  const auto &version = version_manager.value()->get_current_version();
  const auto &schema = version.schema();
  auto segment_metas = version.persisted_segment_metas();

  // 3. Build PackedManifest proto
  proto::PackedManifest manifest_pb;
  manifest_pb.set_pack_version(1);
  *manifest_pb.mutable_schema() = ProtoConverter::ToPb(schema);

  uint64_t total_doc_count = 0;
  for (const auto &seg_meta : segment_metas) {
    *manifest_pb.add_persisted_segment_metas() =
        ProtoConverter::ToPb(*seg_meta);
    total_doc_count += seg_meta->doc_count();
  }

  // Also include writing segment if it has docs
  auto writing_meta = version.writing_segment_meta();
  if (writing_meta && writing_meta->doc_count() > 0) {
    *manifest_pb.add_persisted_segment_metas() =
        ProtoConverter::ToPb(*writing_meta);
    total_doc_count += writing_meta->doc_count();
    segment_metas.push_back(writing_meta);
  }

  manifest_pb.set_total_doc_count(total_doc_count);
  manifest_pb.set_export_time(
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  manifest_pb.set_source_path(collection_path);

  // Paths for IDMap and DeleteStore based on version
  auto del_path = FileHelper::MakeFilePath(
      collection_path, FileID::DELETE_FILE,
      version.delete_snapshot_path_suffix());
  auto idmap_path = FileHelper::MakeFilePath(
      collection_path, FileID::ID_FILE,
      version.id_map_path_suffix());

  // 4. Create FileDumper for output
  auto dumper = core::IndexFactory::CreateDumper("FileDumper");
  if (!dumper) {
    return Status::InternalError("Failed to create FileDumper");
  }
  if (0 != dumper->create(output_path)) {
    return Status::InternalError("Failed to create output file: " + output_path);
  }

  // 5. Write PackManifest section
  std::string manifest_buf;
  manifest_pb.SerializeToString(&manifest_buf);
  auto s = write_section(dumper.get(), "PackManifest",
                         manifest_buf.data(), manifest_buf.size());
  if (!s.ok()) return s;

  // 6. Write IDMap section
  auto id_map = IDMap::CreateAndOpen(
      "export", idmap_path, false, true);
  if (!id_map) {
    return Status::InternalError("Failed to open IDMap");
  }
  std::string idmap_buf;
  s = PackedIDMap::Serialize(id_map.get(), &idmap_buf);
  if (!s.ok()) return s;
  s = write_section(dumper.get(), "IDMap", idmap_buf.data(), idmap_buf.size());
  if (!s.ok()) return s;

  // 7. Write DeleteBitmap section
  if (FileHelper::FileExists(del_path)) {
    std::string del_buf;
    s = read_file(del_path, &del_buf);
    if (!s.ok()) return s;
    s = write_section(dumper.get(), "DeleteBitmap",
                      del_buf.data(), del_buf.size());
    if (!s.ok()) return s;
  }

  // 8. Write sections for each segment
  for (const auto &seg_meta : segment_metas) {
    auto seg_id = seg_meta->id();

    for (const auto &block : seg_meta->persisted_blocks()) {
      if (block.type() == BlockType::SCALAR) {
        // Forward block: read .ipc file
        std::string fwd_path = FileHelper::MakeForwardBlockPath(
            collection_path, seg_id, block.id());
        if (!FileHelper::FileExists(fwd_path)) {
          // Try parquet
          fwd_path = FileHelper::MakeForwardBlockPath(
              collection_path, seg_id, block.id(), true);
        }
        std::string fwd_buf;
        s = read_file(fwd_path, &fwd_buf);
        if (!s.ok()) return s;

        std::string section_name = "Forward." + std::to_string(seg_id) +
                                   "." + std::to_string(block.id());
        s = write_section(dumper.get(), section_name,
                          fwd_buf.data(), fwd_buf.size());
        if (!s.ok()) return s;

      } else if (block.type() == BlockType::VECTOR_INDEX) {
        if (block.columns().empty()) continue;
        const std::string &col_name = block.columns()[0];

        std::string vec_path = FileHelper::MakeVectorIndexPath(
            collection_path, col_name, seg_id, block.id());
        std::string vec_buf;
        s = read_file(vec_path, &vec_buf);
        if (!s.ok()) return s;

        std::string section_name = "VecIdx." + std::to_string(seg_id) +
                                   "." + col_name + "." +
                                   std::to_string(block.id());
        s = write_section(dumper.get(), section_name,
                          vec_buf.data(), vec_buf.size());
        if (!s.ok()) return s;

      } else if (block.type() == BlockType::VECTOR_INDEX_QUANTIZE) {
        if (block.columns().empty()) continue;
        const std::string &col_name = block.columns()[0];

        std::string qvec_path = FileHelper::MakeQuantizeVectorIndexPath(
            collection_path, col_name, seg_id, block.id());
        std::string qvec_buf;
        s = read_file(qvec_path, &qvec_buf);
        if (!s.ok()) return s;

        std::string section_name = "QVecIdx." + std::to_string(seg_id) +
                                   "." + col_name + "." +
                                   std::to_string(block.id());
        s = write_section(dumper.get(), section_name,
                          qvec_buf.data(), qvec_buf.size());
        if (!s.ok()) return s;
      }
    }

    // Also export forward data from writing block if present
    if (seg_meta->has_writing_forward_block()) {
      auto &wb = seg_meta->writing_forward_block().value();
      if (wb.doc_count() > 0) {
        std::string fwd_path = FileHelper::MakeForwardBlockPath(
            collection_path, seg_id, wb.id());
        if (FileHelper::FileExists(fwd_path)) {
          std::string fwd_buf;
          s = read_file(fwd_path, &fwd_buf);
          if (!s.ok()) return s;

          std::string section_name = "Forward." + std::to_string(seg_id) +
                                     "." + std::to_string(wb.id());
          s = write_section(dumper.get(), section_name,
                            fwd_buf.data(), fwd_buf.size());
          if (!s.ok()) return s;
        }
      }
    }
  }

  // 9. Close (finalize footer + TOC)
  if (0 != dumper->close()) {
    return Status::InternalError("Failed to finalize .zvecpack file");
  }

  LOG_INFO("Exported collection to %s (docs: %lu)", output_path.c_str(),
           total_doc_count);
  return Status::OK();
}

}  // namespace zvec
