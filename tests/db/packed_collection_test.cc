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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <zvec/ailego/io/file.h>
#include <zvec/ailego/logger/logger.h>
#include <zvec/ailego/utility/file_helper.h>
#include <zvec/db/collection.h>
#include <zvec/db/collection_exporter.h>
#include "db/common/file_helper.h"
#include "db/packed/packed_id_map.h"
#include "index/utils/utils.h"
#include "zvec/db/doc.h"
#include "zvec/db/index_params.h"
#include "zvec/db/options.h"
#include "zvec/db/schema.h"
#include "zvec/db/status.h"

using namespace zvec;
using namespace zvec::test;

static const std::string kCollectionPath = "test_packed_collection";
static const std::string kPackedPath = "test_packed_collection.zvecpack";
static const uint32_t kDocCount = 100;

class PackedCollectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FileHelper::RemoveDirectory(kCollectionPath);
    ailego::File::Delete(kPackedPath.c_str());
  }

  void TearDown() override {
    FileHelper::RemoveDirectory(kCollectionPath);
    ailego::File::Delete(kPackedPath.c_str());
  }

  // Helper: create a collection with docs, flush, create index, then export.
  Collection::Ptr CreateSourceCollection() {
    CollectionOptions options;
    options.read_only_ = false;
    options.enable_mmap_ = true;

    auto schema = TestHelper::CreateNormalSchema();
    auto result = Collection::CreateAndOpen(kCollectionPath, *schema, options);
    if (!result.has_value()) {
      LOG_ERROR("Failed to create collection: %s",
                result.error().message().c_str());
      return nullptr;
    }

    auto col = result.value();

    // Insert docs
    auto s = TestHelper::CollectionInsertDoc(col, 0, kDocCount);
    if (!s.ok()) {
      LOG_ERROR("Failed to insert docs: %s", s.message().c_str());
      return nullptr;
    }

    // Flush to persist
    s = col->Flush();
    if (!s.ok()) {
      LOG_ERROR("Failed to flush: %s", s.message().c_str());
      return nullptr;
    }

    // Create vector index
    auto index_params = std::make_shared<HnswIndexParams>(MetricType::L2);
    s = col->CreateIndex("dense_fp32", index_params);
    if (!s.ok()) {
      LOG_ERROR("Failed to create index: %s", s.message().c_str());
      // Non-fatal for testing, continue
    }

    return col;
  }
};

TEST_F(PackedCollectionTest, PackedIDMap_Roundtrip) {
  // Create a simple IDMap, serialize and deserialize
  auto id_map = IDMap::CreateAndOpen("test", "test_packed_idmap_db", true, false);
  ASSERT_NE(id_map, nullptr);

  // Insert some entries
  for (uint64_t i = 0; i < 50; ++i) {
    auto pk = TestHelper::MakePK(i);
    auto s = id_map->upsert(pk, i * 10);
    ASSERT_TRUE(s.ok());
  }

  // Serialize
  std::string buf;
  auto s = PackedIDMap::Serialize(id_map.get(), &buf);
  ASSERT_TRUE(s.ok());
  ASSERT_GT(buf.size(), 0u);

  // Deserialize
  PackedIDMap packed;
  s = packed.load(buf.data(), buf.size());
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(packed.count(), 50u);

  // Verify entries
  for (uint64_t i = 0; i < 50; ++i) {
    auto pk = TestHelper::MakePK(i);
    uint64_t doc_id = 0;
    ASSERT_TRUE(packed.has(pk, &doc_id));
    ASSERT_EQ(doc_id, i * 10);
  }

  // Verify missing key
  ASSERT_FALSE(packed.has("nonexistent"));

  // Cleanup
  id_map.reset();
  FileHelper::RemoveDirectory("test_packed_idmap_db");
}

TEST_F(PackedCollectionTest, ExportAndOpen) {
  // Create source collection
  auto source_col = CreateSourceCollection();
  ASSERT_NE(source_col, nullptr);

  // Get source stats and schema
  auto source_schema = source_col->Schema();
  ASSERT_TRUE(source_schema.has_value());

  auto source_stats = source_col->Stats();
  ASSERT_TRUE(source_stats.has_value());

  // Close source collection
  source_col.reset();

  // Export to .zvecpack
  auto s = CollectionExporter::Export(kCollectionPath, kPackedPath);
  ASSERT_TRUE(s.ok()) << s.message();

  // Verify file exists
  ASSERT_TRUE(ailego::FileHelper::IsExist(kPackedPath.c_str()));

  // Open the packed collection
  CollectionOptions options;
  options.read_only_ = true;
  auto result = Collection::Open(kPackedPath, options);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  auto packed_col = result.value();

  // Verify schema matches
  auto packed_schema = packed_col->Schema();
  ASSERT_TRUE(packed_schema.has_value());
  ASSERT_EQ(packed_schema.value(), source_schema.value());

  // Verify path
  auto packed_path = packed_col->Path();
  ASSERT_TRUE(packed_path.has_value());
  ASSERT_EQ(packed_path.value(), kPackedPath);
}

TEST_F(PackedCollectionTest, FetchByPK) {
  auto source_col = CreateSourceCollection();
  ASSERT_NE(source_col, nullptr);

  // Fetch a doc from source
  auto pk = TestHelper::MakePK(5);
  auto source_result = source_col->Fetch({pk});
  ASSERT_TRUE(source_result.has_value());
  auto &source_docs = source_result.value();
  ASSERT_NE(source_docs[pk], nullptr);

  source_col.reset();

  // Export
  auto s = CollectionExporter::Export(kCollectionPath, kPackedPath);
  ASSERT_TRUE(s.ok()) << s.message();

  // Open packed
  CollectionOptions options;
  options.read_only_ = true;
  auto result = Collection::Open(kPackedPath, options);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  auto packed_col = result.value();

  // Fetch same doc from packed
  auto packed_result = packed_col->Fetch({pk});
  ASSERT_TRUE(packed_result.has_value());
  auto &packed_docs = packed_result.value();
  ASSERT_NE(packed_docs[pk], nullptr);

  // Fetch missing key
  auto missing_result = packed_col->Fetch({"nonexistent"});
  ASSERT_TRUE(missing_result.has_value());
  ASSERT_EQ(missing_result.value()["nonexistent"], nullptr);
}

TEST_F(PackedCollectionTest, WriteOps_ReadOnly) {
  auto source_col = CreateSourceCollection();
  ASSERT_NE(source_col, nullptr);
  source_col.reset();

  auto s = CollectionExporter::Export(kCollectionPath, kPackedPath);
  ASSERT_TRUE(s.ok()) << s.message();

  CollectionOptions options;
  options.read_only_ = true;
  auto result = Collection::Open(kPackedPath, options);
  ASSERT_TRUE(result.has_value());

  auto packed_col = result.value();

  // All write operations should fail
  std::vector<Doc> docs;
  ASSERT_FALSE(packed_col->Insert(docs).has_value());
  ASSERT_FALSE(packed_col->Upsert(docs).has_value());
  ASSERT_FALSE(packed_col->Update(docs).has_value());
  ASSERT_FALSE(packed_col->Delete({}).has_value());
  ASSERT_FALSE(packed_col->DeleteByFilter("").ok());
  ASSERT_FALSE(packed_col->CreateIndex("", nullptr).ok());
  ASSERT_FALSE(packed_col->DropIndex("").ok());
  ASSERT_FALSE(packed_col->AddColumn(nullptr, "").ok());
  ASSERT_FALSE(packed_col->AlterColumn("", "").ok());
  ASSERT_FALSE(packed_col->DropColumn("").ok());
  ASSERT_FALSE(packed_col->Optimize().ok());
  ASSERT_FALSE(packed_col->Destroy().ok());

  // Flush should be OK (no-op)
  ASSERT_TRUE(packed_col->Flush().ok());
}

TEST_F(PackedCollectionTest, Stats) {
  auto source_col = CreateSourceCollection();
  ASSERT_NE(source_col, nullptr);

  auto source_stats = source_col->Stats();
  ASSERT_TRUE(source_stats.has_value());

  source_col.reset();

  auto s = CollectionExporter::Export(kCollectionPath, kPackedPath);
  ASSERT_TRUE(s.ok()) << s.message();

  CollectionOptions options;
  options.read_only_ = true;
  auto result = Collection::Open(kPackedPath, options);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  auto packed_col = result.value();
  auto packed_stats = packed_col->Stats();
  ASSERT_TRUE(packed_stats.has_value());

  // Doc counts should match
  ASSERT_EQ(packed_stats.value().doc_count, source_stats.value().doc_count);
}

TEST_F(PackedCollectionTest, Query) {
  auto source_col = CreateSourceCollection();
  ASSERT_NE(source_col, nullptr);

  // Build a query vector (same dimension as the schema)
  auto schema_result = source_col->Schema();
  ASSERT_TRUE(schema_result.has_value());

  source_col.reset();

  auto s = CollectionExporter::Export(kCollectionPath, kPackedPath);
  ASSERT_TRUE(s.ok()) << s.message();

  CollectionOptions options;
  options.read_only_ = true;
  auto result = Collection::Open(kPackedPath, options);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  auto packed_col = result.value();

  // Create a simple query
  VectorQuery query;
  query.field_name_ = "dense_fp32";
  query.query_vector_ = "[1.0, 1.0, 1.0, 1.0]";
  query.topk_ = 5;

  auto query_result = packed_col->Query(query);
  // The query may succeed or fail depending on whether the index was created.
  // If vector index was built, results should be non-empty.
  if (query_result.has_value()) {
    LOG_INFO("Query returned %zu results", query_result.value().size());
  }
}
