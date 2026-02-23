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

#include <string>
#include <zvec/db/status.h>

namespace zvec {

// Exports a regular Collection to a single .zvecpack file.
// The output file can be loaded read-only with Collection::Open().
class CollectionExporter {
 public:
  static Status Export(const std::string &collection_path,
                       const std::string &output_path);
};

}  // namespace zvec
