/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MAPOUTPUTSPEC_H_
#define MAPOUTPUTSPEC_H_

#include <string>
#include "util/Checksum.h"
#include "util/WritableUtils.h"
#include "NativeTask.h"

namespace NativeTask {

using std::string;

/**
 * internal sort method
 */
enum SortAlgorithm {
  CQSORT = 0,
  CPPSORT = 1,
  DUALPIVOTSORT = 2,
};

/**
 * spill file type
 * INTERMEDIATE: a simple key/value sequence file
 * IFILE: classic hadoop IFile
 */
enum OutputFileType {
  INTERMEDIATE = 0,
  IFILE = 1,
};

/**
 * key/value recored order requirements
 * FULLSORT: hadoop  standard
 * GROUPBY:  same key are grouped together, but not in order
 * NOSORT:   no order at all
 */
enum SortOrder {
  FULLORDER = 0,
  GROUPBY = 1,
  NOSORT = 2,
};

enum CompressionType {
  PLAIN = 0,
  SNAPPY = 1,
};

class MapOutputSpec {
public:
  KeyValueType keyType;
  KeyValueType valueType;
  SortOrder sortOrder;
  SortAlgorithm sortAlgorithm;
  string codec;
  ChecksumType checksumType;

  static void getSpecFromConfig(Config * config, MapOutputSpec & spec);
};

} // namespace NativeTask

#endif /* MAPOUTPUTSPEC_H_ */
