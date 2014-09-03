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

#include "lib/commons.h"
#include "lib/MapOutputSpec.h"
#include "NativeTask.h"

namespace NativeTask {

void MapOutputSpec::getSpecFromConfig(Config * config, MapOutputSpec & spec) {
  if (NULL == config) {
    return;
  }
  spec.checksumType = CHECKSUM_CRC32;
  string sortType = config->get(NATIVE_SORT_TYPE, "DUALPIVOTSORT");
  if (sortType == "DUALPIVOTSORT") {
    spec.sortAlgorithm = DUALPIVOTSORT;
  } else {
    spec.sortAlgorithm = CPPSORT;
  }
  if (config->get(MAPRED_COMPRESS_MAP_OUTPUT, "false") == "true") {
    spec.codec = config->get(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC);
  } else {
    spec.codec = "";
  }
  if (config->getBool(MAPRED_SORT_AVOID, false)) {
    spec.sortOrder = NOSORT;
  } else {
    spec.sortOrder = FULLORDER;
  }
  const char * key_class = config->get(MAPRED_MAPOUTPUT_KEY_CLASS);
  if (NULL == key_class) {
    key_class = config->get(MAPRED_OUTPUT_KEY_CLASS);
  }
  if (NULL == key_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.key.class not set");
  }
  spec.keyType = JavaClassToKeyValueType(key_class);
  const char * value_class = config->get(MAPRED_MAPOUTPUT_VALUE_CLASS);
  if (NULL == value_class) {
    value_class = config->get(MAPRED_OUTPUT_VALUE_CLASS);
  }
  if (NULL == value_class) {
    THROW_EXCEPTION(IOException, "mapred.mapoutput.value.class not set");
  }
  spec.valueType = JavaClassToKeyValueType(value_class);
}

} // namespace NativeTask
