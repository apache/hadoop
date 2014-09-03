/*
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

#ifndef MAP_OUTPUT_COLLECTOR_H_
#define MAP_OUTPUT_COLLECTOR_H_

#include "NativeTask.h"
#include "lib/MemoryPool.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"
#include "lib/PartitionBucket.h"
#include "lib/SpillOutputService.h"

namespace NativeTask {
/**
 * MapOutputCollector
 */

struct SortMetrics {
  uint64_t recordCount;
  uint64_t sortTime;

public:
  SortMetrics()
      : recordCount(0), sortTime(0) {
  }
};

class CombineRunnerWrapper : public ICombineRunner {
private:
  Config * _config;
  ICombineRunner * _combineRunner;
  bool _isJavaCombiner;
  bool _combinerInited;
  SpillOutputService * _spillOutput;

public:
  CombineRunnerWrapper(Config * config, SpillOutputService * service)
      : _config(config), _combineRunner(NULL), _isJavaCombiner(false),
          _combinerInited(false), _spillOutput(service) {
  }

  ~CombineRunnerWrapper() {
    if (!_isJavaCombiner) {
      delete _combineRunner;
    }
  }

  virtual void combine(CombineContext type, KVIterator * kvIterator, IFileWriter * writer);

private:
  ICombineRunner * createCombiner();
};

class MapOutputCollector {
  static const uint32_t DEFAULT_MIN_BLOCK_SIZE = 16 * 1024;
  static const uint32_t DEFAULT_MAX_BLOCK_SIZE = 4 * 1024 * 1024;

private:
  Config * _config;

  uint32_t _numPartitions;
  PartitionBucket ** _buckets;

  ComparatorPtr _keyComparator;

  ICombineRunner * _combineRunner;

  Counter * _mapOutputRecords;
  Counter * _mapOutputBytes;
  Counter * _mapOutputMaterializedBytes;
  Counter * _spilledRecords;

  SpillOutputService * _spillOutput;

  uint32_t _defaultBlockSize;

  SpillInfos _spillInfos;

  MapOutputSpec _spec;

  Timer _collectTimer;

  MemoryPool * _pool;

public:
  MapOutputCollector(uint32_t num_partition, SpillOutputService * spillService);

  ~MapOutputCollector();

  void configure(Config * config);

  /**
   * collect one k/v pair
   * @return true success; false buffer full, need spill
   */
  bool collect(const void * key, uint32_t keylen, const void * value, uint32_t vallen,
      uint32_t partitionId);

  KVBuffer * allocateKVBuffer(uint32_t partitionId, uint32_t kvlength);

  void close();

private:
  void init(uint32_t maxBlockSize, uint32_t memory_capacity, ComparatorPtr keyComparator,
      ICombineRunner * combiner);

  void reset();

  /**
   * spill a range of partition buckets, prepare for future
   * Parallel sort & spill, TODO: parallel sort & spill
   */
  void sortPartitions(SortOrder orderType, SortAlgorithm sortType, IFileWriter * writer,
      SortMetrics & metrics);

  ComparatorPtr getComparator(Config * config, MapOutputSpec & spec);

  inline uint32_t GetCeil(uint32_t v, uint32_t unit) {
    return ((v + unit - 1) / unit) * unit;
  }

  uint32_t getDefaultBlockSize(uint32_t memoryCapacity, uint32_t partitionNum,
      uint32_t maxBlockSize) {
    uint32_t defaultBlockSize = memoryCapacity / _numPartitions / 4;
    defaultBlockSize = GetCeil(defaultBlockSize, DEFAULT_MIN_BLOCK_SIZE);
    defaultBlockSize = std::min(defaultBlockSize, maxBlockSize);
    return defaultBlockSize;
  }

  PartitionBucket * getPartition(uint32_t partition);

  /**
   * normal spill use options in _config
   * @param filepaths: spill file path
   */
  void middleSpill(const std::string & spillOutput, const std::string & indexFilePath, bool final);

  /**
   * final merge and/or spill use options in _config, and
   * previous spilled file & in-memory data
   */
  void finalSpill(const std::string & filepath, const std::string & indexpath);
};

} //namespace NativeTask

#endif /* MAP_OUTPUT_COLLECTOR_H_ */
