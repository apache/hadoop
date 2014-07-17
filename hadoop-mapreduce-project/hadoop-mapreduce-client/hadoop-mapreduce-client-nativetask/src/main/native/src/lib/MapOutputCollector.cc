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

#include "commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "FileSystem.h"
#include "NativeObjectFactory.h"
#include "MapOutputCollector.h"
#include "Merge.h"
#include "NativeTask.h"
#include "WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "Combiner.h"
#include "TaskCounters.h"
#include "MinHeap.h"

namespace NativeTask {

ICombineRunner * CombineRunnerWrapper::createCombiner() {

  ICombineRunner * combineRunner = NULL;
  if (NULL != _config->get(NATIVE_COMBINER)) {
    const char * combinerClass = _config->get(NATIVE_COMBINER);
    ObjectCreatorFunc objectCreater = NativeObjectFactory::GetObjectCreator(combinerClass);
    if (NULL == objectCreater) {
      THROW_EXCEPTION_EX(UnsupportException, "Combiner not found: %s", combinerClass);
    } else {
      LOG("[MapOutputCollector::configure] native combiner is enabled: %s", combinerClass);
    }
    combineRunner = new NativeCombineRunner(_config, objectCreater);
  } else {
    CombineHandler * javaCombiner = _spillOutput->getJavaCombineHandler();
    if (NULL != javaCombiner) {
      _isJavaCombiner = true;
      combineRunner = (ICombineRunner *)javaCombiner;
    } else {
      LOG("[MapOutputCollector::getCombiner] cannot get combine handler from java");
    }
  }
  return combineRunner;
}

void CombineRunnerWrapper::combine(CombineContext type, KVIterator * kvIterator,
    IFileWriter * writer) {

  if (!_combinerInited) {
    _combineRunner = createCombiner();
    _combinerInited = true;
  }

  if (NULL != _combineRunner) {
    _combineRunner->combine(type, kvIterator, writer);
  } else {
    LOG("[CombineRunnerWrapper::combine] no valid combiner");
  }
}

/////////////////////////////////////////////////////////////////
// MapOutputCollector
/////////////////////////////////////////////////////////////////

MapOutputCollector::MapOutputCollector(uint32_t numberPartitions, SpillOutputService * spillService)
    : _config(NULL), _buckets(NULL), _keyComparator(NULL), _defaultBlockSize(0),
        _combineRunner(NULL), _spilledRecords(NULL), _spillOutput(spillService), _pool(NULL),
        _numPartitions(numberPartitions) {
  _pool = new MemoryPool();
}

MapOutputCollector::~MapOutputCollector() {

  if (NULL != _buckets) {
    for (uint32_t i = 0; i < _numPartitions; i++) {
      if (NULL != _buckets[i]) {
        delete _buckets[i];
        _buckets[i] = NULL;
      }
    }
  }

  delete[] _buckets;
  _buckets = NULL;

  if (NULL != _pool) {
    delete _pool;
    _pool = NULL;
  }

  if (NULL != _combineRunner) {
    delete _combineRunner;
    _combineRunner = NULL;
  }
}

void MapOutputCollector::init(uint32_t defaultBlockSize, uint32_t memoryCapacity,
    ComparatorPtr keyComparator, Counter * spilledRecords, ICombineRunner * combiner) {

  this->_combineRunner = combiner;

  this->_defaultBlockSize = defaultBlockSize;

  _pool->init(memoryCapacity);

  //TODO: add support for customized comparator
  this->_keyComparator = keyComparator;

  _buckets = new PartitionBucket*[_numPartitions];

  for (uint32_t partitionId = 0; partitionId < _numPartitions; partitionId++) {
    PartitionBucket * pb = new PartitionBucket(_pool, partitionId, keyComparator, _combineRunner,
        defaultBlockSize);

    _buckets[partitionId] = pb;
  }

  _spilledRecords = spilledRecords;

  _collectTimer.reset();
}

void MapOutputCollector::reset() {
  for (uint32_t i = 0; i < _numPartitions; i++) {
    if (NULL != _buckets[i]) {
      _buckets[i]->reset();
    }
  }
  _pool->reset();
}

void MapOutputCollector::configure(Config * config) {
  _config = config;
  MapOutputSpec::getSpecFromConfig(config, _spec);

  uint32_t maxBlockSize = config->getInt(NATIVE_SORT_MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE);
  uint32_t capacity = config->getInt(MAPRED_IO_SORT_MB, 300) * 1024 * 1024;

  uint32_t defaultBlockSize = getDefaultBlockSize(capacity, _numPartitions, maxBlockSize);
  LOG("Native Total MemoryBlockPool: num_partitions %u, min_block_size %uK, max_block_size %uK, capacity %uM", _numPartitions, defaultBlockSize / 1024,
      maxBlockSize / 1024, capacity / 1024 / 1024);

  ComparatorPtr comparator = getComparator(config, _spec);

  Counter * spilledRecord = NativeObjectFactory::GetCounter(TaskCounters::TASK_COUNTER_GROUP,
      TaskCounters::SPILLED_RECORDS);

  ICombineRunner * combiner = NULL;
  if (NULL != config->get(NATIVE_COMBINER)
      // config name for old api and new api
      || NULL != config->get(MAPRED_COMBINE_CLASS_OLD)
      || NULL != config->get(MAPRED_COMBINE_CLASS_NEW)) {
    combiner = new CombineRunnerWrapper(config, _spillOutput);
  }

  init(defaultBlockSize, capacity, comparator, spilledRecord, combiner);
}

KVBuffer * MapOutputCollector::allocateKVBuffer(uint32_t partitionId, uint32_t kvlength) {
  PartitionBucket * partition = getPartition(partitionId);
  if (NULL == partition) {
    THROW_EXCEPTION_EX(IOException, "Partition is NULL, partition_id: %d, num_partitions: %d", partitionId, _numPartitions);
  }

  KVBuffer * dest = partition->allocateKVBuffer(kvlength);

  if (NULL == dest) {
    string * spillpath = _spillOutput->getSpillPath();
    if (NULL == spillpath || spillpath->length() == 0) {
      THROW_EXCEPTION(IOException, "Illegal(empty) spill files path");
    } else {
      middleSpill(*spillpath, "");
      delete spillpath;
    }

    dest = dest = partition->allocateKVBuffer(kvlength);
    if (NULL == dest) {
      // io.sort.mb too small, cann't proceed
      // should not get here, cause get_buffer_to_put can throw OOM exception
      THROW_EXCEPTION(OutOfMemoryException, "key/value pair larger than io.sort.mb");
    }
  }
  return dest;
}

/**
 * collect one k/v pair
 * @return true success; false buffer full, need spill
 */
bool MapOutputCollector::collect(const void * key, uint32_t keylen, const void * value,
    uint32_t vallen, uint32_t partitionId) {
  uint32_t total_length = keylen + vallen + KVBuffer::headerLength();
  KVBuffer * buff = allocateKVBuffer(partitionId, total_length);

  if (NULL == buff) {
    return false;
  }
  buff->fill(key, keylen, value, vallen);
  return true;
}

ComparatorPtr MapOutputCollector::getComparator(Config * config, MapOutputSpec & spec) {
  string nativeComparator = NATIVE_MAPOUT_KEY_COMPARATOR;
  const char * key_class = config->get(MAPRED_MAPOUTPUT_KEY_CLASS);
  if (NULL == key_class) {
    key_class = config->get(MAPRED_OUTPUT_KEY_CLASS);
  }
  nativeComparator.append(".").append(key_class);
  const char * comparatorName = config->get(nativeComparator);
  return NativeTask::get_comparator(spec.keyType, comparatorName);
}

PartitionBucket * MapOutputCollector::getPartition(uint32_t partition) {
  if (partition >= _numPartitions) {
    return NULL;
  }
  return _buckets[partition];
}

/**
 * Spill buffer to file
 * @return Array of spill segments information
 */
void MapOutputCollector::sortPartitions(SortOrder orderType, SortAlgorithm sortType,
    IFileWriter * writer, SortMetrics & metric) {

  uint32_t start_partition = 0;
  uint32_t num_partition = _numPartitions;
  if (orderType == GROUPBY) {
    THROW_EXCEPTION(UnsupportException, "GROUPBY not supported");
  }

  uint64_t sortingTime = 0;
  Timer timer;
  uint64_t recordNum = 0;

  for (uint32_t i = 0; i < num_partition; i++) {
    if (NULL != writer) {
      writer->startPartition();
    }
    PartitionBucket * pb = _buckets[start_partition + i];
    if (pb != NULL) {
      recordNum += pb->getKVCount();
      if (orderType == FULLORDER) {
        timer.reset();
        pb->sort(sortType);
        sortingTime += timer.now() - timer.last();
      }
      if (NULL != writer) {
        pb->spill(writer);
      }
    }
    if (NULL != writer) {
      writer->endPartition();
    }
  }
  metric.sortTime = sortingTime;
  metric.recordCount = recordNum;
}

void MapOutputCollector::middleSpill(const std::string & spillOutput,
    const std::string & indexFilePath) {

  uint64_t collecttime = _collectTimer.now() - _collectTimer.last();
  const uint64_t M = 1000000; //million

  if (spillOutput.empty()) {
    THROW_EXCEPTION(IOException, "MapOutputCollector: Spill file path empty");
  } else {
    OutputStream * fout = FileSystem::getLocal().create(spillOutput, true);

    IFileWriter * writer = new IFileWriter(fout, _spec.checksumType, _spec.keyType, _spec.valueType,
        _spec.codec, _spilledRecords);

    Timer timer;
    SortMetrics metrics;
    sortPartitions(_spec.sortOrder, _spec.sortAlgorithm, writer, metrics);

    SingleSpillInfo * info = writer->getSpillInfo();
    info->path = spillOutput;
    uint64_t spillTime = timer.now() - timer.last() - metrics.sortTime;

    LOG(
        "[MapOutputCollector::mid_spill] Sort and spill: {spilled file path: %s, id: %d, collect: %llu ms, sort: %llu ms, spill: %llu ms, records: %llu, uncompressed total bytes: %llu, compressed total bytes: %llu}",
        info->path.c_str(), _spillInfos.getSpillCount(), collecttime / M, metrics.sortTime  / M, spillTime  / M,
        metrics.recordCount, info->getEndPosition(), info->getRealEndPosition());

    if (indexFilePath.length() > 0) {
      info->writeSpillInfo(indexFilePath);
      delete info;
    } else {
      _spillInfos.add(info);
    }

    delete writer;
    delete fout;

    reset();
    _collectTimer.reset();
  }
}

/**
 * final merge and/or spill, use previous spilled
 * file & in-memory data
 */
void MapOutputCollector::finalSpill(const std::string & filepath,
    const std::string & idx_file_path) {

  const uint64_t M = 1000000; //million
  LOG("[MapOutputCollector::final_merge_and_spill] Spilling file path: %s", filepath.c_str());

  if (_spillInfos.getSpillCount() == 0) {
    middleSpill(filepath, idx_file_path);
    return;
  }

  IFileWriter * writer = IFileWriter::create(filepath, _spec, _spilledRecords);
  Merger * merger = new Merger(writer, _config, _keyComparator, _combineRunner);

  for (size_t i = 0; i < _spillInfos.getSpillCount(); i++) {
    SingleSpillInfo * spill = _spillInfos.getSingleSpillInfo(i);
    MergeEntryPtr pme = IFileMergeEntry::create(spill);
    merger->addMergeEntry(pme);
  }

  SortMetrics metrics;
  sortPartitions(_spec.sortOrder, _spec.sortAlgorithm, NULL, metrics);
  LOG("[MapOutputCollector::mid_spill] Sort final in memory kvs: {sort: %llu ms, records: %llu}",
      metrics.sortTime / M, metrics.recordCount);

  merger->addMergeEntry(new MemoryMergeEntry(_buckets, _numPartitions));

  Timer timer;
  merger->merge();
  LOG(
      "[MapOutputCollector::final_merge_and_spill]  Merge and Spill:{spilled file id: %d, merge and spill time: %llu ms}",
      _spillInfos.getSpillCount(), (timer.now() - timer.last()) / M);

  delete merger;

  // write index
  SingleSpillInfo * spill_range = writer->getSpillInfo();
  spill_range->writeSpillInfo(idx_file_path);
  delete spill_range;
  _spillInfos.deleteAllSpillFiles();
  delete writer;
  reset();
}

void MapOutputCollector::close() {
  string * outputpath = _spillOutput->getOutputPath();
  string * indexpath = _spillOutput->getOutputIndexPath();

  if ((outputpath->length() == 0) || (indexpath->length() == 0)) {
    THROW_EXCEPTION(IOException, "Illegal(empty) map output file/index path");
  }

  finalSpill(*outputpath, *indexpath);

  delete outputpath;
  delete indexpath;
}
} // namespace NativeTask

