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

#ifndef MERGE_H_
#define MERGE_H_

#include "NativeTask.h"
#include "lib/Buffers.h"
#include "lib/MapOutputCollector.h"
#include "lib/IFile.h"
#include "lib/MinHeap.h"

namespace NativeTask {

/**
 * merger
 */
class MergeEntry {

protected:
  // these 3 fields should be filled after next() is called
  const char * _key;
  const char * _value;
  uint32_t _keyLength;
  uint32_t _valueLength;

public:
  MergeEntry()
      : _key(NULL), _value(NULL), _keyLength(0), _valueLength(0) {
  }

  const char * getKey() const {
    return _key;
  }

  const char * getValue() const {
    return _value;
  }

  uint32_t getKeyLength() const {
    return _keyLength;
  }

  uint32_t getValueLength() const {
    return _valueLength;
  }

  virtual ~MergeEntry() {
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual bool nextPartition() = 0;

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual bool next() = 0;
};

/**
 * Merger
 */
typedef MergeEntry * MergeEntryPtr;

class MergeEntryComparator {
private:
  ComparatorPtr _keyComparator;

public:
  MergeEntryComparator(ComparatorPtr comparator)
      : _keyComparator(comparator) {
  }

public:
  bool operator()(const MergeEntryPtr lhs, const MergeEntryPtr rhs) {
    return (*_keyComparator)(lhs->getKey(), lhs->getKeyLength(), rhs->getKey(), rhs->getKeyLength())
        < 0;
  }
};

/**
 * Merge entry for in-memory partition bucket
 */
class MemoryMergeEntry : public MergeEntry {
protected:

  PartitionBucket ** _partitions;
  uint32_t _number;
  int64_t _index;

  KVIterator * _iterator;
  Buffer keyBuffer;
  Buffer valueBuffer;

public:
  MemoryMergeEntry(PartitionBucket ** partitions, uint32_t numberOfPartitions)
      : _partitions(partitions), _number(numberOfPartitions), _index(-1), _iterator(NULL) {
  }

  virtual ~MemoryMergeEntry() {
    if (NULL != _iterator) {
      delete _iterator;
      _iterator = NULL;
    }
  }

  virtual bool nextPartition() {
    ++_index;
    if (_index < _number) {
      PartitionBucket * current = _partitions[_index];
      if (NULL != _iterator) {
        delete _iterator;
        _iterator = NULL;
      }
      if (NULL != current) {
        _iterator = current->getIterator();
      }
      return true;
    }
    return false;
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual bool next() {
    if (NULL == _iterator) {
      return false;
    }
    bool hasNext = _iterator->next(keyBuffer, valueBuffer);

    if (hasNext) {
      _keyLength = keyBuffer.length();
      _key = keyBuffer.data();
      _valueLength = valueBuffer.length();
      _value = valueBuffer.data();
      assert(_value != NULL);
      return true;
    }
    // detect error early
    _keyLength = 0xffffffff;
    _valueLength = 0xffffffff;
    _key = NULL;
    _value = NULL;
    return false;
  }
};

/**
 * Merge entry for intermediate file
 */
class IFileMergeEntry : public MergeEntry {
protected:
  IFileReader * _reader;
  bool new_partition;
public:
  /**
   * @param reader: managed by InterFileMergeEntry
   */

  static IFileMergeEntry * create(SingleSpillInfo * spill);

  IFileMergeEntry(IFileReader * reader)
      : _reader(reader) {
    new_partition = false;
  }

  virtual ~IFileMergeEntry() {
    delete _reader;
    _reader = NULL;
  }

  /**
   * move to next partition
   * 0 on success
   * 1 on no more
   */
  virtual bool nextPartition() {
    return _reader->nextPartition();
  }

  /**
   * move to next key/value
   * 0 on success
   * 1 on no more
   */
  virtual bool next() {
    _key = _reader->nextKey(_keyLength);
    if (unlikely(NULL == _key)) {
      // detect error early
      _keyLength = 0xffffffffU;
      _valueLength = 0xffffffffU;
      return false;
    }
    _value = _reader->value(_valueLength);
    return true;
  }
};

class Merger : public KVIterator {

private:
  vector<MergeEntryPtr> _entries;
  vector<MergeEntryPtr> _heap;
  IFileWriter * _writer;
  Config * _config;
  ICombineRunner * _combineRunner;
  bool _first;
  MergeEntryComparator _comparator;

public:
  Merger(IFileWriter * writer, Config * config, ComparatorPtr comparator,
      ICombineRunner * combineRunner = NULL);

  ~Merger();

  void addMergeEntry(MergeEntryPtr pme);

  void merge();

  virtual bool next(Buffer & key, Buffer & value);
protected:
  bool startPartition();
  void endPartition();
  void initHeap();
  bool next();
};

} // namespace NativeTask

#endif /* MERGE_H_ */
