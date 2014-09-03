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
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/Merge.h"
#include "lib/FileSystem.h"

namespace NativeTask {

IFileMergeEntry * IFileMergeEntry::create(SingleSpillInfo * spill) {
  InputStream * fileOut = FileSystem::getLocal().open(spill->path);
  IFileReader * reader = new IFileReader(fileOut, spill, true);
  return new IFileMergeEntry(reader);
}

Merger::Merger(IFileWriter * writer, Config * config, ComparatorPtr comparator,
    ICombineRunner * combineRunner)
    : _writer(writer), _config(config), _combineRunner(combineRunner), _first(true),
        _comparator(comparator) {
}

Merger::~Merger() {
  _heap.clear();
  for (size_t i = 0; i < _entries.size(); i++) {
    delete _entries[i];
  }
  _entries.clear();
}

void Merger::addMergeEntry(MergeEntryPtr pme) {
  _entries.push_back(pme);
}

/**
 * 0 if success, have next partition
 * 1 if failed, no more
 */
bool Merger::startPartition() {
  bool firstPartitionState = false;
  for (size_t i = 0; i < _entries.size(); i++) {
    bool partitionState = _entries[i]->nextPartition();
    if (i == 0) {
      firstPartitionState = partitionState;
    }
    if (firstPartitionState != partitionState) {
      THROW_EXCEPTION(IOException, "MergeEntry partition number not equal");
    }
  }
  if (firstPartitionState) { // do have new partition
    _writer->startPartition();
  }
  return firstPartitionState;
}

/**
 * finish one partition
 */
void Merger::endPartition() {
  _writer->endPartition();
}

void Merger::initHeap() {
  _heap.clear();
  for (size_t i = 0; i < _entries.size(); i++) {
    MergeEntryPtr pme = _entries[i];
    if (pme->next()) {
      _heap.push_back(pme);
    }
  }
  makeHeap(&(_heap[0]), &(_heap[0]) + _heap.size(), _comparator);
}

bool Merger::next() {
  size_t cur_heap_size = _heap.size();
  if (cur_heap_size > 0) {
    if (!_first) {
      if (_heap[0]->next()) { // have more, adjust heap
        if (cur_heap_size == 1) {
          return true;
        } else if (cur_heap_size == 2) {
          MergeEntryPtr * base = &(_heap[0]);

          if (_comparator(base[1], base[0])) {
            std::swap(base[0], base[1]);
          }
        } else {
          MergeEntryPtr * base = &(_heap[0]);
          heapify(base, 1, cur_heap_size, _comparator);
        }
      } else { // no more, pop heap
        MergeEntryPtr * base = &(_heap[0]);
        popHeap(base, base + cur_heap_size, _comparator);
        _heap.pop_back();
      }
    } else {
      _first = false;
    }
    return _heap.size() > 0;
  }
  return false;
}

bool Merger::next(Buffer & key, Buffer & value) {
  bool result = next();
  if (result) {
    MergeEntryPtr * base = &(_heap[0]);
    key.reset(base[0]->getKey(), base[0]->getKeyLength());
    value.reset(base[0]->getValue(), base[0]->getValueLength());
    return true;
  } else {
    return false;
  }
}

void Merger::merge() {
  uint64_t total_record = 0;
  _heap.reserve(_entries.size());
  MergeEntryPtr * base = &(_heap[0]);
  while (startPartition()) {
    initHeap();
    if (_heap.size() == 0) {
      endPartition();
      continue;
    }
    _first = true;
    if (_combineRunner == NULL) {
      while (next()) {
        _writer->write(base[0]->getKey(), base[0]->getKeyLength(), base[0]->getValue(),
            base[0]->getValueLength());
        total_record++;
      }
    } else {
      _combineRunner->combine(CombineContext(UNKNOWN), this, _writer);
    }
    endPartition();
  }
}

} // namespace NativeTask
