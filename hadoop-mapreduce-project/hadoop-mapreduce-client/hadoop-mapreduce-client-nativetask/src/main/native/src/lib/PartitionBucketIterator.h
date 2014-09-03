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

#ifndef PARTITION_BUCKET_ITERATOR_H_
#define PARTITION_BUCKET_ITERATOR_H_

#include "NativeTask.h"
#include "lib/MemoryPool.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"
#include "lib/PartitionBucket.h"

namespace NativeTask {

class PartitionBucketIterator : public KVIterator {
protected:
  PartitionBucket * _pb;
  std::vector<MemBlockIteratorPtr> _heap;
  MemBlockComparator _comparator;
  bool _first;

public:
  PartitionBucketIterator(PartitionBucket * pb, ComparatorPtr comparator);
  virtual ~PartitionBucketIterator();
  virtual bool next(Buffer & key, Buffer & value);

private:
  bool next();
};

}
;
//namespace NativeTask

#endif /* PARTITION_BUCKET_ITERATOR_H_ */
