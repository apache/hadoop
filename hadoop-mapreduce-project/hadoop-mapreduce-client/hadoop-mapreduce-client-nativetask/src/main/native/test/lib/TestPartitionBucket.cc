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
#include "test_commons.h"
#include "lib/PartitionBucket.h"
#include "lib/PartitionBucketIterator.h"
#include "lib/MemoryBlock.h"
#include "lib/IFile.h"

namespace NativeTask {

class MockIFileWriter : public IFileWriter {
 private:
  char * _buff;
  uint32_t _position;
  uint32_t _capacity;

 public:
  MockIFileWriter(char * buff, uint32_t capacity)
      : IFileWriter(NULL, CHECKSUM_NONE, TextType, TextType, "", NULL), _buff(buff), _position(0),
          _capacity(capacity) {
  }

  virtual void write(const char * key, uint32_t keyLen, const char * value, uint32_t valueLen) {
    KVBuffer * kv = (KVBuffer *)(_buff + _position);
    kv->keyLength = keyLen;
    kv->valueLength = valueLen;
    *((uint32_t *)kv->getKey()) = *((uint32_t *)key);
    *((uint32_t *)kv->getValue()) = *((uint32_t *)value);
    _position += kv->length();
  }

  char * buff() {
    return _buff;
  }
};

TEST(PartitionBucket, general) {
  MemoryPool * pool = new MemoryPool();
  const uint32_t POOL_SIZE = 1024 * 1024; // 1MB
  const uint32_t BLOCK_SIZE = 1024; // 1KB
  const uint32_t PARTITION_ID = 3;
  pool->init(POOL_SIZE);
  ComparatorPtr comparator = NativeTask::get_comparator(BytesType, NULL);
  PartitionBucket * bucket = new PartitionBucket(pool, PARTITION_ID, comparator, NULL, BLOCK_SIZE);
  ASSERT_EQ(0, bucket->getKVCount());
  KVIterator * NULLPOINTER = 0;
  ASSERT_EQ(NULLPOINTER, bucket->getIterator());
  ASSERT_EQ(PARTITION_ID, bucket->getPartitionId());
  bucket->sort(DUALPIVOTSORT);
  bucket->spill(NULL);

  delete bucket;
  delete pool;
}

TEST(PartitionBucket, multipleMemoryBlock) {
  MemoryPool * pool = new MemoryPool();
  const uint32_t POOL_SIZE = 1024 * 1024; // 1MB
  const uint32_t BLOCK_SIZE = 1024; // 1KB
  const uint32_t PARTITION_ID = 3;
  pool->init(POOL_SIZE);
  ComparatorPtr comparator = NativeTask::get_comparator(BytesType, NULL);
  PartitionBucket * bucket = new PartitionBucket(pool, PARTITION_ID, comparator, NULL, BLOCK_SIZE);

  const uint32_t KV_SIZE = 700;
  const uint32_t SMALL_KV_SIZE = 100;
  // To suppress valgrind error
  // the allocated buffer needs to be initialized before
  // create iterator on the PartitionBucker, because
  // those memory will be compared when create minheap
  KVBuffer * kv1 = bucket->allocateKVBuffer(KV_SIZE);
  memset(kv1, 0, KV_SIZE);
  KVBuffer * kv2 = bucket->allocateKVBuffer(SMALL_KV_SIZE);
  memset(kv2, 0, SMALL_KV_SIZE);
  KVBuffer * kv3 = bucket->allocateKVBuffer(KV_SIZE);
  memset(kv3, 0, KV_SIZE);

  ASSERT_EQ(3, bucket->getKVCount());
  KVIterator * NULLPOINTER = 0;
  KVIterator * iter = bucket->getIterator();
  ASSERT_NE(NULLPOINTER, iter);
  delete iter;
  ASSERT_EQ(2, bucket->getMemoryBlockCount());

  bucket->reset();
  iter = bucket->getIterator();
  ASSERT_EQ(NULLPOINTER, iter);
  delete iter;
  ASSERT_EQ(0, bucket->getMemoryBlockCount());

  delete bucket;
  delete pool;
}

TEST(PartitionBucket, sort) {
  MemoryPool * pool = new MemoryPool();
  const uint32_t POOL_SIZE = 1024 * 1024; // 1MB
  const uint32_t BLOCK_SIZE = 1024; // 1KB
  const uint32_t PARTITION_ID = 3;
  pool->init(POOL_SIZE);
  ComparatorPtr comparator = NativeTask::get_comparator(BytesType, NULL);
  PartitionBucket * bucket = new PartitionBucket(pool, PARTITION_ID, comparator, NULL, BLOCK_SIZE);

  const uint32_t KV_SIZE = 700;
  const uint32_t SMALL_KV_SIZE = 100;
  KVBuffer * kv1 = bucket->allocateKVBuffer(KV_SIZE);
  KVBuffer * kv2 = bucket->allocateKVBuffer(SMALL_KV_SIZE);
  KVBuffer * kv3 = bucket->allocateKVBuffer(KV_SIZE);

  const uint32_t SMALL = 10;
  const uint32_t MEDIUM = 100;
  const uint32_t BIG = 1000;

  kv1->keyLength = 4;
  *((uint32_t *)kv1->getKey()) = bswap(BIG);
  kv1->valueLength = KV_SIZE - kv1->headerLength() - kv1->keyLength;

  kv2->keyLength = 4;
  *((uint32_t *)kv2->getKey()) = bswap(SMALL);
  kv2->valueLength = KV_SIZE - kv2->headerLength() - kv2->keyLength;

  kv3->keyLength = 4;
  *((uint32_t *)kv3->getKey()) = bswap(MEDIUM);
  kv3->valueLength = KV_SIZE - kv3->headerLength() - kv3->keyLength;

  bucket->sort(DUALPIVOTSORT);

  KVIterator * iter = bucket->getIterator();

  Buffer key;
  Buffer value;
  iter->next(key, value);

  ASSERT_EQ(SMALL, bswap(*(uint32_t * )key.data()));

  iter->next(key, value);
  ASSERT_EQ(MEDIUM, bswap(*(uint32_t * )key.data()));

  iter->next(key, value);
  ASSERT_EQ(BIG, bswap(*(uint32_t * )key.data()));

  delete iter;
  delete bucket;
  delete pool;
}

TEST(PartitionBucket, spill) {
  MemoryPool * pool = new MemoryPool();
  const uint32_t POOL_SIZE = 1024 * 1024; // 1MB
  const uint32_t BLOCK_SIZE = 1024; // 1KB
  const uint32_t PARTITION_ID = 3;
  pool->init(POOL_SIZE);
  ComparatorPtr comparator = NativeTask::get_comparator(BytesType, NULL);
  PartitionBucket * bucket = new PartitionBucket(pool, PARTITION_ID, comparator, NULL, BLOCK_SIZE);

  const uint32_t KV_SIZE = 700;
  const uint32_t SMALL_KV_SIZE = 100;
  KVBuffer * kv1 = bucket->allocateKVBuffer(KV_SIZE);
  KVBuffer * kv2 = bucket->allocateKVBuffer(SMALL_KV_SIZE);
  KVBuffer * kv3 = bucket->allocateKVBuffer(KV_SIZE);

  const uint32_t SMALL = 10;
  const uint32_t MEDIUM = 100;
  const uint32_t BIG = 1000;

  kv1->keyLength = 4;
  *((uint32_t *)kv1->getKey()) = bswap(BIG);
  kv1->valueLength = KV_SIZE - KVBuffer::headerLength() - kv1->keyLength;

  kv2->keyLength = 4;
  *((uint32_t *)kv2->getKey()) = bswap(SMALL);
  kv2->valueLength = KV_SIZE - KVBuffer::headerLength() - kv2->keyLength;

  kv3->keyLength = 4;
  *((uint32_t *)kv3->getKey()) = bswap(MEDIUM);
  kv3->valueLength = KV_SIZE - KVBuffer::headerLength() - kv3->keyLength;

  bucket->sort(DUALPIVOTSORT);

  uint32_t BUFF_SIZE = 1024 * 1024;
  char * buff = new char[BUFF_SIZE];
  MockIFileWriter writer(buff, BUFF_SIZE);
  bucket->spill(&writer);

  // check the result
  KVBuffer * first = (KVBuffer *)writer.buff();
  ASSERT_EQ(4, first->keyLength);
  ASSERT_EQ(KV_SIZE - KVBuffer::headerLength() - 4, first->valueLength);
  ASSERT_EQ(bswap(SMALL), (*(uint32_t * )(first->getKey())));

  KVBuffer * second = first->next();
  ASSERT_EQ(4, second->keyLength);
  ASSERT_EQ(KV_SIZE - KVBuffer::headerLength() - 4, second->valueLength);
  ASSERT_EQ(bswap(MEDIUM), (*(uint32_t * )(second->getKey())));

  KVBuffer * third = second->next();
  ASSERT_EQ(4, third->keyLength);
  ASSERT_EQ(KV_SIZE - KVBuffer::headerLength() - 4, third->valueLength);
  ASSERT_EQ(bswap(BIG), (*(uint32_t * )(third->getKey())));

  delete [] buff;
  delete bucket;
  delete pool;
}
} // namespace NativeTask
