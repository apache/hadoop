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

#include "lib/commons.h"
#include "util/StringUtil.h"
#include "lib/TaskCounters.h"
#include "MCollectorOutputHandler.h"
#include "lib/NativeObjectFactory.h"
#include "lib/MapOutputCollector.h"
#include "CombineHandler.h"

using std::string;
using std::vector;

namespace NativeTask {

MCollectorOutputHandler::MCollectorOutputHandler()
    : _collector(NULL), _dest(NULL), _endium(LARGE_ENDIUM) {
}

MCollectorOutputHandler::~MCollectorOutputHandler() {
  _dest = NULL;
  delete _collector;
  _collector = NULL;
}

void MCollectorOutputHandler::configure(Config * config) {
  if (NULL == config) {
    return;
  }

  uint32_t partition = config->getInt(MAPRED_NUM_REDUCES, 1);

  _collector = new MapOutputCollector(partition, this);
  _collector->configure(config);
}

void MCollectorOutputHandler::finish() {
  _collector->close();
  BatchHandler::finish();
}

void MCollectorOutputHandler::handleInput(ByteBuffer & in) {
  char * buff = in.current();
  uint32_t length = in.remain();

  const char * end = buff + length;
  char * pos = buff;
  if (_kvContainer.remain() > 0) {
    uint32_t filledLength = _kvContainer.fill(pos, length);
    pos += filledLength;
  }

  while (end - pos > 0) {
    KVBufferWithParititionId * kvBuffer = (KVBufferWithParititionId *)pos;

    if (unlikely(end - pos < KVBuffer::headerLength())) {
      THROW_EXCEPTION(IOException, "k/v meta information incomplete");
    }

    if (_endium == LARGE_ENDIUM) {
      kvBuffer->partitionId = bswap(kvBuffer->partitionId);
      kvBuffer->buffer.keyLength = bswap(kvBuffer->buffer.keyLength);
      kvBuffer->buffer.valueLength = bswap(kvBuffer->buffer.valueLength);
    }

    uint32_t kvLength = kvBuffer->buffer.length();

    KVBuffer * dest = allocateKVBuffer(kvBuffer->partitionId, kvLength);
    _kvContainer.wrap((char *)dest, kvLength);

    pos += 4; //skip the partition length
    uint32_t filledLength = _kvContainer.fill(pos, end - pos);
    pos += filledLength;
  }
}

KVBuffer * MCollectorOutputHandler::allocateKVBuffer(uint32_t partitionId, uint32_t kvlength) {
  KVBuffer * dest = _collector->allocateKVBuffer(partitionId, kvlength);
  return dest;
}

} // namespace NativeTask
