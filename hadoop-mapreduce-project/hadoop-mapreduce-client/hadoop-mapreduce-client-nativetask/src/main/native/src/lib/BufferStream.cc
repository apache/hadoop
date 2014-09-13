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
#include "lib/BufferStream.h"

namespace NativeTask {

int32_t InputBuffer::read(void * buff, uint32_t length) {
  uint32_t rd = _capacity - _position < length ? _capacity - _position : length;
  if (rd > 0) {
    memcpy(buff, _buff + _position, rd);
    _position += rd;
    return rd;
  }
  return length == 0 ? 0 : -1;
}

void OutputBuffer::write(const void * buff, uint32_t length) {
  if (_position + length <= _capacity) {
    memcpy(_buff + _position, buff, length);
    _position += length;
  } else {
    THROW_EXCEPTION(IOException, "OutputBuffer too small to write");
  }
}

} // namespace NativeTask
