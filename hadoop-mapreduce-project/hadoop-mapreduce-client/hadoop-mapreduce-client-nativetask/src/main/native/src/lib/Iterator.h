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

#ifndef ITERATOR_H_
#define ITERATOR_H_

#include "NativeTask.h"

namespace NativeTask {

class KeyGroupIteratorImpl : public KeyGroupIterator {
protected:
  // for KeyGroupIterator
  KeyGroupIterState _keyGroupIterState;
  KVIterator * _iterator;
  string _currentGroupKey;
  Buffer _key;
  Buffer _value;
  bool _first;

public:
  KeyGroupIteratorImpl(KVIterator * iterator);
  bool nextKey();
  const char * getKey(uint32_t & len);
  const char * nextValue(uint32_t & len);

protected:
  bool next();
};

} //namespace NativeTask
#endif
