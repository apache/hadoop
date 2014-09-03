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
#include "lib/Iterator.h"
#include "lib/commons.h"

namespace NativeTask {

KeyGroupIteratorImpl::KeyGroupIteratorImpl(KVIterator * iterator)
    : _keyGroupIterState(NEW_KEY), _iterator(iterator), _first(true) {
}

bool KeyGroupIteratorImpl::nextKey() {
  if (_keyGroupIterState == NO_MORE) {
    return false;
  }

  uint32_t temp;
  while (_keyGroupIterState == SAME_KEY || _keyGroupIterState == NEW_KEY_VALUE) {
    nextValue(temp);
  }
  if (_keyGroupIterState == NEW_KEY) {
    if (_first == true) {
      _first = false;
      if (!next()) {
        _keyGroupIterState = NO_MORE;
        return false;
      }
    }
    _keyGroupIterState = NEW_KEY_VALUE;
    _currentGroupKey.assign(_key.data(), _key.length());
    return true;
  }
  return false;
}

const char * KeyGroupIteratorImpl::getKey(uint32_t & len) {
  len = (uint32_t)_key.length();
  return _key.data();
}

const char * KeyGroupIteratorImpl::nextValue(uint32_t & len) {
  switch (_keyGroupIterState) {
  case NEW_KEY: {
    return NULL;
  }
  case SAME_KEY: {
    if (next()) {
      if (_key.length() == _currentGroupKey.length()) {
        if (fmemeq(_key.data(), _currentGroupKey.c_str(), _key.length())) {
          len = _value.length();
          return _value.data();
        }
      }
      _keyGroupIterState = NEW_KEY;
      return NULL;
    }
    _keyGroupIterState = NO_MORE;
    return NULL;
  }
  case NEW_KEY_VALUE: {
    _keyGroupIterState = SAME_KEY;
    len = _value.length();
    return _value.data();
  }
  case NO_MORE:
    return NULL;
  }
  return NULL;
}

bool KeyGroupIteratorImpl::next() {
  bool result = _iterator->next(_key, _value);
  return result;
}

} // namespace NativeTask
