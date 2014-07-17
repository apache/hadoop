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
#include "Combiner.h"
#include "StringUtil.h"

namespace NativeTask {

NativeCombineRunner::NativeCombineRunner(Config * config, ObjectCreatorFunc combinerCreator)
    : _config(config), _combinerCreator(combinerCreator), _keyGroupCount(0) {
  if (NULL == _combinerCreator) {
    THROW_EXCEPTION_EX(UnsupportException, "Create combiner failed");
  }
}

KeyGroupIterator * NativeCombineRunner::createKeyGroupIterator(KVIterator * iter) {
  return new KeyGroupIteratorImpl(iter);
}

void NativeCombineRunner::combine(CombineContext context, KVIterator * iterator,
    IFileWriter * writer) {
  Configurable * combiner = (Configurable *)(_combinerCreator());
  if (NULL != combiner) {
    combiner->configure(_config);
  }

  NativeObjectType type = combiner->type();
  switch (type) {
  case MapperType: {
    Mapper * mapper = (Mapper*)combiner;
    mapper->setCollector(writer);

    Buffer key;
    Buffer value;
    while (iterator->next(key, value)) {
      mapper->map(key.data(), key.length(), value.data(), value.length());
    }
    mapper->close();
    delete mapper;
  }
    break;
  case ReducerType: {
    Reducer * reducer = (Reducer*)combiner;
    reducer->setCollector(writer);
    KeyGroupIterator * kg = createKeyGroupIterator(iterator);
    while (kg->nextKey()) {
      _keyGroupCount++;
      reducer->reduce(*kg);
    }
    reducer->close();
    delete reducer;
  }
    break;
  default:
    THROW_EXCEPTION(UnsupportException, "Combiner type not support");
  }
}

} /* namespace NativeTask */
