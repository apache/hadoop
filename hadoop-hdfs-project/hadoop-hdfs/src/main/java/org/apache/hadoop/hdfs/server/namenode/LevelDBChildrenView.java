/**
 * Licensed to the Apache Software Foundation (ASF) under oo
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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

class LevelDBChildrenView extends DBChildrenView {
  private final long parentId;
  private final org.apache.hadoop.hdfs.hdfsdb.Iterator it;

  LevelDBChildrenView(long parentId, org.apache.hadoop.hdfs.hdfsdb.Iterator it) {
    this.parentId = parentId;
    this.it = it;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public void seekTo(ByteBuffer start) {
    byte[] key = LevelDBROTransaction.inodeChildKey(parentId, start);
    it.seek(key);
  }

  @Override
  public void close() throws IOException {
    it.close();
  }

  @Override
  public Iterator<Map.Entry<ByteBuffer, Long>> iterator() {
    return new Iterator<Map.Entry<ByteBuffer, Long>>() {
      @Override
      public boolean hasNext() {
        if (!it.hasNext()) {
          return false;
        }
        byte[] key = it.peekNext().getKey();
        return key.length >= 10 && key[9] == 1;
      }

      @Override
      public Map.Entry<ByteBuffer, Long> next() {
        Map.Entry<byte[], byte[]> n = it.next();
        long v = ByteBuffer.wrap(n.getValue()).order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer().get();
        return new AbstractMap.SimpleImmutableEntry<>(
            ByteBuffer.wrap(n.getKey(), 10, n.getKey().length - 10), v);
      }
    };
  }
}
