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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;

enum FlatINodeFeatureId {
  INODE_FILE(1, FlatINodeFileFeature.class, new Wrapper<FlatINodeFileFeature>() {
    @Override
    public FlatINodeFileFeature wrap(ByteBuffer data) {
      return FlatINodeFileFeature.wrap(data);
    }
  });

  private interface Wrapper<T> {
    T wrap(ByteBuffer data);
  }

  private final int id;
  private final Class<? extends FlatINode.Feature> clazz;
  private final Wrapper<? extends FlatINode.Feature> constructor;

  private static final Map<Class<? extends FlatINode.Feature>,
      FlatINodeFeatureId> map;
  private static final FlatINodeFeatureId[] VALUES = values();

  static {
    ImmutableMap.Builder<Class<? extends FlatINode.Feature>, FlatINodeFeatureId>
        m = ImmutableMap.builder();
    for (FlatINodeFeatureId f : values()) {
      m.put(f.clazz, f);
    }
    map = m.build();
  }

  FlatINodeFeatureId(
      int id, Class<? extends FlatINode.Feature> clazz,
      Wrapper<? extends FlatINode.Feature> wrap) {
    this.id = id;
    this.clazz = clazz;
    this.constructor = wrap;
  }

  static FlatINodeFeatureId valueOf(Class<? extends FlatINode.Feature> clazz) {
    return map.get(clazz);
  }

  static FlatINodeFeatureId valueOf(int id) {
    return VALUES[id - 1];
  }

  int id() { return id; }
  FlatINode.Feature wrap(ByteBuffer data) { return constructor.wrap(data); }
}
