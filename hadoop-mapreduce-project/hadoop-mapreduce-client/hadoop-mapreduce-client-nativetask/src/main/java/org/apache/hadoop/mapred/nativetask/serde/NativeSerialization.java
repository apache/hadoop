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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Writable;

@InterfaceAudience.Private
public class NativeSerialization {

  private final ConcurrentHashMap<String, Class<?>> map =
    new ConcurrentHashMap<String, Class<?>>();

  public boolean accept(Class<?> c) {
    return Writable.class.isAssignableFrom(c);
  }

  @SuppressWarnings("unchecked")
  public INativeSerializer<Writable> getSerializer(Class<?> c) throws IOException {

    if (null == c) {
      return null;
    }
    if (!Writable.class.isAssignableFrom(c)) {
      throw new IOException("Cannot serialize type " + c.getName() +
                            ", we only accept subclass of Writable");
    }
    final String name = c.getName();
    final Class<?> serializer = map.get(name);

    if (null != serializer) {
      try {
        return (INativeSerializer<Writable>) serializer.newInstance();
      } catch (final Exception e) {
        throw new IOException(e);
      }
    }
    return new DefaultSerializer();
  }

  public void register(String klass, Class<?> serializer) throws IOException {
    if (null == klass || null == serializer) {
      throw new IOException("invalid arguments, klass or serializer is null");
    }

    if (!INativeSerializer.class.isAssignableFrom(serializer)) {
      throw new IOException("Serializer is not assigable from INativeSerializer");
    }

    final Class<?> storedSerializer = map.get(klass);
    if (null == storedSerializer) {
      map.put(klass, serializer);
      return;
    } else {
      if (!storedSerializer.getName().equals(serializer.getName())) {
        throw new IOException("Error! Serializer already registered, existing: " +
                              storedSerializer.getName() + ", new: " +
                              serializer.getName());
      }
    }
  }

  public void reset() {
    map.clear();
  }

  private static NativeSerialization instance = new NativeSerialization();

  public static NativeSerialization getInstance() {
    return instance;
  }
}