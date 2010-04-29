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

package org.apache.hadoop.io.serializer.avro;

import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 * Serialization for Avro Reflect classes. For a class to be accepted by this 
 * serialization, it must either be in the package list configured via 
 * {@link AvroReflectSerialization#AVRO_REFLECT_PACKAGES} or implement 
 * {@link AvroReflectSerializable} interface.
 *
 */
@SuppressWarnings("unchecked")
public class AvroReflectSerialization extends AvroSerialization<Object>{

  /**
   * Key to configure packages that contain classes to be serialized and 
   * deserialized using this class. Multiple packages can be specified using 
   * comma-separated list.
   */
  public static final String AVRO_REFLECT_PACKAGES = "avro.reflect.pkgs";

  private Set<String> packages; 

  @Override
  public synchronized boolean accept(Class<?> c) {
    if (packages == null) {
      getPackages();
    }
    return AvroReflectSerializable.class.isAssignableFrom(c) || 
      packages.contains(c.getPackage().getName());
  }

  private void getPackages() {
    String[] pkgList  = getConf().getStrings(AVRO_REFLECT_PACKAGES);
    packages = new HashSet<String>();
    if (pkgList != null) {
      for (String pkg : pkgList) {
        packages.add(pkg.trim());
      }
    }
  }

  @Override
  public DatumReader getReader(Class<Object> clazz) {
    try {
      return new ReflectDatumReader(clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Schema getSchema(Object t) {
    return ReflectData.get().getSchema(t.getClass());
  }

  @Override
  public DatumWriter getWriter(Class<Object> clazz) {
    return new ReflectDatumWriter();
  }

}
