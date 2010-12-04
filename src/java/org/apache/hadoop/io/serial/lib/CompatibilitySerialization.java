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

package org.apache.hadoop.io.serial.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.TypedSerialization;
import org.apache.hadoop.io.serial.lib.avro.AvroComparator;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class allows user-defined old style serializers to run inside the new
 * framework. This will only be used for user serializations that haven't been
 * ported yet.
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompatibilitySerialization extends TypedSerialization<Object> 
                                        implements Configurable {
  private org.apache.hadoop.io.serializer.SerializationFactory factory;
  
  @SuppressWarnings("unchecked")
  private org.apache.hadoop.io.serializer.Serialization 
    serialization = null;

  public CompatibilitySerialization() {
    // NOTHING
  }

  @Override
  public CompatibilitySerialization clone() {
    CompatibilitySerialization result = 
      (CompatibilitySerialization) super.clone();
    result.factory = factory;
    result.serialization = serialization;
    return result;
  }

  @Override
  public Class<Object> getBaseType() {
    return Object.class;
  }

  @Override
  public boolean accept(Class<? extends Object> candidateClass) {
    return factory.getSerialization(candidateClass) != null;
  }

  @Override
  public void setSpecificType(Class<?> cls) {
    super.setSpecificType(cls);
    serialization = factory.getSerialization(cls);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object deserialize(InputStream stream, Object reusableObject,
                            Configuration conf) throws IOException {
    org.apache.hadoop.io.serializer.Deserializer deserializer =
      serialization.getDeserializer(specificType);
    deserializer.open(stream);
    Object result = deserializer.deserialize(reusableObject);
    // if the object is new, configure it
    if (result != reusableObject) {
      ReflectionUtils.setConf(result, conf);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RawComparator getRawComparator() {
    if (specificType == null) {
      throw new 
        IllegalArgumentException("Must have specific type for comparision");
    } else if (serialization instanceof 
                 org.apache.hadoop.io.serializer.WritableSerialization) {
      return WritableComparator.get((Class) specificType);
    } else if (serialization instanceof
                 org.apache.hadoop.io.serializer.avro.AvroReflectSerialization){
      Schema schema = ReflectData.get().getSchema(specificType);
      return new AvroComparator(schema);
    } else if (serialization instanceof
                org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization){
      Schema schema = SpecificData.get().getSchema(specificType);
      return new AvroComparator(schema);
    } else if (Comparable.class.isAssignableFrom(specificType)) {
      // if the type is comparable, we can deserialize
      return new DeserializationRawComparator(this, null);
    } else {
      return new MemcmpRawComparator();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(OutputStream stream, Object object) throws IOException {
    org.apache.hadoop.io.serializer.Serializer serializer = 
      serialization.getSerializer(specificType);
    serializer.open(stream);
    serializer.serialize(object);
  }

  @Override
  public String getName() {
    return "compatibility";
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
    factory = new org.apache.hadoop.io.serializer.SerializationFactory(conf);
  }
}
