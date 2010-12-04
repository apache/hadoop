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

package org.apache.hadoop.io.serial.lib.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.protobuf.Message;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.TypedSerialization;

/**
 * A binding for Protocol Buffer serialization.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ProtoBufSerialization extends TypedSerialization<Message>{
  private Method builderFactory;

  public ProtoBufSerialization() {}
  public ProtoBufSerialization(Class<? extends Message> cls) {
    super(cls);
    setBuilderFactory(cls);
  }

  @Override
  public ProtoBufSerialization clone() {
    ProtoBufSerialization result = (ProtoBufSerialization) super.clone();
    result.builderFactory = builderFactory;
    return result;
  }

  @Override
  public Class<Message> getBaseType() {
    return Message.class;
  }

  private void setBuilderFactory(Class<? extends Message> cls) {
    if (cls == null) {
      builderFactory = null;
    } else {
      try {
        builderFactory = cls.getDeclaredMethod("parseFrom", 
                                               InputStream.class);
      } catch (NoSuchMethodException nsme) {
        throw new IllegalArgumentException("Can't find parseFrom in " +
                                           cls.getName());
      }
    }
  }

  @Override
  public void setSpecificType(Class<? extends Message> cls) {
    super.setSpecificType(cls);
    setBuilderFactory(cls);
  }

  @Override
  public Message deserialize(InputStream stream, Message reusableObject,
                                 Configuration conf) throws IOException {
    try {
      return (Message) builderFactory.invoke(null, stream);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("can't access parseFrom " +
                                         " on " + getSpecificType().getName());
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("can't invoke parseFrom " +
                                         " on " + getSpecificType().getName());
    }
  }

  @Override
  public RawComparator getRawComparator() {
    return new ProtoBufComparator(getSpecificType());
  }

  @Override
  public void serialize(OutputStream stream, 
                        Message object) throws IOException {
    if (specificType != object.getClass()) {
      throw new IOException("Type mismatch in serialization: expected "
          + specificType + "; received " + object.getClass());
    }
    object.writeTo(stream);
  }

  @Override
  public String getName() {
    return "protobuf";
  }
}
