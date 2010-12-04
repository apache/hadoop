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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.TypedSerialization;

/**
 * A serialization binding for Java serialization. It has the advantage of
 * handling all serializable Java types, but is not space or time efficient. In
 * particular, the type information is repeated in each record.
 * It is not enabled by default.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JavaSerialization extends TypedSerialization<Serializable> {
  
  @Override
  public Serializable deserialize(InputStream stream,
                                  Serializable reusableObject,
                                  Configuration conf) throws IOException {
    ObjectInputStream ois = new ObjectInputStream(stream) {
      @Override protected void readStreamHeader() {
        // no header
      }
    };
    try {
      // ignore passed-in object
      Serializable result = (Serializable) ois.readObject();
      return result;
    } catch (ClassNotFoundException e) {
      throw new IOException(e.toString());
    }
  }

  @Override
  public void deserializeSelf(InputStream in, Configuration conf) {
    // nothing
  }

  @SuppressWarnings("unchecked")
  @Override
  public RawComparator getRawComparator() {
    return new DeserializationRawComparator(this, null);
  }

  @Override
  public void serialize(OutputStream stream, Serializable object
                        ) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(stream) {
      @Override protected void writeStreamHeader() {
        // no header
      }
    };
    oos.reset(); // clear (class) back-references
    oos.writeObject(object);
    oos.flush();
  }

  @Override
  public void serializeSelf(OutputStream out) throws IOException {
    // nothing
  }

  @Override
  public Class<Serializable> getBaseType() {
    return Serializable.class;
  }
  
  @Override
  public String getName() {
    return "java";
  }
  
  @Override
  public void fromString(String metadata) {
    // NOTHING
  }
  
  @Override
  public String toString() {
    return "<Java Serialization>";
  }
}
