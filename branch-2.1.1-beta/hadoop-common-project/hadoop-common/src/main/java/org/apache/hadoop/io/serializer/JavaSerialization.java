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

package org.apache.hadoop.io.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * An experimental {@link Serialization} for Java {@link Serializable} classes.
 * </p>
 * @see JavaSerializationComparator
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JavaSerialization implements Serialization<Serializable> {

  static class JavaSerializationDeserializer<T extends Serializable>
    implements Deserializer<T> {

    private ObjectInputStream ois;

    @Override
    public void open(InputStream in) throws IOException {
      ois = new ObjectInputStream(in) {
        @Override protected void readStreamHeader() {
          // no header
        }
      };
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(T object) throws IOException {
      try {
        // ignore passed-in object
        return (T) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException(e.toString());
      }
    }

    @Override
    public void close() throws IOException {
      ois.close();
    }

  }

  static class JavaSerializationSerializer
    implements Serializer<Serializable> {

    private ObjectOutputStream oos;

    @Override
    public void open(OutputStream out) throws IOException {
      oos = new ObjectOutputStream(out) {
        @Override protected void writeStreamHeader() {
          // no header
        }
      };
    }

    @Override
    public void serialize(Serializable object) throws IOException {
      oos.reset(); // clear (class) back-references
      oos.writeObject(object);
    }

    @Override
    public void close() throws IOException {
      oos.close();
    }

  }

  @Override
  @InterfaceAudience.Private
  public boolean accept(Class<?> c) {
    return Serializable.class.isAssignableFrom(c);
  }

  @Override
  @InterfaceAudience.Private
  public Deserializer<Serializable> getDeserializer(Class<Serializable> c) {
    return new JavaSerializationDeserializer<Serializable>();
  }

  @Override
  @InterfaceAudience.Private
  public Serializer<Serializable> getSerializer(Class<Serializable> c) {
    return new JavaSerializationSerializer();
  }

}
