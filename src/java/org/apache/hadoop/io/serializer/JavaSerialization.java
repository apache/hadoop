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
import java.util.Map;

import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * An experimental {@link Serialization} for Java {@link Serializable} classes.
 * </p>
 * @see JavaSerializationComparator
 */
public class JavaSerialization extends SerializationBase<Serializable> {

  static class JavaSerializationDeserializer<T extends Serializable>
    extends DeserializerBase<T> {

    private ObjectInputStream ois;

    public void open(InputStream in) throws IOException {
      ois = new ObjectInputStream(in) {
        @Override protected void readStreamHeader() {
          // no header
        }
      };
    }
    
    @SuppressWarnings("unchecked")
    public T deserialize(T object) throws IOException {
      try {
        // ignore passed-in object
        return (T) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException(e.toString());
      }
    }

    public void close() throws IOException {
      ois.close();
    }

  }

  static class JavaSerializationSerializer<T extends Serializable>
      extends SerializerBase<T> {

    private ObjectOutputStream oos;
    private Map<String, String> metadata;

    public JavaSerializationSerializer(Map<String, String> metadata) {
      this.metadata = metadata;
    }

    public void open(OutputStream out) throws IOException {
      oos = new ObjectOutputStream(out) {
        @Override protected void writeStreamHeader() {
          // no header
        }
      };
    }

    public void serialize(T object) throws IOException {
      oos.reset(); // clear (class) back-references
      oos.writeObject(object);
    }

    public void close() throws IOException {
      oos.close();
    }

    @Override
    public Map<String, String> getMetadata() throws IOException {
      return metadata;
    }
  }

  public boolean accept(Map<String, String> metadata) {
    String intendedSerializer = metadata.get(SERIALIZATION_KEY);
    if (intendedSerializer != null &&
        !getClass().getName().equals(intendedSerializer)) {
      return false;
    }

    Class<?> c = getClassFromMetadata(metadata);
    return Serializable.class.isAssignableFrom(c);
  }

  public DeserializerBase<Serializable> getDeserializer(
      Map<String, String> metadata) {
    return new JavaSerializationDeserializer<Serializable>();
  }

  public SerializerBase<Serializable> getSerializer(
      Map<String, String> metadata) {
    return new JavaSerializationSerializer<Serializable>(metadata);
  }

  @SuppressWarnings("unchecked")
  @Override
  public RawComparator<Serializable> getRawComparator(
      Map<String, String> metadata) {
    Class<?> klazz = getClassFromMetadata(metadata);
    if (null == klazz) {
      throw new IllegalArgumentException(
          "Cannot get comparator without " + SerializationBase.CLASS_KEY
          + " set in metadata");
    }

    if (Serializable.class.isAssignableFrom(klazz)) {
      try {
        return (RawComparator<Serializable>) new JavaSerializationComparator();
      } catch (IOException ioe) {
        throw new IllegalArgumentException(
            "Could not instantiate JavaSerializationComparator for type "
            + klazz.getName(), ioe);
      }
    } else {
      throw new IllegalArgumentException("Class " + klazz.getName()
          + " is incompatible with JavaSerialization");
    }
  }
}
