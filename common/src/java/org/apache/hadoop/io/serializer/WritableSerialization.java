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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link SerializationBase} for {@link Writable}s that delegates to
 * {@link Writable#write(java.io.DataOutput)} and
 * {@link Writable#readFields(java.io.DataInput)}.
 */
public class WritableSerialization extends SerializationBase<Writable> {
  
  static class WritableDeserializer extends DeserializerBase<Writable> {

    private Class<?> writableClass;
    private DataInputStream dataIn;
    
    public WritableDeserializer(Configuration conf, Class<?> c) {
      setConf(conf);
      this.writableClass = c;
    }
    
    @Override
    public void open(InputStream in) {
      if (in instanceof DataInputStream) {
        dataIn = (DataInputStream) in;
      } else {
        dataIn = new DataInputStream(in);
      }
    }
    
    @Override
    public Writable deserialize(Writable w) throws IOException {
      Writable writable;
      if (w == null) {
        writable 
          = (Writable) ReflectionUtils.newInstance(writableClass, getConf());
      } else {
        writable = w;
      }
      writable.readFields(dataIn);
      return writable;
    }

    @Override
    public void close() throws IOException {
      dataIn.close();
    }
    
  }
  
  static class WritableSerializer extends SerializerBase<Writable> {
    
    private Map<String, String> metadata;
    private DataOutputStream dataOut;
    
    public WritableSerializer(Map<String, String> metadata) {
      this.metadata = metadata;
    }
    
    @Override
    public void open(OutputStream out) {
      if (out instanceof DataOutputStream) {
        dataOut = (DataOutputStream) out;
      } else {
        dataOut = new DataOutputStream(out);
      }
    }

    @Override
    public void serialize(Writable w) throws IOException {
      w.write(dataOut);
    }

    @Override
    public void close() throws IOException {
      dataOut.close();
    }

    @Override
    public Map<String, String> getMetadata() throws IOException {
      return metadata;
    }

  }

  @Override
  public boolean accept(Map<String, String> metadata) {
    if (getClass().getName().equals(metadata.get(SERIALIZATION_KEY))) {
      return true;
    }
    Class<?> c = getClassFromMetadata(metadata);
    return c == null ? false : Writable.class.isAssignableFrom(c);
  }

  @Override
  public SerializerBase<Writable> getSerializer(Map<String, String> metadata) {
    return new WritableSerializer(metadata);
  }
  
  @Override
  public DeserializerBase<Writable> getDeserializer(Map<String, String> metadata) {
    Class<?> c = getClassFromMetadata(metadata);
    return new WritableDeserializer(getConf(), c);
  }

}
