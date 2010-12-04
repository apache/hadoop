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
package org.apache.hadoop.io.serial.lib.thrift;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.TypedSerialization;
import org.apache.hadoop.io.serial.lib.DeserializationRawComparator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;

/**
 * Serialize using the compact Thrift representation.
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ThriftSerialization extends TypedSerialization<TBase> {
  private final TProtocol protocol;
  private final StreamTransport transport;

  public ThriftSerialization() {
    transport = new StreamTransport();
    protocol = new TCompactProtocol(transport);
  }

  public ThriftSerialization(Class<? extends TBase> cls) {
    this();
    setSpecificType(cls);
  }

  @Override
  public Class<TBase> getBaseType() {
    return TBase.class;
  }

  @Override
  public TBase deserialize(InputStream stream, TBase reusableObject,
                           Configuration conf) throws IOException {
    transport.open(stream);
    TBase result = reusableObject;
    if (result == null) {
      result = ReflectionUtils.newInstance(getSpecificType(), conf);
    } else {
      if (specificType != result.getClass()) {
        throw new IOException("Type mismatch in deserialization: expected "
            + specificType + "; received " + result.getClass());
      }
    }
    try {
      result.read(protocol);
      transport.close();
    } catch (TException te) {
      transport.close();
      throw new IOException("problem reading thrift object", te);
    }
    return result;
  }

  @Override
  public RawComparator getRawComparator() {
    return new DeserializationRawComparator(this, null);
  }

  @Override
  public void serialize(OutputStream stream, TBase object) throws IOException {
    if (specificType != object.getClass()) {
      throw new IOException("Type mismatch in serialization: expected "
                            + specificType + "; received " + object.getClass());
    }

    transport.open(stream);
    try {
      object.write(protocol);
      transport.close();
    } catch (TException te) {
      transport.close();
      throw new IOException("problem writing thrift object", te);
    }
  }

  @Override
  public String getName() {
    return "thrift";
  }
}
