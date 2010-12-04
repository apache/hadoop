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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serial.TypedSerialization;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link TypedSerialization} for {@link Writable}s that delegates to
 * {@link Writable#write} and {@link Writable#readFields}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WritableSerialization extends TypedSerialization<Writable> {

  public WritableSerialization() {}
  
  public WritableSerialization(Class<? extends Writable> specificType) {
    super(specificType);
  }

  @Override
  public Writable deserialize(InputStream stream,
                              Writable w,
                              Configuration conf) throws IOException {
    Writable writable;
    if (w == null) {
      writable  = (Writable) ReflectionUtils.newInstance(specificType, conf);
    } else {
      if (w.getClass() != specificType) {
        throw new IllegalArgumentException("Type mismatch in deserialization: "+
                                           "expected: " + specificType +
                                           "; received " + w.getClass());
      }
      writable = w;
    }
    writable.readFields(ensureDataInput(stream));
    return writable;
  }

  @Override
  public void serialize(OutputStream out, Writable w) throws IOException {
    if (specificType != w.getClass()) {
      throw new IOException("Type mismatch in serialization: expected "
          + specificType + "; received " + w.getClass());
    }
    w.write(ensureDataOutput(out));
  }

  @Override
  @SuppressWarnings("unchecked")
  public RawComparator getRawComparator() {
    return (RawComparator) WritableComparator.get(
        (Class<WritableComparable>) specificType);
  }
  
  @Override
  public Class<Writable> getBaseType() {
    return Writable.class;
  }
  
  @Override
  public String getName() {
    return "writable";
  }
}
