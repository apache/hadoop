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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.INativeComparable;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings({ "rawtypes" })
public class TestNativeSerialization {
  @Test
  public void testRegisterAndGet() throws IOException {
    final NativeSerialization serialization = NativeSerialization.getInstance();
    serialization.reset();

    serialization.register(WritableKey.class.getName(), ComparableKeySerializer.class);

    INativeSerializer serializer = serialization.getSerializer(WritableKey.class);
    Assert.assertEquals(ComparableKeySerializer.class.getName(), serializer.getClass().getName());

    serializer = serialization.getSerializer(WritableValue.class);
    Assert.assertEquals(DefaultSerializer.class.getName(), serializer.getClass().getName());

    boolean ioExceptionThrown = false;
    try {
      serializer = serialization.getSerializer(NonWritableValue.class);
    } catch (final IOException e) {
      ioExceptionThrown = true;
    }
    Assert.assertTrue(ioExceptionThrown);
  }

  public static class WritableKey implements Writable {
    private int value;

    public WritableKey(int a) {
      this.value = a;
    }

    public int getLength() {
      return 4;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int v) {
      this.value = v;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class WritableValue implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  public static class NonWritableValue {
  }

  public static class ComparableKeySerializer
    implements INativeComparable, INativeSerializer<WritableKey> {

    @Override
    public int getLength(WritableKey w) throws IOException {
      return w.getLength();
    }

    @Override
    public void serialize(WritableKey w, DataOutput out) throws IOException {
      out.writeInt(w.getValue());
    }

    @Override
    public void deserialize(DataInput in, int length, WritableKey w) throws IOException {
      w.setValue(in.readInt());
    }
  }
}
