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
package org.apache.hadoop.mapred.nativetask.testutil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;

public class MockValueClass implements Writable {
  private final static int DEFAULT_ARRAY_LENGTH = 16;
  private int a = 0;
  private byte[] array;
  private final LongWritable longWritable;
  private final Text txt;
  private final Random rand = new Random();

  public MockValueClass() {
    a = rand.nextInt();
    array = new byte[DEFAULT_ARRAY_LENGTH];
    rand.nextBytes(array);
    longWritable = new LongWritable(rand.nextLong());
    txt = new Text(BytesUtil.toStringBinary(array));
  }

  public MockValueClass(byte[] seed) {
    a = seed.length;
    array = new byte[seed.length];
    System.arraycopy(seed, 0, array, 0, seed.length);
    longWritable = new LongWritable(a);
    txt = new Text(BytesUtil.toStringBinary(array));
  }

  public void set(byte[] seed) {
    a = seed.length;
    array = new byte[seed.length];
    System.arraycopy(seed, 0, array, 0, seed.length);
    longWritable.set(a);
    txt.set(BytesUtil.toStringBinary(array));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(a);
    out.writeInt(array.length);
    out.write(array);
    longWritable.write(out);
    txt.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    a = in.readInt();
    final int length = in.readInt();
    array = new byte[length];
    in.readFully(array);
    longWritable.readFields(in);
    txt.readFields(in);
  }
}
