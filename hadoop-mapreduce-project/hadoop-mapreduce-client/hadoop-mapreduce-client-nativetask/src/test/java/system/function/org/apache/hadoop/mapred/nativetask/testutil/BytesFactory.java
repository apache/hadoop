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

import java.util.Random;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.nativetask.util.BytesUtil;


public class BytesFactory {
  public static Random r = new Random();

  public static Object newObject(byte[] seed, String className) {
    r.setSeed(seed.hashCode());
    if (className.equals(IntWritable.class.getName())) {
      return new IntWritable(Ints.fromByteArray(seed));
    } else if (className.equals(FloatWritable.class.getName())) {
      return new FloatWritable(r.nextFloat());
    } else if (className.equals(DoubleWritable.class.getName())) {
      return new DoubleWritable(r.nextDouble());
    } else if (className.equals(LongWritable.class.getName())) {
      return new LongWritable(Longs.fromByteArray(seed));
    } else if (className.equals(VIntWritable.class.getName())) {
      return new VIntWritable(Ints.fromByteArray(seed));
    } else if (className.equals(VLongWritable.class.getName())) {
      return new VLongWritable(Longs.fromByteArray(seed));
    } else if (className.equals(BooleanWritable.class.getName())) {
      return new BooleanWritable(seed[0] % 2 == 1 ? true : false);
    } else if (className.equals(Text.class.getName())) {
      return new Text(BytesUtil.toStringBinary(seed));
    } else if (className.equals(ByteWritable.class.getName())) {
      return new ByteWritable(seed.length > 0 ? seed[0] : 0);
    } else if (className.equals(BytesWritable.class.getName())) {
      return new BytesWritable(seed);
    } else if (className.equals(UTF8.class.getName())) {
      return new UTF8(BytesUtil.toStringBinary(seed));
    } else if (className.equals(MockValueClass.class.getName())) {
      return new MockValueClass(seed);
    } else {
      return null;
    }
  }


  public static <VTYPE> byte[] fromBytes(byte[] bytes) throws Exception {
    throw new Exception("Not supported");
  }

  public static <VTYPE> byte[] toBytes(VTYPE obj) {
    final String className = obj.getClass().getName();
    if (className.equals(IntWritable.class.getName())) {
      return Ints.toByteArray(((IntWritable) obj).get());
    } else if (className.equals(FloatWritable.class.getName())) {
      return BytesUtil.toBytes(((FloatWritable) obj).get());
    } else if (className.equals(DoubleWritable.class.getName())) {
      return BytesUtil.toBytes(((DoubleWritable) obj).get());
    } else if (className.equals(LongWritable.class.getName())) {
      return Longs.toByteArray(((LongWritable) obj).get());
    } else if (className.equals(VIntWritable.class.getName())) {
      return Ints.toByteArray(((VIntWritable) obj).get());
    } else if (className.equals(VLongWritable.class.getName())) {
      return Longs.toByteArray(((VLongWritable) obj).get());
    } else if (className.equals(BooleanWritable.class.getName())) {
      return BytesUtil.toBytes(((BooleanWritable) obj).get());
    } else if (className.equals(Text.class.getName())) {
      return ((Text)obj).copyBytes();
    } else if (className.equals(ByteWritable.class.getName())) {
      return Ints.toByteArray((int) ((ByteWritable) obj).get());
    } else if (className.equals(BytesWritable.class.getName())) {
      // TODO: copyBytes instead?
      return ((BytesWritable) obj).getBytes();
    } else {
      return new byte[0];
    }
  }
}
