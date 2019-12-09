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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HadoopPlatform extends Platform {
  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopPlatform.class);

  public HadoopPlatform() throws IOException {
  }

  @Override
  public void init() throws IOException {
    registerKey(NullWritable.class.getName(), NullWritableSerializer.class);
    registerKey(Text.class.getName(), TextSerializer.class);
    registerKey(LongWritable.class.getName(), LongWritableSerializer.class);
    registerKey(IntWritable.class.getName(), IntWritableSerializer.class);
    registerKey(Writable.class.getName(), DefaultSerializer.class);
    registerKey(BytesWritable.class.getName(), BytesWritableSerializer.class);
    registerKey(BooleanWritable.class.getName(), BoolWritableSerializer.class);
    registerKey(ByteWritable.class.getName(), ByteWritableSerializer.class);
    registerKey(FloatWritable.class.getName(), FloatWritableSerializer.class);
    registerKey(DoubleWritable.class.getName(), DoubleWritableSerializer.class);
    registerKey(VIntWritable.class.getName(), VIntWritableSerializer.class);
    registerKey(VLongWritable.class.getName(), VLongWritableSerializer.class);

    LOG.info("Hadoop platform inited");
  }

  @Override
  public boolean support(String keyClassName, INativeSerializer<?> serializer, JobConf job) {
    if (keyClassNames.contains(keyClassName)
      && serializer instanceof INativeComparable) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean define(Class<?> comparatorClass) {
    return false;
  }

  @Override
  public String name() {
    return "Hadoop";
  }
}
