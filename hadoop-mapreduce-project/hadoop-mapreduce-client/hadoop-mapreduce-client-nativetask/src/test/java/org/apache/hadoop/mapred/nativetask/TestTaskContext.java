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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.junit.Test;
import org.junit.Assert;

public class TestTaskContext {

  @Test
  public void testTaskContext() {
    TaskContext context = new TaskContext(null, null, null, null, null, null,
        null);
    
    context.setInputKeyClass(IntWritable.class);
    Assert.assertEquals(IntWritable.class.getName(), context.getInputKeyClass
        ().getName());
 
    context.setInputValueClass(Text.class);
    Assert.assertEquals(Text.class.getName(), context.getInputValueClass()
        .getName());
   
    context.setOutputKeyClass(LongWritable.class);
    Assert.assertEquals(LongWritable.class.getName(), context
        .getOutputKeyClass().getName());

    context.setOutputValueClass(FloatWritable.class);
    Assert.assertEquals(FloatWritable.class.getName(), context
        .getOutputValueClass().getName());
  }
}
