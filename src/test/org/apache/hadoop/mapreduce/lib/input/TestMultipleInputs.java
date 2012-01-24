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
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @see TestDelegatingInputFormat
 */
public class TestMultipleInputs extends TestCase {
  
  public void testAddInputPathWithFormat() throws IOException {
    final Job job = new Job();
    MultipleInputs.addInputPath(job, new Path("/foo"), TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path("/bar"),
        KeyValueTextInputFormat.class);
    final Map<Path, InputFormat> inputs = MultipleInputs
       .getInputFormatMap(new JobContext(job.getConfiguration(), new JobID()));
    assertEquals(TextInputFormat.class, inputs.get(new Path("/foo")).getClass());
    assertEquals(KeyValueTextInputFormat.class, inputs.get(new Path("/bar"))
       .getClass());
  }

  public void testAddInputPathWithMapper() throws IOException {
    final Job job = new Job();
    MultipleInputs.addInputPath(job, new Path("/foo"), TextInputFormat.class,
       MapClass.class);
    MultipleInputs.addInputPath(job, new Path("/bar"),
       KeyValueTextInputFormat.class, MapClass2.class);
    final Map<Path, InputFormat> inputs = MultipleInputs
       .getInputFormatMap(job);
    final Map<Path, Class<? extends Mapper>> maps = MultipleInputs
       .getMapperTypeMap(job);

    assertEquals(TextInputFormat.class, inputs.get(new Path("/foo")).getClass());
    assertEquals(KeyValueTextInputFormat.class, inputs.get(new Path("/bar"))
       .getClass());
    assertEquals(MapClass.class, maps.get(new Path("/foo")));
    assertEquals(MapClass2.class, maps.get(new Path("/bar")));
  }

  static class MapClass extends Mapper<String, String, String, String> {
  }

  static class MapClass2 extends MapClass {
  }
}
