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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;

/**
 * Base mapper for writing entities to the timeline service. Subclasses
 * override {@link #writeEntities(Configuration, TimelineCollectorManager,
 * org.apache.hadoop.mapreduce.Mapper.Context)} to create and write entities
 * to the timeline service.
 */
abstract class EntityWriterV2
    extends org.apache.hadoop.mapreduce.Mapper
        <IntWritable, IntWritable, Writable, Writable> {
  @Override
  public void map(IntWritable key, IntWritable val, Context context)
      throws IOException {

    // create the timeline collector manager wired with the writer
    Configuration tlConf = new YarnConfiguration();
    TimelineCollectorManager manager = new TimelineCollectorManager("test");
    manager.init(tlConf);
    manager.start();
    try {
      // invoke the method to have the subclass write entities
      writeEntities(tlConf, manager, context);
    } finally {
      manager.close();
    }
  }

  protected abstract void writeEntities(Configuration tlConf,
      TimelineCollectorManager manager, Context context) throws IOException;
}