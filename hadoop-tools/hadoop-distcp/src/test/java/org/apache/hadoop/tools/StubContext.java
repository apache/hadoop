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

package org.apache.hadoop.tools;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class StubContext {

  private StubStatusReporter reporter = new StubStatusReporter();
  private RecordReader<Text, CopyListingFileStatus> reader;
  private StubInMemoryWriter writer = new StubInMemoryWriter();
  private Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapperContext;

  public StubContext(Configuration conf,
      RecordReader<Text, CopyListingFileStatus> reader, int taskId)
      throws IOException, InterruptedException {

    WrappedMapper<Text, CopyListingFileStatus, Text, Text> wrappedMapper
            = new WrappedMapper<Text, CopyListingFileStatus, Text, Text>();

    MapContextImpl<Text, CopyListingFileStatus, Text, Text> contextImpl
            = new MapContextImpl<Text, CopyListingFileStatus, Text, Text>(conf,
            getTaskAttemptID(taskId), reader, writer,
            null, reporter, null);

    this.reader = reader;
    this.mapperContext = wrappedMapper.getMapContext(contextImpl);
  }

  public Mapper<Text, CopyListingFileStatus, Text, Text>.Context getContext() {
    return mapperContext;
  }

  public StatusReporter getReporter() {
    return reporter;
  }

  public RecordReader<Text, CopyListingFileStatus> getReader() {
    return reader;
  }

  public void setReader(RecordReader<Text, CopyListingFileStatus> reader) {
    this.reader = reader;
  }

  public StubInMemoryWriter getWriter() {
    return writer;
  }

  public static class StubStatusReporter extends StatusReporter {

    private Counters counters = new Counters();

    public StubStatusReporter() {
	    /*
      final CounterGroup counterGroup
              = new CounterGroup("FileInputFormatCounters",
                                 "FileInputFormatCounters");
      counterGroup.addCounter(new Counter("BYTES_READ",
                                          "BYTES_READ",
                                          0));
      counters.addGroup(counterGroup);
      */
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return counters.findCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return counters.findCounter(group, name);
    }

    @Override
    public void progress() {}

    @Override
    public float getProgress() {
      return 0F;
    }

    @Override
    public void setStatus(String status) {}
  }


  public static class StubInMemoryWriter extends RecordWriter<Text, Text> {

    List<Text> keys = new ArrayList<Text>();

    List<Text> values = new ArrayList<Text>();

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
      keys.add(key);
      values.add(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    }

    public List<Text> keys() {
      return keys;
    }

    public List<Text> values() {
      return values;
    }

  }

  public static TaskAttemptID getTaskAttemptID(int taskId) {
    return new TaskAttemptID("", 0, TaskType.MAP, taskId, 0);
  }
}
