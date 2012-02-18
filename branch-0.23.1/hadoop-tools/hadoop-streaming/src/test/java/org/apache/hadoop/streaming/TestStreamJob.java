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

package org.apache.hadoop.streaming;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This class tests hadoop Streaming's StreamJob class.
 */
public class TestStreamJob {
  
  @Test(expected = IllegalArgumentException.class)
  public void testCreateJobWithExtraArgs() throws IOException {
    ArrayList<String> dummyArgs = new ArrayList<String>();
    dummyArgs.add("-input"); dummyArgs.add("dummy");
    dummyArgs.add("-output"); dummyArgs.add("dummy");
    dummyArgs.add("-mapper"); dummyArgs.add("dummy");
    dummyArgs.add("dummy");
    dummyArgs.add("-reducer"); dummyArgs.add("dummy");
    StreamJob.createJob(dummyArgs.toArray(new String[] {}));
  }
  
  @Test
  public void testCreateJob() throws IOException {
    JobConf job;
    ArrayList<String> dummyArgs = new ArrayList<String>();
    dummyArgs.add("-input"); dummyArgs.add("dummy");
    dummyArgs.add("-output"); dummyArgs.add("dummy");
    dummyArgs.add("-mapper"); dummyArgs.add("dummy");
    dummyArgs.add("-reducer"); dummyArgs.add("dummy");
    ArrayList<String> args;
    
    args = new ArrayList<String>(dummyArgs);
    args.add("-inputformat");
    args.add("org.apache.hadoop.mapred.KeyValueTextInputFormat");
    job = StreamJob.createJob(args.toArray(new String[] {}));
    assertEquals(KeyValueTextInputFormat.class, job.getInputFormat().getClass());
    
    args = new ArrayList<String>(dummyArgs);
    args.add("-inputformat");
    args.add("org.apache.hadoop.mapred.SequenceFileInputFormat");
    job = StreamJob.createJob(args.toArray(new String[] {}));
    assertEquals(SequenceFileInputFormat.class, job.getInputFormat().getClass());
    
    args = new ArrayList<String>(dummyArgs);
    args.add("-inputformat");
    args.add("org.apache.hadoop.mapred.KeyValueTextInputFormat");
    args.add("-inputreader");
    args.add("StreamXmlRecordReader,begin=<doc>,end=</doc>");
    job = StreamJob.createJob(args.toArray(new String[] {}));
    assertEquals(StreamInputFormat.class, job.getInputFormat().getClass());
  }
  
  @Test
  public void testOptions() throws Exception {
    StreamJob streamJob = new StreamJob();
    assertEquals(1, streamJob.run(new String[0]));
    assertEquals(0, streamJob.run(new String[] {"-help"}));
    assertEquals(0, streamJob.run(new String[] {"-info"}));
  }
}
