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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TestMRJobClient;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.util.Tool;

import org.junit.BeforeClass;
import org.junit.Ignore;
@Ignore
public class TestMRCJCJobClient extends TestMRJobClient {

  @BeforeClass
  public static void setupClass() throws Exception {
    setupClassBase(TestMRCJCJobClient.class);
  }

  private String runJob() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(),
                        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello1\n");
    wr.write("hello2\n");
    wr.write("hello3\n");
    wr.close();

    JobConf conf = createJobConf();
    conf.setJobName("mr");
    conf.setJobPriority(JobPriority.HIGH);
    
    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

    FileInputFormat.setInputPaths(conf, getInputDir());
    FileOutputFormat.setOutputPath(conf, getOutputDir());

    return JobClient.runJob(conf).getID().toString();
  }
  
  public static int runTool(Configuration conf, Tool tool, String[] args,
      OutputStream out) throws Exception {
    return TestMRJobClient.runTool(conf, tool, args, out);
  }
  
  static void verifyJobPriority(String jobId, String priority,
      JobConf conf)  throws Exception {
    TestMRCJCJobClient test = new TestMRCJCJobClient();
    test.verifyJobPriority(jobId, priority, conf, test.createJobClient());
  }
  
  public void testJobClient() throws Exception {
    Configuration conf = createJobConf();
    String jobId = runJob();
    testGetCounter(jobId, conf);
    testAllJobList(jobId, conf);
    testChangingJobPriority(jobId, conf);
  }
  
  protected CLI createJobClient() 
      throws IOException {
    return new JobClient();
  }
}
