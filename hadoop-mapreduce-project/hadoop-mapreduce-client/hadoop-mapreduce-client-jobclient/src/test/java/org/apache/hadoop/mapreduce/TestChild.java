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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestChild extends HadoopTestCase {
  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data","/tmp"))
    .toURI().toString().replace(' ', '+');
  private final Path inDir = new Path(TEST_ROOT_DIR, "./wc/input");
  private final Path outDir = new Path(TEST_ROOT_DIR, "./wc/output");
  
  private final static String OLD_CONFIGS = "test.old.configs";
  private final static String TASK_OPTS_VAL = "-Xmx200m";
  private final static String MAP_OPTS_VAL = "-Xmx200m";
  private final static String REDUCE_OPTS_VAL = "-Xmx300m";
  
  public TestChild() throws IOException {
    super(HadoopTestCase.CLUSTER_MR , HadoopTestCase.LOCAL_FS, 2, 2);
  }

  static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      boolean oldConfigs = conf.getBoolean(OLD_CONFIGS, false);
      if (oldConfigs) {
        String javaOpts = conf.get(JobConf.MAPRED_TASK_JAVA_OPTS);
        assertNotNull(javaOpts, JobConf.MAPRED_TASK_JAVA_OPTS + " is null!");
        assertEquals(javaOpts, TASK_OPTS_VAL,
            JobConf.MAPRED_TASK_JAVA_OPTS + " has value of: " + javaOpts);
      } else {
        String mapJavaOpts = conf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS);
        assertNotNull(mapJavaOpts, JobConf.MAPRED_MAP_TASK_JAVA_OPTS + " is null!");
        assertEquals(mapJavaOpts, MAP_OPTS_VAL,
            JobConf.MAPRED_MAP_TASK_JAVA_OPTS + " has value of: " + mapJavaOpts);
      }
      
      Level logLevel = 
        Level.toLevel(conf.get(JobConf.MAPRED_MAP_TASK_LOG_LEVEL, 
                               Level.INFO.toString()));  
      assertEquals(logLevel, Level.OFF,
          JobConf.MAPRED_MAP_TASK_LOG_LEVEL + "has value of " + logLevel);
    }
  }
  
  static class MyReducer 
  extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      boolean oldConfigs = conf.getBoolean(OLD_CONFIGS, false);
      if (oldConfigs) {
        String javaOpts = conf.get(JobConf.MAPRED_TASK_JAVA_OPTS);
        assertNotNull(javaOpts, JobConf.MAPRED_TASK_JAVA_OPTS + " is null!");
        assertEquals(javaOpts, TASK_OPTS_VAL,
            JobConf.MAPRED_TASK_JAVA_OPTS + " has value of: " + javaOpts);
      } else {
        String reduceJavaOpts = conf.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS);
        assertNotNull(reduceJavaOpts,
            JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS + " is null!");
        assertEquals(reduceJavaOpts, REDUCE_OPTS_VAL,
            JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS + " has value of: " + reduceJavaOpts);
      }
      
      Level logLevel = 
        Level.toLevel(conf.get(JobConf.MAPRED_REDUCE_TASK_LOG_LEVEL, 
                               Level.INFO.toString()));  
      assertEquals(logLevel, Level.OFF,
          JobConf.MAPRED_REDUCE_TASK_LOG_LEVEL + "has value of " + logLevel);
    }
  }
  
  private Job submitAndValidateJob(JobConf conf, int numMaps, int numReds, 
                                   boolean oldConfigs) 
      throws IOException, InterruptedException, ClassNotFoundException {
    conf.setBoolean(OLD_CONFIGS, oldConfigs);
    if (oldConfigs) {
      conf.set(JobConf.MAPRED_TASK_JAVA_OPTS, TASK_OPTS_VAL);
    } else {
      conf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, MAP_OPTS_VAL);
      conf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, REDUCE_OPTS_VAL);
    }
    
    conf.set(JobConf.MAPRED_MAP_TASK_LOG_LEVEL, Level.OFF.toString());
    conf.set(JobConf.MAPRED_REDUCE_TASK_LOG_LEVEL, Level.OFF.toString());
    
    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 
                numMaps, numReds);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    assertFalse(job.isConnected(),
        "Job already has a job tracker connection, before it's submitted");
    job.submit();
    assertTrue(job.isConnected(),
        "Job doesn't have a job tracker connection, even though it's been submitted");
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());

    // Check output directory
    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(outDir), "Job output directory doesn't exit!");
    FileStatus[] list = fs.listStatus(outDir, new OutputFilter());
    int numPartFiles = numReds == 0 ? numMaps : numReds;
    assertTrue(list.length == numPartFiles,
        "Number of part-files is " + list.length + " and not " + numPartFiles);
    return job;
  }

  @Test
  public void testChild() throws Exception {
    try {
      submitAndValidateJob(createJobConf(), 1, 1, true);
      submitAndValidateJob(createJobConf(), 1, 1, false);
    } finally {
      tearDown();
    }
  }
  
  private static class OutputFilter implements PathFilter {
    public boolean accept(Path path) {
      return !(path.getName().startsWith("_"));
    }
  }
}
