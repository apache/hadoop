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

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ReflectionUtils;
import junit.framework.TestCase;
import java.io.*;
import java.util.*;

/** 
 * TestMapOutputOrder checks if there is a 1-1 correspondence between
 * the order of Map Input files (returned from InputFormat.getSplits())
 * and the Map Output Files
 */
public class TestMapOutputOrder extends TestCase 
{
  private static final Log LOG =
    LogFactory.getLog(TestTextInputFormat.class.getName());

  JobConf jobConf = new JobConf(TestMapOutputOrder.class);
  JobClient jc;

  private static class TestMapper extends MapReduceBase implements Mapper {
    public void map(WritableComparable key, Writable val,
                   OutputCollector output, Reporter reporter)
      throws IOException {
      output.collect(null, val);
    }
  }

  private static void writeFile(FileSystem fs, Path name,
                                CompressionCodec codec,
                                String contents) throws IOException {
    OutputStream stm;
    if (codec == null) {
      stm = fs.create(name);
    } else {
      stm = codec.createOutputStream(fs.create(name));
    }
    stm.write(contents.getBytes()); 
    stm.close();
  } 

  private static String readFile(FileSystem fs, Path name,
                                CompressionCodec codec) throws IOException {
    InputStream stm;
    if (codec == null) {
      stm = fs.open(name);
    } else {
      stm = codec.createInputStream(fs.open(name));
    }

    String contents = "";
    int b = stm.read();
    while (b != -1) {
       contents += (char) b;
       b = stm.read();
    }
    stm.close();
    return contents;
  }

  public void testMapOutputOrder() throws Exception {
    String nameNode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;

    try {
      final int taskTrackers = 3;
      final int jobTrackerPort = 60070;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 1, true, null);
      fileSys = dfs.getFileSystem();
      nameNode = fileSys.getName();
      mr = new MiniMRCluster(taskTrackers, nameNode, 3);
      final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();

      Path testdir = new Path("/testing/mapoutputorder/");
      Path inDir = new Path(testdir, "input");
      Path outDir = new Path(testdir, "output");
      FileSystem fs = FileSystem.getNamed(nameNode, conf);
      fs.delete(testdir);
      jobConf.set("fs.default.name", nameNode);
      jobConf.set("mapred.job.tracker", jobTrackerName);
      jobConf.setInputFormat(TextInputFormat.class);
      jobConf.setInputPath(inDir);
      jobConf.setOutputPath(outDir);
      jobConf.setMapperClass(TestMapper.class);
      jobConf.setNumMapTasks(3);
      jobConf.setMapOutputKeyClass(LongWritable.class);
      jobConf.setMapOutputValueClass(Text.class); 
      jobConf.setNumReduceTasks(0);
      jobConf.setJar("build/test/testjar/testjob.jar");

      if (!fs.mkdirs(testdir)) {
        throw new IOException("Mkdirs failed to create " + testdir.toString());
      }
      if (!fs.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }

      // create input files
      CompressionCodec gzip = new GzipCodec();
      ReflectionUtils.setConf(gzip, jobConf);
      String[] inpStrings = new String[3];
      inpStrings[0] = "part1_line1\npart1_line2\n";
      inpStrings[1] = "part2_line1\npart2_line2\npart2_line3\n";
      inpStrings[2] = "part3_line1\n";
      writeFile(fs, new Path(inDir, "part1.txt.gz"), gzip, inpStrings[0]);
      writeFile(fs, new Path(inDir, "part2.txt.gz"), gzip, inpStrings[1]);
      writeFile(fs, new Path(inDir, "part3.txt.gz"), gzip, inpStrings[2]);

      // run job
      jc = new JobClient(jobConf);

      RunningJob rj = jc.runJob(jobConf);
      assertTrue("job was complete", rj.isComplete());
      assertTrue("job was successful", rj.isSuccessful());

      // check map output files
      Path[] outputPaths = fs.listPaths(outDir);
      String contents;
      for (int i = 0; i < outputPaths.length; i++) {
        LOG.debug("Output Path (#" + (i+1) +"): " + outputPaths[i].getName());
        contents = readFile(fs, outputPaths[i], null);
        LOG.debug("Contents: " + contents);
        assertTrue(new String("Input File #" + (i+1) + " == Map Output File #" + (i+1)), inpStrings[i].equals(contents));
      }
    }
    finally {
      // clean-up
      if (fileSys != null) { fileSys.close(); }
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }
}
