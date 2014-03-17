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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with multiple directories
 * and check for correct classpath
 */
public class TestMiniMRClasspath {
  
  
  static void configureWordCount(FileSystem fs, JobConf conf, String input,
      int numMaps, int numReduces, Path inDir, Path outDir) throws IOException {
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes(input);
    file.close();
    FileSystem.setDefaultUri(conf, fs.getUri());
    conf.set(JTConfig.FRAMEWORK_NAME, JTConfig.YARN_FRAMEWORK_NAME);
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);
    
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.set("mapred.mapper.class", "testjar.ClassWordCount$MapClass");        
    conf.set("mapred.combine.class", "testjar.ClassWordCount$Reduce");
    conf.set("mapred.reducer.class", "testjar.ClassWordCount$Reduce");
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    //set the tests jar file
    conf.setJarByClass(TestMiniMRClasspath.class);
  }
  
  static String launchWordCount(URI fileSys, JobConf conf, String input,
                                int numMaps, int numReduces) 
  throws IOException {
    final Path inDir = new Path("/testing/wc/input");
    final Path outDir = new Path("/testing/wc/output");
    FileSystem fs = FileSystem.get(fileSys, conf);
    configureWordCount(fs, conf, input, numMaps, numReduces, inDir, outDir);
    JobClient.runJob(conf);
    StringBuffer result = new StringBuffer();
    {
      Path[] parents = FileUtil.stat2Paths(fs.listStatus(outDir.getParent()));
      Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
              new Utils.OutputFileUtils.OutputFilesFilter()));
      for(int i=0; i < fileList.length; ++i) {
        BufferedReader file = 
          new BufferedReader(new InputStreamReader(fs.open(fileList[i])));
        String line = file.readLine();
        while (line != null) {
          result.append(line);
          result.append("\n");
          line = file.readLine();
        }
        file.close();
      }
    }
    return result.toString();
  }

  static String launchExternal(URI uri, JobConf conf, String input,
                               int numMaps, int numReduces)
    throws IOException {

    final Path inDir = new Path("/testing/ext/input");
    final Path outDir = new Path("/testing/ext/output");
    FileSystem fs = FileSystem.get(uri, conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    FileSystem.setDefaultUri(conf, uri);
    conf.set(JTConfig.FRAMEWORK_NAME, JTConfig.YARN_FRAMEWORK_NAME);
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are counts
    conf.setOutputValueClass(IntWritable.class);
    // the values are the messages
    conf.set(JobContext.OUTPUT_KEY_CLASS, "testjar.ExternalWritable");

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    
    conf.set("mapred.mapper.class", "testjar.ExternalMapperReducer"); 
    conf.set("mapred.reducer.class", "testjar.ExternalMapperReducer");

    // set the tests jar file
    conf.setJarByClass(TestMiniMRClasspath.class);
    JobClient.runJob(conf);
    StringBuffer result = new StringBuffer();
    Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
                                 new Utils.OutputFileUtils
                                          .OutputFilesFilter()));
    for (int i = 0; i < fileList.length; ++i) {
      BufferedReader file = new BufferedReader(new InputStreamReader(
                                                                     fs.open(fileList[i])));
      String line = file.readLine();
      while (line != null) {
        result.append(line);
        line = file.readLine();
        result.append("\n");
      }
      file.close();
    }

    return result.toString();
  }
   
  @Test
  public void testClassPath() throws IOException {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;
      final int jobTrackerPort = 60050;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster.Builder(conf).build();
      fileSys = dfs.getFileSystem();
      namenode = fileSys.getUri().toString();
      mr = new MiniMRCluster(taskTrackers, namenode, 3);
      JobConf jobConf = mr.createJobConf();
      String result;
      result = launchWordCount(fileSys.getUri(), jobConf,
          "The quick brown fox\nhas many silly\n" + "red fox sox\n", 3, 1);
      Assert.assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n"
          + "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result);
          
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
  @Test
  public void testExternalWritable()
    throws IOException {
 
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;

    try {
      
      final int taskTrackers = 4;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster.Builder(conf).build();
      fileSys = dfs.getFileSystem();
      namenode = fileSys.getUri().toString();
      mr = new MiniMRCluster(taskTrackers, namenode, 3);      
      JobConf jobConf = mr.createJobConf();
      String result;
      
      result = launchExternal(fileSys.getUri(), jobConf,
          "Dennis was here!\nDennis again!", 3, 1);
      Assert.assertEquals("Dennis again!\t1\nDennis was here!\t1\n", result);
      
    } 
    finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
}
