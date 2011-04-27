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

package org.apache.hadoop.fs;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;

/**
 * test the har file system
 * create a har filesystem
 * run fs commands
 * and then run a map reduce job
 */
public class TestHarFileSystem extends TestCase {
  private Path inputPath;
  private MiniDFSCluster dfscluster;
  private MiniMRCluster mapred;
  private FileSystem fs;
  private Path filea, fileb, filec, filed;
  private Path archivePath;
  
  protected void setUp() throws Exception {
    super.setUp();
    dfscluster = new MiniDFSCluster(new JobConf(), 2, true, null);
    fs = dfscluster.getFileSystem();
    mapred = new MiniMRCluster(2, fs.getUri().toString(), 1);
    inputPath = new Path(fs.getHomeDirectory(), "test"); 
    filea = new Path(inputPath,"a");
    fileb = new Path(inputPath,"b");
    filec = new Path(inputPath,"c");
    // check for har containing escape worthy characters
    // in there name
    filed = new Path(inputPath, "d%d");
    archivePath = new Path(fs.getHomeDirectory(), "tmp");
  }
  
  protected void tearDown() throws Exception {
    try {
      if (mapred != null) {
        mapred.shutdown();
      }
      if (dfscluster != null) {
        dfscluster.shutdown();
      }
    } catch(Exception e) {
      System.err.println(e);
    }
    super.tearDown();
  }
  
  static class TextMapperReducer implements Mapper<LongWritable, Text, Text, Text>, 
            Reducer<Text, Text, Text, Text> {
    
    public void configure(JobConf conf) {
      //do nothing 
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      output.collect(value, new Text(""));
    }

    public void close() throws IOException {
      // do nothing
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      while(values.hasNext()) { 
        values.next();
        output.collect(key, null);
      }
    }
  }
  
  public void testArchives() throws Exception {
    fs.mkdirs(inputPath);
    
    FSDataOutputStream out = fs.create(filea); 
    out.write("a".getBytes());
    out.close();
    out = fs.create(fileb);
    out.write("b".getBytes());
    out.close();
    out = fs.create(filec);
    out.write("c".getBytes());
    out.close();
    out = fs.create(filed);
    out.write("d".getBytes());
    out.close();
    Configuration conf = mapred.createJobConf();
    
    // check to see if fs.har.impl.disable.cache is true
    boolean archivecaching = conf.getBoolean("fs.har.impl.disable.cache", false);
    assertTrue(archivecaching);
    HadoopArchives har = new HadoopArchives(conf);
    String[] args = new String[3];
    //check for destination not specfied
    args[0] = "-archiveName";
    args[1] = "foo.har";
    args[2] = inputPath.toString();
    int ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
    args = new String[4];
    //check for wrong archiveName
    args[0] = "-archiveName";
    args[1] = "/d/foo.har";
    args[2] = inputPath.toString();
    args[3] = archivePath.toString();
    ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
//  se if dest is a file 
    args[1] = "foo.har";
    args[3] = filec.toString();
    ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
    //this is a valid run
    args[0] = "-archiveName";
    args[1] = "foo.har";
    args[2] = inputPath.toString();
    args[3] = archivePath.toString();
    ret = ToolRunner.run(har, args);
    //checl for the existenece of the archive
    assertTrue(ret == 0);
    ///try running it again. it should not 
    // override the directory
    ret = ToolRunner.run(har, args);
    assertTrue(ret != 0);
    Path finalPath = new Path(archivePath, "foo.har");
    Path fsPath = new Path(inputPath.toUri().getPath());
    String relative = fsPath.toString().substring(1);
    Path filePath = new Path(finalPath, relative);
    //make it a har path 
    Path harPath = new Path("har://" + filePath.toUri().getPath());
    assertTrue(fs.exists(new Path(finalPath, "_index")));
    assertTrue(fs.exists(new Path(finalPath, "_masterindex")));
    assertTrue(!fs.exists(new Path(finalPath, "_logs")));
    //creation tested
    //check if the archive is same
    // do ls and cat on all the files
    FsShell shell = new FsShell(conf);
    args = new String[2];
    args[0] = "-ls";
    args[1] = harPath.toString();
    ret = ToolRunner.run(shell, args);
    // ls should work.
    assertTrue((ret == 0));
    //now check for contents of filea
    // fileb and filec
    Path harFilea = new Path(harPath, "a");
    Path harFileb = new Path(harPath, "b");
    Path harFilec = new Path(harPath, "c");
    Path harFiled = new Path(harPath, "d%d");
    FileSystem harFs = harFilea.getFileSystem(conf);
    FSDataInputStream fin = harFs.open(harFilea);
    byte[] b = new byte[4];
    int readBytes = fin.read(b);
    fin.close();
    assertTrue("strings are equal ", (b[0] == "a".getBytes()[0]));
    fin = harFs.open(harFileb);
    fin.read(b);
    fin.close();
    assertTrue("strings are equal ", (b[0] == "b".getBytes()[0]));
    fin = harFs.open(harFilec);
    fin.read(b);
    fin.close();
    assertTrue("strings are equal ", (b[0] == "c".getBytes()[0]));
    fin = harFs.open(harFiled);
    fin.read(b);
    fin.close();
    assertTrue("strings are equal ", (b[0] == "d".getBytes()[0]));
    
    // ok all files match 
    // run a map reduce job
    Path outdir = new Path(fs.getHomeDirectory(), "mapout"); 
    JobConf jobconf = mapred.createJobConf();
    FileInputFormat.addInputPath(jobconf, harPath);
    jobconf.setInputFormat(TextInputFormat.class);
    jobconf.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(jobconf, outdir);
    jobconf.setMapperClass(TextMapperReducer.class);
    jobconf.setMapOutputKeyClass(Text.class);
    jobconf.setMapOutputValueClass(Text.class);
    jobconf.setReducerClass(TextMapperReducer.class);
    jobconf.setNumReduceTasks(1);
    JobClient.runJob(jobconf);
    args[1] = outdir.toString();
    ret = ToolRunner.run(shell, args);
    
    FileStatus[] status = fs.globStatus(new Path(outdir, "part*"));
    Path reduceFile = status[0].getPath();
    FSDataInputStream reduceIn = fs.open(reduceFile);
    b = new byte[8];
    reduceIn.read(b);
    //assuming all the 8 bytes were read.
    Text readTxt = new Text(b);
    assertTrue("a\nb\nc\nd\n".equals(readTxt.toString()));
    assertTrue("number of bytes left should be -1", reduceIn.read(b) == -1);
    reduceIn.close();
  }
}
