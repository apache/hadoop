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

import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class TestNonLocalJobJarSubmission extends ClusterMapReduceTestCase {
  
  public void testNonLocalJobJarSubmission() throws Exception {
    FileSystem hdfs = getFileSystem();
    
    OutputStream os = hdfs.create(new Path(getInputDir(), "dummy.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("dummy\n");
    wr.close();
    
    String jobJarPath = createAndUploadJobJar(hdfs);
    
    JobConf conf = createJobConf();
    conf.setJobName("mr");
    conf.setJobPriority(JobPriority.HIGH);
    
    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    conf.setMapperClass(ClasspathMapper.class);
    conf.setReducerClass(ClasspathReducer.class);

    FileInputFormat.setInputPaths(conf, getInputDir());
    FileOutputFormat.setOutputPath(conf, getOutputDir());
    
    conf.setJar(jobJarPath);
    
    String jobID = JobClient.runJob(conf).getID().toString();
    
    verifyClasspath(hdfs, jobID);
  }
  
  private String createAndUploadJobJar(FileSystem hdfs) throws Exception {
    File tmp =
        new File(System.getProperty("test.build.data", "/tmp"), getClass()
            .getSimpleName());
    if (!tmp.exists()) {
      tmp.mkdirs();
    }
    File jobJarFile = new File(tmp, "jobjar-on-local.jar");
    JarOutputStream jstream =
        new JarOutputStream(new FileOutputStream(jobJarFile));
    ZipEntry ze = new ZipEntry("lib/lib1.jar");
    jstream.putNextEntry(ze);
    jstream.closeEntry();
    ze = new ZipEntry("lib/lib2.jar");
    jstream.putNextEntry(ze);
    jstream.closeEntry();
    jstream.finish();
    jstream.close();
    
    Path jobJarPath = new Path(getTestRootDir().makeQualified(hdfs), "jobjar-on-hdfs.jar");
    hdfs.moveFromLocalFile(new Path(jobJarFile.toURI().toString()), jobJarPath);
    if (jobJarFile.exists()) {  // just to make sure
      jobJarFile.delete();
    }
    return jobJarPath.toUri().toString();
  }

  private void verifyClasspath(FileSystem hdfs, String jobID) throws Exception {
    boolean containsLib1Jar = false;
    String lib1JarStr = "jobcache/" + jobID + "/jars/lib/lib1.jar";
    boolean containsLib2Jar = false;
    String lib2JarStr = "jobcache/" + jobID + "/jars/lib/lib2.jar";
    
    FileStatus[] fstats = hdfs.listStatus(getOutputDir());
    for (FileStatus fstat : fstats) {
      Path p = fstat.getPath();
      if (hdfs.isFile(p) && p.getName().startsWith("part-")) {
        InputStream is = hdfs.open(p);
        Scanner sc = new Scanner(is);
        while (sc.hasNextLine()) {
          String line = sc.nextLine();
          containsLib1Jar = (containsLib1Jar || line.endsWith(lib1JarStr));
          containsLib2Jar = (containsLib2Jar || line.endsWith(lib2JarStr));
        }
        sc.close();
        is.close();
      }
    }
    
    assertTrue("lib/lib1.jar should have been unzipped from the job jar and "
            + "added to the classpath but was not", containsLib1Jar);
    assertTrue("lib/lib2.jar should have been unzipped from the job jar and "
            + "added to the classpath but was not", containsLib2Jar);
  }
  
  static class ClasspathMapper 
      implements Mapper<LongWritable, Text, IntWritable, Text> {
    
    private static final IntWritable zero = new IntWritable(0);
      
    public void configure(JobConf job) {
    }

    public void map(LongWritable key, Text val,
                    OutputCollector<IntWritable, Text> out,
                    Reporter reporter) throws IOException {
        
      ClassLoader applicationClassLoader = this.getClass().getClassLoader();
      if (applicationClassLoader == null) {
          applicationClassLoader = ClassLoader.getSystemClassLoader();
      }
      URL[] urls = ((URLClassLoader)applicationClassLoader).getURLs();
      for(URL url : urls) {
          out.collect(zero, new Text(url.toString()));
      }
    }
    
    public void close() {
    }
  }
  
  static class ClasspathReducer 
      implements Reducer<IntWritable, Text, Text, NullWritable> {
    
    public void configure(JobConf job) {
    }

    public void reduce(IntWritable key, Iterator<Text> it,
                       OutputCollector<Text, NullWritable> out,
                       Reporter reporter) throws IOException {
      while (it.hasNext()) {
        out.collect(it.next(), NullWritable.get());
      }
    }
    
    public void close() {
    }
  }
}
