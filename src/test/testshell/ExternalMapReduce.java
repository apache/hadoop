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

package testshell;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * will be in an external jar and used for 
 * test in TestJobShell.java.
 */
public class ExternalMapReduce extends Configured implements Tool {

  public void configure(JobConf job) {
    // do nothing
  }

  public void close()
    throws IOException {

  }

  // Executes the given shell command. Returns zero on success.
  private static int execCommandAndCatchEx(String []argv) throws IOException {
    Process p = Runtime.getRuntime().exec(argv);
    int ret = -1;
    try {
      ret = p.waitFor();
    } catch(InterruptedException ie) {
      //do nothing here.
    }
    return ret;
  }

  // Verifies that the given list of files exist on the local file system.
  // Throws on failure.
  private static void verifyFilesExist(String [] fileList) throws IOException {
    // fork off ls to see if the file exists.
    // java file.exists() does not work on symlinks on java6
    if (Shell.WINDOWS) {
      // FIXME: Checking file existence one by one until multiple file
      // scenario is supported by winutils
      for (int i = 0; i< fileList.length; ++i) {
        String[] argv = new String[3];
        argv[0] = Shell.WINUTILS;
        argv[1] = "ls";
        argv[2] = fileList[i];

        int ret = execCommandAndCatchEx(argv);
        if (ret != 0) {
          throw new IOException(fileList[i] + " does not exist");
        }
      }
    } else {
      String[] argv = new String[fileList.length + 1];
      argv[0] = "ls";
      for (int i = 0; i < fileList.length; ++i) {
        argv[i + 1] = fileList[i];
      }

      int ret = execCommandAndCatchEx(argv);
      if (ret != 0) {
        throw new IOException("files_tmp does not exist");
      }
    }
  }

  // Check if the given item exists in classpath
  private static boolean itemInClasspath(String elementName) 
    throws FileNotFoundException, IOException {
    boolean found = false;
    String classpath = System.getProperty("java.class.path");

    // Check if the element exists in the classpath
    if (classpath.indexOf(elementName) >= 0) {
      found = true;
    } else {
      // In case of Windows, classpath is embedded in a referencing jar
      if (Shell.WINDOWS) {
        String[] cpFiles = classpath.split(File.pathSeparator);
        if (cpFiles != null && cpFiles.length == 1 && 
            cpFiles[0].endsWith(".jar")) {
          // Search for the element in the jar manifest
          found = itemInJarClasspath(elementName, cpFiles[0]);
        }
      }
    }
    return found;
  }
  
  // Check if the given item exists as a classpath element in a jar file
  private static boolean itemInJarClasspath(String elementName, 
                                            String jarFileName) 
    throws FileNotFoundException, IOException{
    // Load the Jar manifest
    JarInputStream jarStream = new JarInputStream(
        new FileInputStream(jarFileName));
    Manifest jarManifest = jarStream.getManifest();
    String classpath = jarManifest.getMainAttributes().getValue(
        Attributes.Name.CLASS_PATH.toString());
    
    // Check for the element in the classpath list
    return (classpath.indexOf(elementName) >= 0);
  }
  
  public static class MapClass extends MapReduceBase 
    implements Mapper<WritableComparable, Writable,
                      WritableComparable, IntWritable> {
    public void map(WritableComparable key, Writable value,
                    OutputCollector<WritableComparable, IntWritable> output,
                    Reporter reporter)
      throws IOException {
      //check for classpath
      String classpath = System.getProperty("java.class.path");
      if (!itemInClasspath("testjob.jar")) {
        throw new IOException("failed to find in the library " + classpath);
      }
      if (!itemInClasspath("test.jar")) {
        throw new IOException("failed to find the library test.jar in" 
            + classpath);
      }

      String[] expectedFileList = { "files_tmp", "localfilelink",
          "dfsfilelink", "tarlink", "ziplink", "test.tgz", "jarlink" }; 

      verifyFilesExist(expectedFileList);

      File file = new File("./jarlink/test.txt");
      if (!file.canExecute()) {
        throw new IOException("jarlink/test.txt is not executable");
      }
    }
  }

  public static class Reduce extends MapReduceBase
    implements Reducer<WritableComparable, Writable,
                       WritableComparable, IntWritable> {
    public void reduce(WritableComparable key, Iterator<Writable> values,
                       OutputCollector<WritableComparable, IntWritable> output,
                       Reporter reporter)
      throws IOException {
     //do nothing
    }
  }
  
  public int run(String[] argv) throws IOException {
    if (argv.length < 2) {
      System.out.println("ExternalMapReduce <input> <output>");
      return -1;
    }
    Path outDir = new Path(argv[1]);
    Path input = new Path(argv[0]);
    JobConf testConf = new JobConf(getConf(), ExternalMapReduce.class);
    
    //try to load a class from libjar
    try {
      testConf.getClassByName("testjar.ClassWordCount");
    } catch (ClassNotFoundException e) {
      System.out.println("Could not find class from libjar");
      return -1;
    }
    
    
    testConf.setJobName("external job");
    FileInputFormat.setInputPaths(testConf, input);
    FileOutputFormat.setOutputPath(testConf, outDir);
    testConf.setMapperClass(MapClass.class);
    testConf.setReducerClass(Reduce.class);
    testConf.setNumReduceTasks(1);
    JobClient.runJob(testConf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
                     new ExternalMapReduce(), args);
    System.exit(res);
  }
}
