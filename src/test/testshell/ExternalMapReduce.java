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
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * will be in an external jar and used for 
 * test in TestJobShell.java.
 */
public class ExternalMapReduce
  implements Mapper<WritableComparable, Writable,
                    WritableComparable, IntWritable>,
             Reducer<WritableComparable, Writable,
                     WritableComparable, IntWritable> {

  public void configure(JobConf job) {
    // do nothing
  }

  public void close()
    throws IOException {

  }

  public void map(WritableComparable key, Writable value,
                  OutputCollector<WritableComparable, IntWritable> output,
                  Reporter reporter)
    throws IOException {
    //check for classpath
    String classpath = System.getProperty("java.class.path");
    if (classpath.indexOf("testjob.jar") == -1) {
      throw new IOException("failed to find in the library " + classpath);
    }
    File f = new File("files_tmp");
    //check for files 
    if (!f.exists()) {
      throw new IOException("file file_tmpfile not found");
    }
  }

  public void reduce(WritableComparable key, Iterator<Writable> values,
                     OutputCollector<WritableComparable, IntWritable> output,
                     Reporter reporter)
    throws IOException {
   //do nothing
  }
  
  public static int main(String[] argv) throws IOException {
    if (argv.length < 2) {
      System.out.println("ExternalMapReduce <input> <output>");
      return -1;
    }
    Path outDir = new Path(argv[1]);
    Path input = new Path(argv[0]);
    Configuration commandConf = JobClient.getCommandLineConfig();
    JobConf testConf = new JobConf(commandConf, ExternalMapReduce.class);
    testConf.setJobName("external job");
    testConf.setInputPath(input);
    testConf.setOutputPath(outDir);
    testConf.setMapperClass(ExternalMapReduce.class);
    testConf.setReducerClass(ExternalMapReduce.class);
    testConf.setNumReduceTasks(1);
    JobClient.runJob(testConf);
    return 0;
  }
}
