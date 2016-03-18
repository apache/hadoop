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
package org.apache.hadoop.conf;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.Utils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This testcase tests that a JobConf without default values submits jobs
 * properly and the JT applies its own default values to it to make the job
 * run properly.
 */
public class TestNoDefaultsJobConf extends HadoopTestCase {

  public TestNoDefaultsJobConf() throws IOException {
    super(HadoopTestCase.CLUSTER_MR, HadoopTestCase.DFS_FS, 1, 1);
  }

  @Test
  public void testNoDefaults() throws Exception {
    JobConf configuration = new JobConf();
    assertTrue(configuration.get("hadoop.tmp.dir", null) != null);

    configuration = new JobConf(false);
    assertTrue(configuration.get("hadoop.tmp.dir", null) == null);


    Path inDir = new Path("testing/jobconf/input");
    Path outDir = new Path("testing/jobconf/output");

    OutputStream os = getFileSystem().create(new Path(inDir, "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello\n");
    wr.write("hello\n");
    wr.close();

    JobConf conf = new JobConf(false);

    conf.set("fs.defaultFS", createJobConf().get("fs.defaultFS"));

    conf.setJobName("mr");

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

    FileInputFormat.setInputPaths(conf, inDir);

    FileOutputFormat.setOutputPath(conf, outDir);

    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                           getFileSystem().listStatus(outDir,
                           new Utils.OutputFileUtils.OutputFilesFilter()));
    if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      int counter = 0;
      while (line != null) {
        counter++;
        assertTrue(line.contains("hello"));
        line = reader.readLine();
      }
      reader.close();
      assertEquals(2, counter);
    }

  }

}
