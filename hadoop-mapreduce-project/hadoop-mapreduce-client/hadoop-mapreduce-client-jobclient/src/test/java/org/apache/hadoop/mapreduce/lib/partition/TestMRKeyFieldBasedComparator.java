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

package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestMRKeyFieldBasedComparator extends HadoopTestCase {
  Configuration conf;
  
  String line1 = "123 -123 005120 123.9 0.01 0.18 010 10.0 4444.1 011 011 234";
  String line2 = "134 -12 005100 123.10 -1.01 0.19 02 10.1 4444";

  public TestMRKeyFieldBasedComparator() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
    conf = createJobConf();
    conf.set(MRJobConfig.MAP_OUTPUT_KEY_FIELD_SEPERATOR, " ");
  }

  private void testComparator(String keySpec, int expect)
      throws Exception {
    String root = System.getProperty("test.build.data", "/tmp");
    Path inDir = new Path(root, "test_cmp/in");
    Path outDir = new Path(root, "test_cmp/out");
    
    conf.set("mapreduce.partition.keycomparator.options", keySpec);
    conf.set("mapreduce.partition.keypartitioner.options", "-k1.1,1.1");
    conf.set(MRJobConfig.MAP_OUTPUT_KEY_FIELD_SEPERATOR, " ");

    Job job = MapReduceTestUtil.createJob(conf, inDir, outDir, 1, 1,
                line1 +"\n" + line2 + "\n"); 
    job.setMapperClass(InverseMapper.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setSortComparatorClass(KeyFieldBasedComparator.class);
    job.setPartitionerClass(KeyFieldBasedPartitioner.class);

    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());

    // validate output
    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(outDir,
        new Utils.OutputFileUtils.OutputFilesFilter()));
    if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      //make sure we get what we expect as the first line, and also
      //that we have two lines (both the lines must end up in the same
      //reducer since the partitioner takes the same key spec for all
      //lines
      if (expect == 1) {
        assertTrue(line.startsWith(line1));
      } else if (expect == 2) {
        assertTrue(line.startsWith(line2));
      }
      line = reader.readLine();
      if (expect == 1) {
        assertTrue(line.startsWith(line2));
      } else if (expect == 2) {
        assertTrue(line.startsWith(line1));
      }
      reader.close();
    }
  }

  @Test
  public void testBasicUnixComparator() throws Exception {
    testComparator("-k1,1n", 1);
    testComparator("-k2,2n", 1);
    testComparator("-k2.2,2n", 2);
    testComparator("-k3.4,3n", 2);
    testComparator("-k3.2,3.3n -k4,4n", 2);
    testComparator("-k3.2,3.3n -k4,4nr", 1);
    testComparator("-k2.4,2.4n", 2);
    testComparator("-k7,7", 1);
    testComparator("-k7,7n", 2);
    testComparator("-k8,8n", 1);
    testComparator("-k9,9", 2);
    testComparator("-k11,11",2);
    testComparator("-k10,10",2);
    
    testWithoutMRJob("-k9,9", 1);
    
    testWithoutMRJob("-k9n", 1);
  }
  
  byte[] line1_bytes = line1.getBytes();
  byte[] line2_bytes = line2.getBytes();

  private void testWithoutMRJob(String keySpec, int expect) throws Exception {
    KeyFieldBasedComparator<Void, Void> keyFieldCmp = 
      new KeyFieldBasedComparator<Void, Void>();
    conf.set("mapreduce.partition.keycomparator.options", keySpec);
    keyFieldCmp.setConf(conf);
    int result = keyFieldCmp.compare(line1_bytes, 0, line1_bytes.length,
        line2_bytes, 0, line2_bytes.length);
    if ((expect >= 0 && result < 0) || (expect < 0 && result >= 0))
      fail();
  }
}
