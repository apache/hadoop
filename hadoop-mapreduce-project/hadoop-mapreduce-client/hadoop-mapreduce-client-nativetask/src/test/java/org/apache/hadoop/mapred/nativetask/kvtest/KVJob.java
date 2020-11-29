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
package org.apache.hadoop.mapred.nativetask.kvtest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.nativetask.testutil.BytesFactory;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVJob {
  public static final String INPUTPATH = "nativetask.kvtest.inputfile.path";
  public static final String OUTPUTPATH = "nativetask.kvtest.outputfile.path";
  private static final Logger LOG = LoggerFactory.getLogger(KVJob.class);
  Job job = null;

  public static class ValueMapper<KTYPE, VTYPE> extends Mapper<KTYPE, VTYPE, KTYPE, VTYPE> {
    @Override
    public void map(KTYPE key, VTYPE value, Context context)
      throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class KVMReducer<KTYPE, VTYPE> extends Reducer<KTYPE, VTYPE, KTYPE, VTYPE> {
    public void reduce(KTYPE key, VTYPE value, Context context)
      throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class KVReducer<KTYPE, VTYPE> extends Reducer<KTYPE, VTYPE, KTYPE, VTYPE> {

    @Override
    @SuppressWarnings({"unchecked"})
    public void reduce(KTYPE key, Iterable<VTYPE> values, Context context)
      throws IOException, InterruptedException {
      long resultlong = 0;// 8 bytes match BytesFactory.fromBytes function
      final CRC32 crc32 = new CRC32();
      for (final VTYPE val : values) {
        crc32.reset();
        crc32.update(BytesFactory.toBytes(val));
        resultlong += crc32.getValue();
      }
      final VTYPE V = null;
      context.write(key, (VTYPE) BytesFactory.newObject(Longs.toByteArray(resultlong),
                                                        V.getClass().getName()));
    }
  }

  public KVJob(String jobname, Configuration conf,
               Class<?> keyclass, Class<?> valueclass,
               String inputpath, String outputpath) throws Exception {
    job = Job.getInstance(conf, jobname);
    job.setJarByClass(KVJob.class);
    job.setMapperClass(KVJob.ValueMapper.class);
    job.setOutputKeyClass(keyclass);
    job.setMapOutputValueClass(valueclass);
    
    if (conf.get(TestConstants.NATIVETASK_KVTEST_CREATEFILE).equals("true")) {
      final FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path(inputpath), true);
      fs.close();
      final TestInputFile testfile = new TestInputFile(Integer.valueOf(conf.get(
          TestConstants.FILESIZE_KEY, "1000")),
          keyclass.getName(), valueclass.getName(), conf);
      StopWatch sw = new StopWatch().start();
      testfile.createSequenceTestFile(inputpath);
      LOG.info("Created test file " + inputpath + " in "
          + sw.now(TimeUnit.MILLISECONDS) + "ms");
    }
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(inputpath));
    FileOutputFormat.setOutputPath(job, new Path(outputpath));
  }

  public boolean runJob() throws Exception {
    return job.waitForCompletion(true);
  }
}
