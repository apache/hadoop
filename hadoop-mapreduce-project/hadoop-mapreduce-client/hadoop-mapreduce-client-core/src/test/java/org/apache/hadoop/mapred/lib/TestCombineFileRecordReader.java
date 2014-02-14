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

package org.apache.hadoop.mapred.lib;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.fs.FileUtil;

import org.junit.Test;
import org.mockito.Mockito;
import org.junit.Assert;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestCombineFileRecordReader {

  private static Path outDir = new Path(System.getProperty("test.build.data",
            "/tmp"), TestCombineFileRecordReader.class.getName());

  private static class TextRecordReaderWrapper
    extends org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper<LongWritable,Text> {
    // this constructor signature is required by CombineFileRecordReader
    public TextRecordReaderWrapper(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer idx) throws IOException {
      super(new TextInputFormat(), split, conf, reporter, idx);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInitNextRecordReader() throws IOException{
    JobConf conf = new JobConf();
    Path[] paths = new Path[3];
    long[] fileLength = new long[3];
    File[] files = new File[3];
    LongWritable key = new LongWritable(1);
    Text value = new Text();
    try {
      for(int i=0;i<3;i++){
        fileLength[i] = i;
        File dir = new File(outDir.toString());
        dir.mkdir();
        files[i] = new File(dir,"testfile"+i);
        FileWriter fileWriter = new FileWriter(files[i]);
        fileWriter.close();
        paths[i] = new Path(outDir+"/testfile"+i);
      }
      CombineFileSplit combineFileSplit = new CombineFileSplit(conf, paths, fileLength);
      Reporter reporter = Mockito.mock(Reporter.class);
      CombineFileRecordReader cfrr = new CombineFileRecordReader(conf, combineFileSplit,
        reporter,  TextRecordReaderWrapper.class);
      verify(reporter).progress();
      Assert.assertFalse(cfrr.next(key,value));
      verify(reporter, times(3)).progress();
    } finally {
      FileUtil.fullyDelete(new File(outDir.toString()));
    }

  }
}

