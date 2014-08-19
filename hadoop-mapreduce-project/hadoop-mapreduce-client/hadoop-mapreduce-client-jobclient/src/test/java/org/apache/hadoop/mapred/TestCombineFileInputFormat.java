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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestCombineFileInputFormat {
  private static final Log LOG =
    LogFactory.getLog(TestCombineFileInputFormat.class.getName());
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "/tmp")),
             "TestCombineFileInputFormat").makeQualified(localFs);

  private static void writeFile(FileSystem fs, Path name, 
                                String contents) throws IOException {
    OutputStream stm;
    stm = fs.create(name);
    stm.write(contents.getBytes());
    stm.close();
  }
  
  /**
   * Test getSplits
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testSplits() throws IOException {
    JobConf job = new JobConf(defaultConf);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "test.txt"), 
              "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n");
    FileInputFormat.setInputPaths(job, workDir);
    CombineFileInputFormat format = new CombineFileInputFormat() {
      @Override
      public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new CombineFileRecordReader(job, (CombineFileSplit)split, reporter, CombineFileRecordReader.class);
      }
    };
    final int SIZE_SPLITS = 1;
    LOG.info("Trying to getSplits with splits = " + SIZE_SPLITS);
    InputSplit[] splits = format.getSplits(job, SIZE_SPLITS);
    LOG.info("Got getSplits = " + splits.length);
    assertEquals("splits == " + SIZE_SPLITS, SIZE_SPLITS, splits.length);
  }
}
