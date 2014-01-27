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

package org.apache.hadoop.mapreduce.split;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

public class TestJobSplitWriter {

  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")), "TestJobSplitWriter");

  @Test
  public void testMaxBlockLocationsNewSplits() throws Exception {
    TEST_DIR.mkdirs();
    try {
      Configuration conf = new Configuration();
      conf.setInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY, 4);
      Path submitDir = new Path(TEST_DIR.getAbsolutePath());
      FileSystem fs = FileSystem.getLocal(conf);
      FileSplit split = new FileSplit(new Path("/some/path"), 0, 1,
          new String[] { "loc1", "loc2", "loc3", "loc4", "loc5" });
      JobSplitWriter.createSplitFiles(submitDir, conf, fs,
          new FileSplit[] { split });
      JobSplit.TaskSplitMetaInfo[] infos =
          SplitMetaInfoReader.readSplitMetaInfo(new JobID(), fs, conf,
              submitDir);
      assertEquals("unexpected number of splits", 1, infos.length);
      assertEquals("unexpected number of split locations",
          4, infos[0].getLocations().length);
    } finally {
      FileUtil.fullyDelete(TEST_DIR);
    }
  }

  @Test
  public void testMaxBlockLocationsOldSplits() throws Exception {
    TEST_DIR.mkdirs();
    try {
      Configuration conf = new Configuration();
      conf.setInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY, 4);
      Path submitDir = new Path(TEST_DIR.getAbsolutePath());
      FileSystem fs = FileSystem.getLocal(conf);
      org.apache.hadoop.mapred.FileSplit split =
          new org.apache.hadoop.mapred.FileSplit(new Path("/some/path"), 0, 1,
              new String[] { "loc1", "loc2", "loc3", "loc4", "loc5" });
      JobSplitWriter.createSplitFiles(submitDir, conf, fs,
          new org.apache.hadoop.mapred.InputSplit[] { split });
      JobSplit.TaskSplitMetaInfo[] infos =
          SplitMetaInfoReader.readSplitMetaInfo(new JobID(), fs, conf,
              submitDir);
      assertEquals("unexpected number of splits", 1, infos.length);
      assertEquals("unexpected number of split locations",
          4, infos[0].getLocations().length);
    } finally {
      FileUtil.fullyDelete(TEST_DIR);
    }
  }
}
