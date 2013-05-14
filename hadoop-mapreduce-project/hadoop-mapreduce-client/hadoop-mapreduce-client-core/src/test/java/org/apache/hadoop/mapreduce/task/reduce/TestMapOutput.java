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
package org.apache.hadoop.mapreduce.task.reduce;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMapOutput {

  private static final FileContext lfs = getLfs();
  private static final FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Path base =
    lfs.makeQualified(new Path("target", TestMapOutput.class.getName()));

  @BeforeClass
  public static void createBase() throws IOException {
    lfs.mkdir(base,  null, true);
  }

  @AfterClass
  public static void removeBase() throws IOException {
    lfs.delete(base, true);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDiskOutputBufferLeak() throws IOException {
    MapOutputFile mof = mock(MapOutputFile.class);
    when(mof.getInputFileForWrite(any(TaskID.class), anyLong())).thenReturn(
        new Path(base, "mapoutputfile"));
    TaskAttemptID mtid = new TaskAttemptID("0", 1, TaskType.MAP, 1, 1);
    MergeManager<Text,Text> merger = mock(MergeManager.class);
    MapOutput<Text,Text> odmo = new MapOutput<Text,Text>(
        mtid, merger, 0, new JobConf(), null, 0, true, mof);
    Assert.assertNotNull(odmo.getDisk());
    odmo.commit();
    Assert.assertNull(odmo.getDisk());

    odmo = new MapOutput<Text,Text>(
        mtid, merger, 0, new JobConf(), null, 0, true, mof);
    Assert.assertNotNull(odmo.getDisk());
    odmo.abort();
    Assert.assertNull(odmo.getDisk());
  }
}
