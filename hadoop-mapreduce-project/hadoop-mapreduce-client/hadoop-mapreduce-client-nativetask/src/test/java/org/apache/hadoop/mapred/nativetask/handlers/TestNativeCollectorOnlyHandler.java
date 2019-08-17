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
package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.ICombineHandler;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.testutil.TestConstants;
import org.apache.hadoop.mapred.nativetask.util.OutputUtil;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

@SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
public class TestNativeCollectorOnlyHandler {

  private NativeCollectorOnlyHandler handler;
  private INativeHandler nativeHandler;
  private BufferPusher pusher;
  private ICombineHandler combiner;
  private TaskContext taskContext;
  private static final String LOCAL_DIR = TestConstants.NATIVETASK_TEST_DIR + "/local";

  @Before
  public void setUp() throws IOException {
    this.nativeHandler = Mockito.mock(INativeHandler.class);
    this.pusher = Mockito.mock(BufferPusher.class);
    this.combiner = Mockito.mock(ICombineHandler.class);
    JobConf jobConf = new JobConf();
    jobConf.set(OutputUtil.NATIVE_TASK_OUTPUT_MANAGER,
        "org.apache.hadoop.mapred.nativetask.util.LocalJobOutputFiles");
    jobConf.set("mapred.local.dir", LOCAL_DIR);
    this.taskContext = new TaskContext(jobConf,
        BytesWritable.class, BytesWritable.class,
        BytesWritable.class,
        BytesWritable.class,
        null,
        null);

    Mockito.when(nativeHandler.getInputBuffer()).thenReturn(
      new InputBuffer(BufferType.HEAP_BUFFER, 100));
  }

  @After
  public void tearDown() throws IOException {
    FileSystem.getLocal(new Configuration()).delete(new Path(LOCAL_DIR));
  }

  @Test
  public void testCollect() throws IOException {
    this.handler = new NativeCollectorOnlyHandler(taskContext, nativeHandler, pusher, combiner);
    handler.collect(new BytesWritable(), new BytesWritable(), 100);
    handler.close();
    handler.close();

    Mockito.verify(pusher, Mockito.times(1)).collect(any(BytesWritable.class),
        any(BytesWritable.class), anyInt());

    Mockito.verify(pusher, Mockito.times(1)).close();
    Mockito.verify(combiner, Mockito.times(1)).close();
    Mockito.verify(nativeHandler, Mockito.times(1)).close();
  }

  @Test
  public void testGetCombiner() throws IOException {
    this.handler = new NativeCollectorOnlyHandler(taskContext, nativeHandler, pusher, combiner);
    Mockito.when(combiner.getId()).thenReturn(100L);
    final ReadWriteBuffer result = handler.onCall(
      NativeCollectorOnlyHandler.GET_COMBINE_HANDLER, null);
    Assert.assertEquals(100L, result.readLong());
  }

  @Test
  public void testOnCall() throws IOException {
    this.handler = new NativeCollectorOnlyHandler(taskContext, nativeHandler, pusher, combiner);
    boolean thrown = false;
    try {
      handler.onCall(new Command(-1), null);
    } catch(final IOException e) {
      thrown = true;
    }
    Assert.assertTrue("exception thrown", thrown);

    final String expectedOutputPath = StringUtils.join(File.separator,
        new String[] {LOCAL_DIR, "output", "file.out"});
    final String expectedOutputIndexPath = StringUtils.join(File.separator,
        new String[] {LOCAL_DIR, "output", "file.out.index"});
    final String expectedSpillPath = StringUtils.join(File.separator,
        new String[] {LOCAL_DIR, "output", "spill0.out"});

    final String outputPath = handler.onCall(
      NativeCollectorOnlyHandler.GET_OUTPUT_PATH, null).readString();
    Assert.assertEquals(expectedOutputPath, outputPath);

    final String outputIndexPath = handler.onCall(
      NativeCollectorOnlyHandler.GET_OUTPUT_INDEX_PATH, null).readString();
    Assert.assertEquals(expectedOutputIndexPath, outputIndexPath);

    final String spillPath = handler.onCall(
      NativeCollectorOnlyHandler.GET_SPILL_PATH, null).readString();
    Assert.assertEquals(expectedSpillPath, spillPath);
  }
}
