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

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.fs.LocalFileSystem;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * A JUnit test for testing availability and accessibility of main API that is needed
 * for sub-classes of ShuffleProviderPlugin and ShuffleConsumerPlugin.
 * The importance of this test is for preserving API with 3rd party plugins.
 */
public class TestShufflePlugin {

  static class TestShuffleConsumerPlugin implements ShuffleConsumerPlugin {
    @Override
    public void init(ShuffleConsumerPlugin.Context context) {
      // just verify that Context has kept its public interface
      context.getReduceTask();
      context.getConf();
      context.getUmbilical();
      context.getReporter();
    }
    @Override
    public boolean fetchOutputs() throws IOException{
      return true;
    }
    @Override
    public Throwable getMergeThrowable(){
      return null;
    }
    @Override
    public RawKeyValueIterator createKVIterator(JobConf job, FileSystem fs, Reporter reporter) throws IOException{
      return null;
    }
    @Override
    public void close(){
    }
  }

  @Test
  /**
   * A testing method instructing core hadoop to load an external ShuffleConsumerPlugin
   * as if it came from a 3rd party.
   */
  public void testConsumerPluginAbility() {

    try{
      // create JobConf with mapreduce.job.shuffle.consumer.plugin=TestShuffleConsumerPlugin
      JobConf jobConf = new JobConf();
      jobConf.setClass(JobContext.SHUFFLE_CONSUMER_PLUGIN_ATTR,
          TestShufflePlugin.TestShuffleConsumerPlugin.class,
          ShuffleConsumerPlugin.class);

      ShuffleConsumerPlugin shuffleConsumerPlugin = null;
      Class<? extends ShuffleConsumerPlugin> clazz =
          jobConf.getClass(JobContext.SHUFFLE_CONSUMER_PLUGIN_ATTR, null, ShuffleConsumerPlugin.class);
      assertNotNull("Unable to get " + JobContext.SHUFFLE_CONSUMER_PLUGIN_ATTR, clazz);

      // load 3rd party plugin through core's factory method
      shuffleConsumerPlugin = ReflectionUtils.newInstance(clazz, jobConf);
      assertNotNull("Unable to load " + JobContext.SHUFFLE_CONSUMER_PLUGIN_ATTR, shuffleConsumerPlugin);
    }
    catch (Exception e) {
      assertTrue("Threw exception:" + e, false);
    }
  }

  static class TestShuffleProviderPlugin implements ShuffleProviderPlugin {
    @Override
    public void initialize(TaskTracker tt) {
    }
    @Override
    public void destroy(){
    }
  }

  @Test
  /**
   * A testing method instructing core hadoop to load an external ShuffleConsumerPlugin
   * as if it came from a 3rd party.
   */
  public void testProviderPluginAbility() {

    try{
      // create JobConf with mapreduce.job.shuffle.provider.plugin=TestShuffleProviderPlugin
      JobConf jobConf = new JobConf();
      jobConf.setClass(TaskTracker.SHUFFLE_PROVIDER_PLUGIN_CLASSES,
          TestShufflePlugin.TestShuffleProviderPlugin.class,
          ShuffleProviderPlugin.class);

      ShuffleProviderPlugin shuffleProviderPlugin = null;
      Class<? extends ShuffleProviderPlugin> clazz =
          jobConf.getClass(TaskTracker.SHUFFLE_PROVIDER_PLUGIN_CLASSES, null, ShuffleProviderPlugin.class);
      assertNotNull("Unable to get " + TaskTracker.SHUFFLE_PROVIDER_PLUGIN_CLASSES, clazz);

      // load 3rd party plugin through core's factory method
      shuffleProviderPlugin = ReflectionUtils.newInstance(clazz, jobConf);
      assertNotNull("Unable to load " + TaskTracker.SHUFFLE_PROVIDER_PLUGIN_CLASSES, shuffleProviderPlugin);
    }
    catch (Exception e) {
      assertTrue("Threw exception:" + e, false);
    }
  }

  @Test
  /**
   * A method for testing availability and accessibility of API that is needed for sub-classes of ShuffleProviderPlugin
   */
  public void testProvider() {
    //mock creation
    ShuffleProviderPlugin mockShuffleProvider = mock(ShuffleProviderPlugin.class);
    TaskTracker mockTT = mock(TaskTracker.class);
    TaskController mockTaskController = mock(TaskController.class);

    mockShuffleProvider.initialize(mockTT);
    mockShuffleProvider.destroy();
    try {
      mockTT.getJobConf();
      mockTT.getJobConf(mock(JobID.class));
      mockTT.getIntermediateOutputDir("","","");
      mockTT.getTaskController();
      mockTaskController.getRunAsUser(mock(JobConf.class));
    }
    catch (Exception e){
      assertTrue("Threw exception:" + e, false);
    }
  }

  @Test
  /**
   * A method for testing availability and accessibility of API that is needed for sub-classes of ShuffleConsumerPlugin
   */
  public void testConsumer() {
    //mock creation
    ShuffleConsumerPlugin mockShuffleConsumer = mock(ShuffleConsumerPlugin.class);
    ReduceTask mockReduceTask = mock(ReduceTask.class);
    JobConf mockJobConf = mock(JobConf.class);
    TaskUmbilicalProtocol mockUmbilical = mock(TaskUmbilicalProtocol.class);
    TaskReporter mockReporter = mock(TaskReporter.class);
    LocalFileSystem mockLocalFileSystem = mock(LocalFileSystem.class);

    mockReduceTask.getTaskID();
    mockReduceTask.getJobID();
    mockReduceTask.getNumMaps();
    mockReduceTask.getPartition();
    mockReduceTask.getJobFile();
    mockReduceTask.getJvmContext();

    mockReporter.progress();

    try {
      String [] dirs = mockJobConf.getLocalDirs();
      ShuffleConsumerPlugin.Context context = new ShuffleConsumerPlugin.Context(mockReduceTask, mockUmbilical, mockJobConf, mockReporter);
      mockShuffleConsumer.init(context);
      mockShuffleConsumer.fetchOutputs();
      mockShuffleConsumer.createKVIterator(mockJobConf, mockLocalFileSystem.getRaw(), mockReporter);
      mockShuffleConsumer.close();
    }
    catch (Exception e){
      assertTrue("Threw exception:" + e, false);
    }
  }
}
