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
package org.apache.hadoop.mapred.gridmix;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.gridmix.DebugJobProducer.MockJob;
import org.apache.hadoop.mapred.gridmix.TestHighRamJob.DummyGridmixJob;
import org.apache.hadoop.mapred.gridmix.TestResourceUsageEmulators.FakeProgressive;
import org.apache.hadoop.mapred.gridmix.emulators.resourceusage.TotalHeapUsageEmulatorPlugin;
import org.apache.hadoop.mapred.gridmix.emulators.resourceusage.TotalHeapUsageEmulatorPlugin.DefaultHeapUsageEmulator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

/**
 * Test Gridmix memory emulation.
 */
public class TestGridmixMemoryEmulation {
  /**
   * This is a dummy class that fakes heap usage.
   */
  private static class FakeHeapUsageEmulatorCore 
  extends DefaultHeapUsageEmulator {
    private int numCalls = 0;
    
    @Override
    public void load(long sizeInMB) {
      ++numCalls;
      super.load(sizeInMB);
    }
    
    // Get the total number of times load() was invoked
    int getNumCalls() {
      return numCalls;
    }
    
    // Get the total number of 1mb objects stored within
    long getHeapUsageInMB() {
      return getHeapSpaceSize();
    }
    
    @Override
    public void reset() {
      // no op to stop emulate() from resetting
    }
    
    /**
     * For re-testing purpose.
     */
    void resetFake() {
      numCalls = 0;
      super.reset();
    }
  }

  /**
   * This is a dummy class that fakes the heap usage emulator plugin.
   */
  private static class FakeHeapUsageEmulatorPlugin 
  extends TotalHeapUsageEmulatorPlugin {
    private FakeHeapUsageEmulatorCore core;
    
    public FakeHeapUsageEmulatorPlugin(FakeHeapUsageEmulatorCore core) {
      super(core);
      this.core = core;
    }
    
    @Override
    protected long getMaxHeapUsageInMB() {
      return Long.MAX_VALUE / ONE_MB;
    }
    
    @Override
    protected long getTotalHeapUsageInMB() {
      return core.getHeapUsageInMB();
    }
  }
  
  /**
   * Test {@link TotalHeapUsageEmulatorPlugin}'s core heap usage emulation 
   * engine.
   */
  @Test
  public void testHeapUsageEmulator() throws IOException {
    FakeHeapUsageEmulatorCore heapEmulator = new FakeHeapUsageEmulatorCore();
    
    long testSizeInMB = 10; // 10 mb
    long previousHeap = heapEmulator.getHeapUsageInMB();
    heapEmulator.load(testSizeInMB);
    long currentHeap = heapEmulator.getHeapUsageInMB();
    
    // check if the heap has increased by expected value
    assertEquals("Default heap emulator failed to load 10mb", 
                 previousHeap + testSizeInMB, currentHeap);
    
    // test reset
    heapEmulator.resetFake();
    assertEquals("Default heap emulator failed to reset", 
                 0, heapEmulator.getHeapUsageInMB());
  }

  /**
   * Test {@link TotalHeapUsageEmulatorPlugin}.
   */
  @Test
  public void testTotalHeapUsageEmulatorPlugin() throws Exception {
    Configuration conf = new Configuration();
    // set the dummy resource calculator for testing
    ResourceCalculatorPlugin monitor = new DummyResourceCalculatorPlugin();
    long maxHeapUsage = 1024 * TotalHeapUsageEmulatorPlugin.ONE_MB; // 1GB
    conf.setLong(DummyResourceCalculatorPlugin.MAXPMEM_TESTING_PROPERTY, 
                 maxHeapUsage);
    monitor.setConf(conf);
    
    // no buffer to be reserved
    conf.setFloat(TotalHeapUsageEmulatorPlugin.MIN_HEAP_FREE_RATIO, 0F);
    // only 1 call to be made per cycle
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_LOAD_RATIO, 1F);
    long targetHeapUsageInMB = 200; // 200mb
    
    // fake progress indicator
    FakeProgressive fakeProgress = new FakeProgressive();
    
    // fake heap usage generator
    FakeHeapUsageEmulatorCore fakeCore = new FakeHeapUsageEmulatorCore();
    
    // a heap usage emulator with fake core
    FakeHeapUsageEmulatorPlugin heapPlugin = 
      new FakeHeapUsageEmulatorPlugin(fakeCore);
    
    // test with invalid or missing resource usage value
    ResourceUsageMetrics invalidUsage = 
      TestResourceUsageEmulators.createMetrics(0);
    heapPlugin.initialize(conf, invalidUsage, null, null);
    
    // test if disabled heap emulation plugin's emulate() call is a no-operation
    // this will test if the emulation plugin is disabled or not
    int numCallsPre = fakeCore.getNumCalls();
    long heapUsagePre = fakeCore.getHeapUsageInMB();
    heapPlugin.emulate();
    int numCallsPost = fakeCore.getNumCalls();
    long heapUsagePost = fakeCore.getHeapUsageInMB();
    
    //  test if no calls are made heap usage emulator core
    assertEquals("Disabled heap usage emulation plugin works!", 
                 numCallsPre, numCallsPost);
    //  test if no calls are made heap usage emulator core
    assertEquals("Disabled heap usage emulation plugin works!", 
                 heapUsagePre, heapUsagePost);
    
    // test with get progress
    float progress = heapPlugin.getProgress();
    assertEquals("Invalid progress of disabled cumulative heap usage emulation "
                 + "plugin!", 1.0f, progress, 0f);
    
    // test with wrong/invalid configuration
    Boolean failed = null;
    invalidUsage = 
      TestResourceUsageEmulators.createMetrics(maxHeapUsage 
                                   + TotalHeapUsageEmulatorPlugin.ONE_MB);
    try {
      heapPlugin.initialize(conf, invalidUsage, monitor, null);
      failed = false;
    } catch (Exception e) {
      failed = true;
    }
    assertNotNull("Fail case failure!", failed);
    assertTrue("Expected failure!", failed); 
    
    // test with valid resource usage value
    ResourceUsageMetrics metrics = 
      TestResourceUsageEmulators.createMetrics(targetHeapUsageInMB 
                                   * TotalHeapUsageEmulatorPlugin.ONE_MB);
    
    // test with default emulation interval
    // in every interval, the emulator will add 100% of the expected usage 
    // (since gridmix.emulators.resource-usage.heap.load-ratio=1)
    // so at 10%, emulator will add 10% (difference), at 20% it will add 10% ...
    // So to emulate 200MB, it will add
    //   20mb + 20mb + 20mb + 20mb + .. = 200mb 
    testEmulationAccuracy(conf, fakeCore, monitor, metrics, heapPlugin, 200, 
                          10);
    
    // test with custom value for emulation interval of 20%
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_EMULATION_PROGRESS_INTERVAL,
                  0.2F);
    //  40mb + 40mb + 40mb + 40mb + 40mb = 200mb
    testEmulationAccuracy(conf, fakeCore, monitor, metrics, heapPlugin, 200, 5);
    
    // test with custom value of free heap ratio and load ratio = 1
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_LOAD_RATIO, 1F);
    conf.setFloat(TotalHeapUsageEmulatorPlugin.MIN_HEAP_FREE_RATIO, 0.5F);
    //  40mb + 0mb + 80mb + 0mb + 0mb = 120mb
    testEmulationAccuracy(conf, fakeCore, monitor, metrics, heapPlugin, 120, 2);
    
    // test with custom value of heap load ratio and min free heap ratio = 0
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_LOAD_RATIO, 0.5F);
    conf.setFloat(TotalHeapUsageEmulatorPlugin.MIN_HEAP_FREE_RATIO, 0F);
    // 20mb (call#1) + 20mb (call#1) + 20mb (call#2) + 20mb (call#2) +.. = 200mb
    testEmulationAccuracy(conf, fakeCore, monitor, metrics, heapPlugin, 200, 
                          10);
    
    // test with custom value of free heap ratio = 0.3 and load ratio = 0.5
    conf.setFloat(TotalHeapUsageEmulatorPlugin.MIN_HEAP_FREE_RATIO, 0.25F);
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_LOAD_RATIO, 0.5F);
    // 20mb (call#1) + 20mb (call#1) + 30mb (call#2) + 0mb (call#2) 
    // + 30mb (call#3) + 0mb (call#3) + 35mb (call#4) + 0mb (call#4)
    // + 37mb (call#5) + 0mb (call#5) = 162mb
    testEmulationAccuracy(conf, fakeCore, monitor, metrics, heapPlugin, 162, 6);
    
    // test if emulation interval boundary is respected
    fakeProgress = new FakeProgressive(); // initialize
    conf.setFloat(TotalHeapUsageEmulatorPlugin.MIN_HEAP_FREE_RATIO, 0F);
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_LOAD_RATIO, 1F);
    conf.setFloat(TotalHeapUsageEmulatorPlugin.HEAP_EMULATION_PROGRESS_INTERVAL,
                  0.25F);
    heapPlugin.initialize(conf, metrics, monitor, fakeProgress);
    fakeCore.resetFake();
    // take a snapshot after the initialization
    long initHeapUsage = fakeCore.getHeapUsageInMB();
    long initNumCallsUsage = fakeCore.getNumCalls();
    // test with 0 progress
    testEmulationBoundary(0F, fakeCore, fakeProgress, heapPlugin, initHeapUsage, 
                          initNumCallsUsage, "[no-op, 0 progress]");
    // test with 24% progress
    testEmulationBoundary(0.24F, fakeCore, fakeProgress, heapPlugin, 
                          initHeapUsage, initNumCallsUsage, 
                          "[no-op, 24% progress]");
    // test with 25% progress
    testEmulationBoundary(0.25F, fakeCore, fakeProgress, heapPlugin, 
        targetHeapUsageInMB / 4, 1, "[op, 25% progress]");
    // test with 80% progress
    testEmulationBoundary(0.80F, fakeCore, fakeProgress, heapPlugin, 
        (targetHeapUsageInMB * 4) / 5, 2, "[op, 80% progress]");
    
    // now test if the final call with 100% progress ramps up the heap usage
    testEmulationBoundary(1F, fakeCore, fakeProgress, heapPlugin, 
        targetHeapUsageInMB, 3, "[op, 100% progress]");
  }

  // test whether the heap usage emulator achieves the desired target using
  // desired calls to the underling core engine.
  private static void testEmulationAccuracy(Configuration conf, 
                        FakeHeapUsageEmulatorCore fakeCore,
                        ResourceCalculatorPlugin monitor,
                        ResourceUsageMetrics metrics,
                        TotalHeapUsageEmulatorPlugin heapPlugin,
                        long expectedTotalHeapUsageInMB,
                        long expectedTotalNumCalls)
  throws Exception {
    FakeProgressive fakeProgress = new FakeProgressive();
    fakeCore.resetFake();
    heapPlugin.initialize(conf, metrics, monitor, fakeProgress);
    int numLoops = 0;
    while (fakeProgress.getProgress() < 1) {
      ++numLoops;
      float progress = numLoops / 100.0F;
      fakeProgress.setProgress(progress);
      heapPlugin.emulate();
    }
    
    // test if the resource plugin shows the expected usage
    assertEquals("Cumulative heap usage emulator plugin failed (total usage)!", 
                 expectedTotalHeapUsageInMB, fakeCore.getHeapUsageInMB(), 1L);
    // test if the resource plugin shows the expected num calls
    assertEquals("Cumulative heap usage emulator plugin failed (num calls)!", 
                 expectedTotalNumCalls, fakeCore.getNumCalls(), 0L);
  }

  // tests if the heap usage emulation plugin emulates only at the expected
  // progress gaps
  private static void testEmulationBoundary(float progress, 
      FakeHeapUsageEmulatorCore fakeCore, FakeProgressive fakeProgress, 
      TotalHeapUsageEmulatorPlugin heapPlugin, long expectedTotalHeapUsageInMB, 
      long expectedTotalNumCalls, String info) throws Exception {
    fakeProgress.setProgress(progress);
    heapPlugin.emulate();
    // test heap usage
    assertEquals("Emulation interval test for heap usage failed " + info + "!", 
                 expectedTotalHeapUsageInMB, fakeCore.getHeapUsageInMB(), 0L);
    // test num calls
    assertEquals("Emulation interval test for heap usage failed " + info + "!", 
                 expectedTotalNumCalls, fakeCore.getNumCalls(), 0L);
  }
  
  /**
   * Test the specified task java heap options.
   */
  @SuppressWarnings("deprecation")
  private void testJavaHeapOptions(String mapOptions, 
      String reduceOptions, String taskOptions, String defaultMapOptions, 
      String defaultReduceOptions, String defaultTaskOptions, 
      String expectedMapOptions, String expectedReduceOptions, 
      String expectedTaskOptions) throws Exception {
    Configuration simulatedConf = new Configuration();
    // reset the configuration parameters
    simulatedConf.unset(MRJobConfig.MAP_JAVA_OPTS);
    simulatedConf.unset(MRJobConfig.REDUCE_JAVA_OPTS);
    simulatedConf.unset(JobConf.MAPRED_TASK_JAVA_OPTS);
    
    // set the default map task options
    if (defaultMapOptions != null) {
      simulatedConf.set(MRJobConfig.MAP_JAVA_OPTS, defaultMapOptions);
    }
    // set the default reduce task options
    if (defaultReduceOptions != null) {
      simulatedConf.set(MRJobConfig.REDUCE_JAVA_OPTS, defaultReduceOptions);
    }
    // set the default task options
    if (defaultTaskOptions != null) {
      simulatedConf.set(JobConf.MAPRED_TASK_JAVA_OPTS, defaultTaskOptions);
    }
    
    Configuration originalConf = new Configuration();
    // reset the configuration parameters
    originalConf.unset(MRJobConfig.MAP_JAVA_OPTS);
    originalConf.unset(MRJobConfig.REDUCE_JAVA_OPTS);
    originalConf.unset(JobConf.MAPRED_TASK_JAVA_OPTS);
    
    // set the map task options
    if (mapOptions != null) {
      originalConf.set(MRJobConfig.MAP_JAVA_OPTS, mapOptions);
    }
    // set the reduce task options
    if (reduceOptions != null) {
      originalConf.set(MRJobConfig.REDUCE_JAVA_OPTS, reduceOptions);
    }
    // set the task options
    if (taskOptions != null) {
      originalConf.set(JobConf.MAPRED_TASK_JAVA_OPTS, taskOptions);
    }
    
    // configure the task jvm's heap options
    GridmixJob.configureTaskJVMOptions(originalConf, simulatedConf);
    
    assertEquals("Map heap options mismatch!", expectedMapOptions, 
                 simulatedConf.get(MRJobConfig.MAP_JAVA_OPTS));
    assertEquals("Reduce heap options mismatch!", expectedReduceOptions, 
                 simulatedConf.get(MRJobConfig.REDUCE_JAVA_OPTS));
    assertEquals("Task heap options mismatch!", expectedTaskOptions, 
                 simulatedConf.get(JobConf.MAPRED_TASK_JAVA_OPTS));
  }
  
  /**
   * Test task-level java heap options configuration in {@link GridmixJob}.
   */
  @Test
  public void testJavaHeapOptions() throws Exception {
    // test missing opts
    testJavaHeapOptions(null, null, null, null, null, null, null, null, 
                        null);
    
    // test original heap opts and missing default opts
    testJavaHeapOptions("-Xms10m", "-Xms20m", "-Xms30m", null, null, null,
                        null, null, null);
    
    // test missing opts with default opts
    testJavaHeapOptions(null, null, null, "-Xms10m", "-Xms20m", "-Xms30m",
                        "-Xms10m", "-Xms20m", "-Xms30m");
    
    // test empty option
    testJavaHeapOptions("", "", "", null, null, null, null, null, null);
    
    // test empty default option and no original heap options
    testJavaHeapOptions(null, null, null, "", "", "", "", "", "");
    
    // test empty opts and default opts
    testJavaHeapOptions("", "", "", "-Xmx10m -Xms1m", "-Xmx50m -Xms2m", 
                        "-Xms2m -Xmx100m", "-Xmx10m -Xms1m", "-Xmx50m -Xms2m", 
                        "-Xms2m -Xmx100m");
    
    // test custom heap opts with no default opts
    testJavaHeapOptions("-Xmx10m", "-Xmx20m", "-Xmx30m", null, null, null,
                        "-Xmx10m", "-Xmx20m", "-Xmx30m");
    
    // test heap opts with default opts (multiple value)
    testJavaHeapOptions("-Xms5m -Xmx200m", "-Xms15m -Xmx300m", 
                        "-Xms25m -Xmx50m", "-XXabc", "-XXxyz", "-XXdef", 
                        "-XXabc -Xmx200m", "-XXxyz -Xmx300m", "-XXdef -Xmx50m");
    
    // test heap opts with default opts (duplication of -Xmx)
    testJavaHeapOptions("-Xms5m -Xmx200m", "-Xms15m -Xmx300m", 
                        "-Xms25m -Xmx50m", "-XXabc -Xmx500m", "-XXxyz -Xmx600m",
                        "-XXdef -Xmx700m", "-XXabc -Xmx200m", "-XXxyz -Xmx300m",
                        "-XXdef -Xmx50m");
    
    // test heap opts with default opts (single value)
    testJavaHeapOptions("-Xmx10m", "-Xmx20m", "-Xmx50m", "-Xms2m", 
                        "-Xms3m", "-Xms5m", "-Xms2m -Xmx10m", "-Xms3m -Xmx20m",
                        "-Xms5m -Xmx50m");
    
    // test heap opts with default opts (duplication of -Xmx)
    testJavaHeapOptions("-Xmx10m", "-Xmx20m", "-Xmx50m", "-Xmx2m", 
                        "-Xmx3m", "-Xmx5m", "-Xmx10m", "-Xmx20m", "-Xmx50m");
  }
  
  /**
   * Test disabled task heap options configuration in {@link GridmixJob}.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testJavaHeapOptionsDisabled() throws Exception {
    Configuration gridmixConf = new Configuration();
    gridmixConf.setBoolean(GridmixJob.GRIDMIX_TASK_JVM_OPTIONS_ENABLE, false);
    
    // set the default values of simulated job
    gridmixConf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1m");
    gridmixConf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2m");
    gridmixConf.set(JobConf.MAPRED_TASK_JAVA_OPTS, "-Xmx3m");
    
    // set the default map and reduce task options for original job
    final JobConf originalConf = new JobConf();
    originalConf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx10m");
    originalConf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx20m");
    originalConf.set(JobConf.MAPRED_TASK_JAVA_OPTS, "-Xmx30m");
    
    // define a mock job
    MockJob story = new MockJob(originalConf) {
      public JobConf getJobConf() {
        return originalConf;
      }
    };
    
    GridmixJob job = new DummyGridmixJob(gridmixConf, story);
    Job simulatedJob = job.getJob();
    Configuration simulatedConf = simulatedJob.getConfiguration();
    
    assertEquals("Map heap options works when disabled!", "-Xmx1m", 
                 simulatedConf.get(MRJobConfig.MAP_JAVA_OPTS));
    assertEquals("Reduce heap options works when disabled!", "-Xmx2m", 
                 simulatedConf.get(MRJobConfig.REDUCE_JAVA_OPTS));
    assertEquals("Task heap options works when disabled!", "-Xmx3m", 
                 simulatedConf.get(JobConf.MAPRED_TASK_JAVA_OPTS));
  }
}
