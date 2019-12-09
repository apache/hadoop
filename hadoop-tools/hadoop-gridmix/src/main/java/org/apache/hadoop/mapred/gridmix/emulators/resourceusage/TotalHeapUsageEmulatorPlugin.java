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
package org.apache.hadoop.mapred.gridmix.emulators.resourceusage;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.gridmix.Progressive;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

/**
 * <p>A {@link ResourceUsageEmulatorPlugin} that emulates the total heap 
 * usage by loading the JVM heap memory. Adding smaller chunks of data to the 
 * heap will essentially use up some heap space thus forcing the JVM to expand 
 * its heap and thus resulting into increase in the heap usage.</p>
 * 
 * <p>{@link TotalHeapUsageEmulatorPlugin} emulates the heap usage in steps. 
 * The frequency of emulation can be configured via 
 * {@link #HEAP_EMULATION_PROGRESS_INTERVAL}.
 * Heap usage values are matched via emulation only at specific interval 
 * boundaries.
 * </p>
 *  
 * {@link TotalHeapUsageEmulatorPlugin} is a wrapper program for managing 
 * the heap usage emulation feature. It internally uses an emulation algorithm 
 * (called as core and described using {@link HeapUsageEmulatorCore}) for 
 * performing the actual emulation. Multiple calls to this core engine should 
 * use up some amount of heap.
 */
public class TotalHeapUsageEmulatorPlugin 
implements ResourceUsageEmulatorPlugin {
  // Configuration parameters
  //  the core engine to emulate heap usage
  protected HeapUsageEmulatorCore emulatorCore;
  //  the progress bar
  private Progressive progress;
  //  decides if this plugin can emulate heap usage or not
  private boolean enabled = true;
  //  the progress boundaries/interval where emulation should be done
  private float emulationInterval;
  //  target heap usage to emulate
  private long targetHeapUsageInMB = 0;
  
  /**
   * The frequency (based on task progress) with which memory-emulation code is
   * run. If the value is set to 0.1 then the emulation will happen at 10% of 
   * the task's progress. The default value of this parameter is 
   * {@link #DEFAULT_EMULATION_PROGRESS_INTERVAL}.
   */
  public static final String HEAP_EMULATION_PROGRESS_INTERVAL = 
    "gridmix.emulators.resource-usage.heap.emulation-interval";
  
  // Default value for emulation interval
  private static final float DEFAULT_EMULATION_PROGRESS_INTERVAL = 0.1F; // 10 %

  private float prevEmulationProgress = 0F;
  
  /**
   * The minimum buffer reserved for other non-emulation activities.
   */
  public static final String MIN_HEAP_FREE_RATIO = 
    "gridmix.emulators.resource-usage.heap.min-free-ratio";
  
  private float minFreeHeapRatio;
  
  private static final float DEFAULT_MIN_FREE_HEAP_RATIO = 0.3F;
  
  /**
   * Determines the unit increase per call to the core engine's load API. This
   * is expressed as a percentage of the difference between the expected total 
   * heap usage and the current usage. 
   */
  public static final String HEAP_LOAD_RATIO = 
    "gridmix.emulators.resource-usage.heap.load-ratio";
  
  private float heapLoadRatio;
  
  private static final float DEFAULT_HEAP_LOAD_RATIO = 0.1F;
  
  public static final int ONE_MB = 1024 * 1024;
  
  /**
   * Defines the core heap usage emulation algorithm. This engine is expected
   * to perform certain memory intensive operations to consume some
   * amount of heap. {@link #load(long)} should load the current heap and 
   * increase the heap usage by the specified value. This core engine can be 
   * initialized using the {@link #initialize(ResourceCalculatorPlugin, long)} 
   * API to suit the underlying hardware better.
   */
  public interface HeapUsageEmulatorCore {
    /**
     * Performs some memory intensive operations to use up some heap.
     */
    public void load(long sizeInMB);
    
    /**
     * Initialize the core.
     */
    public void initialize(ResourceCalculatorPlugin monitor, 
                           long totalHeapUsageInMB);
    
    /**
     * Reset the resource usage
     */
    public void reset();
  }
  
  /**
   * This is the core engine to emulate the heap usage. The only responsibility 
   * of this class is to perform certain memory intensive operations to make 
   * sure that some desired value of heap is used.
   */
  public static class DefaultHeapUsageEmulator 
  implements HeapUsageEmulatorCore {
    // store the unit loads in a list
    private static final ArrayList<Object> heapSpace =
        new ArrayList<Object>();
    
    /**
     * Increase heap usage by current process by the given amount.
     * This is done by creating objects each of size 1MB.
     */
    public void load(long sizeInMB) {
      for (long i = 0; i < sizeInMB; ++i) {
        // Create another String object of size 1MB
        heapSpace.add((Object)new byte[ONE_MB]);
      }
    }

    /**
     * Gets the total number of 1mb objects stored in the emulator.
     *
     * @return total number of 1mb objects.
     */
    @VisibleForTesting
    public int getHeapSpaceSize() {
      return heapSpace.size();
    }

    /**
     * This will initialize the core and check if the core can emulate the 
     * desired target on the underlying hardware.
     */
    public void initialize(ResourceCalculatorPlugin monitor, 
                           long totalHeapUsageInMB) {
      long maxPhysicalMemoryInMB = monitor.getPhysicalMemorySize() / ONE_MB ;
      if(maxPhysicalMemoryInMB < totalHeapUsageInMB) {
        throw new RuntimeException("Total heap the can be used is " 
            + maxPhysicalMemoryInMB 
            + " bytes while the emulator is configured to emulate a total of " 
            + totalHeapUsageInMB + " bytes");
      }
    }
    
    /**
     * Clear references to all the GridMix-allocated special objects so that 
     * heap usage is reduced.
     */
    @Override
    public void reset() {
      heapSpace.clear();
    }
  }
  
  public TotalHeapUsageEmulatorPlugin() {
    this(new DefaultHeapUsageEmulator());
  }
  
  /**
   * For testing.
   */
  public TotalHeapUsageEmulatorPlugin(HeapUsageEmulatorCore core) {
    emulatorCore = core;
  }
  
  protected long getTotalHeapUsageInMB() {
    return Runtime.getRuntime().totalMemory() / ONE_MB;
  }
  
  protected long getMaxHeapUsageInMB() {
    return Runtime.getRuntime().maxMemory() / ONE_MB;
  }
  
  @Override
  public float getProgress() {
    return enabled 
           ? Math.min(1f, ((float)getTotalHeapUsageInMB())/targetHeapUsageInMB)
           : 1.0f;
  }
  
  @Override
  public void emulate() throws IOException, InterruptedException {
    if (enabled) {
      float currentProgress = progress.getProgress();
      if (prevEmulationProgress < currentProgress 
          && ((currentProgress - prevEmulationProgress) >= emulationInterval
              || currentProgress == 1)) {

        long maxHeapSizeInMB = getMaxHeapUsageInMB();
        long committedHeapSizeInMB = getTotalHeapUsageInMB();
        
        // Increase committed heap usage, if needed
        // Using a linear weighing function for computing the expected usage
        long expectedHeapUsageInMB = 
          Math.min(maxHeapSizeInMB,
                   (long) (targetHeapUsageInMB * currentProgress));
        if (expectedHeapUsageInMB < maxHeapSizeInMB
            && committedHeapSizeInMB < expectedHeapUsageInMB) {
          long bufferInMB = (long)(minFreeHeapRatio * expectedHeapUsageInMB);
          long currentDifferenceInMB = 
            expectedHeapUsageInMB - committedHeapSizeInMB;
          long currentIncrementLoadSizeInMB = 
                (long)(currentDifferenceInMB * heapLoadRatio);
          // Make sure that at least 1 MB is incremented.
          currentIncrementLoadSizeInMB = 
            Math.max(1, currentIncrementLoadSizeInMB);
          while (committedHeapSizeInMB + bufferInMB < expectedHeapUsageInMB) {
            // add blocks in order of X% of the difference, X = 10% by default
            emulatorCore.load(currentIncrementLoadSizeInMB);
            committedHeapSizeInMB = getTotalHeapUsageInMB();
          }
        }
        
        // store the emulation progress boundary
        prevEmulationProgress = currentProgress;
      }
      
      // reset the core so that the garbage is reclaimed
      emulatorCore.reset();
    }
  }

  @Override
  public void initialize(Configuration conf, ResourceUsageMetrics metrics,
                         ResourceCalculatorPlugin monitor,
                         Progressive progress) {
    this.progress = progress;
    
    // get the target heap usage
    targetHeapUsageInMB = metrics.getHeapUsage() / ONE_MB;
    if (targetHeapUsageInMB <= 0 ) {
      enabled = false;
      return;
    } else {
      // calibrate the core heap-usage utility
      emulatorCore.initialize(monitor, targetHeapUsageInMB);
      enabled = true;
    }
    
    emulationInterval = 
      conf.getFloat(HEAP_EMULATION_PROGRESS_INTERVAL, 
                    DEFAULT_EMULATION_PROGRESS_INTERVAL);
    
    minFreeHeapRatio = conf.getFloat(MIN_HEAP_FREE_RATIO, 
                                     DEFAULT_MIN_FREE_HEAP_RATIO);
    
    heapLoadRatio = conf.getFloat(HEAP_LOAD_RATIO, DEFAULT_HEAP_LOAD_RATIO);
    
    prevEmulationProgress = 0;
  }
}