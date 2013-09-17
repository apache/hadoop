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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.gridmix.Progressive;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

/**
 * <p>A {@link ResourceUsageEmulatorPlugin} that emulates the cumulative CPU 
 * usage by performing certain CPU intensive operations. Performing such CPU 
 * intensive operations essentially uses up some CPU. Every 
 * {@link ResourceUsageEmulatorPlugin} is configured with a feedback module i.e 
 * a {@link ResourceCalculatorPlugin}, to monitor the resource usage.</p>
 * 
 * <p>{@link CumulativeCpuUsageEmulatorPlugin} emulates the CPU usage in steps. 
 * The frequency of emulation can be configured via 
 * {@link #CPU_EMULATION_PROGRESS_INTERVAL}.
 * CPU usage values are matched via emulation only on the interval boundaries.
 * </p>
 *  
 * {@link CumulativeCpuUsageEmulatorPlugin} is a wrapper program for managing 
 * the CPU usage emulation feature. It internally uses an emulation algorithm 
 * (called as core and described using {@link CpuUsageEmulatorCore}) for 
 * performing the actual emulation. Multiple calls to this core engine should 
 * use up some amount of CPU.<br>
 * 
 * <p>{@link CumulativeCpuUsageEmulatorPlugin} provides a calibration feature 
 * via {@link #initialize(Configuration, ResourceUsageMetrics, 
 *                        ResourceCalculatorPlugin, Progressive)} to calibrate 
 *  the plugin and its core for the underlying hardware. As a result of 
 *  calibration, every call to the emulation engine's core should roughly use up
 *  1% of the total usage value to be emulated. This makes sure that the 
 *  underlying hardware is profiled before use and that the plugin doesn't 
 *  accidently overuse the CPU. With 1% as the unit emulation target value for 
 *  the core engine, there will be roughly 100 calls to the engine resulting in 
 *  roughly 100 calls to the feedback (resource usage monitor) module. 
 *  Excessive usage of the feedback module is discouraged as 
 *  it might result into excess CPU usage resulting into no real CPU emulation.
 *  </p>
 */
public class CumulativeCpuUsageEmulatorPlugin 
implements ResourceUsageEmulatorPlugin {
  protected CpuUsageEmulatorCore emulatorCore;
  private ResourceCalculatorPlugin monitor;
  private Progressive progress;
  private boolean enabled = true;
  private float emulationInterval; // emulation interval
  private long targetCpuUsage = 0;
  private float lastSeenProgress = 0;
  private long lastSeenCpuUsage = 0;
  
  // Configuration parameters
  public static final String CPU_EMULATION_PROGRESS_INTERVAL = 
    "gridmix.emulators.resource-usage.cpu.emulation-interval";
  private static final float DEFAULT_EMULATION_FREQUENCY = 0.1F; // 10 times

  /**
   * This is the core CPU usage emulation algorithm. This is the core engine
   * which actually performs some CPU intensive operations to consume some
   * amount of CPU. Multiple calls of {@link #compute()} should help the 
   * plugin emulate the desired level of CPU usage. This core engine can be
   * calibrated using the {@link #calibrate(ResourceCalculatorPlugin, long)}
   * API to suit the underlying hardware better. It also can be used to optimize
   * the emulation cycle.
   */
  public interface CpuUsageEmulatorCore {
    /**
     * Performs some computation to use up some CPU.
     */
    public void compute();
    
    /**
     * Allows the core to calibrate itself.
     */
    public void calibrate(ResourceCalculatorPlugin monitor, 
                          long totalCpuUsage);
  }
  
  /**
   * This is the core engine to emulate the CPU usage. The only responsibility 
   * of this class is to perform certain math intensive operations to make sure 
   * that some desired value of CPU is used.
   */
  public static class DefaultCpuUsageEmulator implements CpuUsageEmulatorCore {
    // number of times to loop for performing the basic unit computation
    private int numIterations;
    private final Random random;
    
    /**
     * This is to fool the JVM and make it think that we need the value 
     * stored in the unit computation i.e {@link #compute()}. This will prevent
     * the JVM from optimizing the code.
     */
    protected double returnValue;
    
    /**
     * Initialized the {@link DefaultCpuUsageEmulator} with default values. 
     * Note that the {@link DefaultCpuUsageEmulator} should be calibrated 
     * (see {@link #calibrate(ResourceCalculatorPlugin, long)}) when initialized
     * using this constructor.
     */
    public DefaultCpuUsageEmulator() {
      this(-1);
    }
    
    DefaultCpuUsageEmulator(int numIterations) {
      this.numIterations = numIterations;
      random = new Random();
    }
    
    /**
     * This will consume some desired level of CPU. This API will try to use up
     * 'X' percent of the target cumulative CPU usage. Currently X is set to 
     * 10%.
     */
    public void compute() {
      for (int i = 0; i < numIterations; ++i) {
        performUnitComputation();
      }
    }
    
    // Perform unit computation. The complete CPU emulation will be based on 
    // multiple invocations to this unit computation module.
    protected void performUnitComputation() {
      //TODO can this be configurable too. Users/emulators should be able to 
      // pick and choose what MATH operations to run.
      // Example :
      //           BASIC : ADD, SUB, MUL, DIV
      //           ADV   : SQRT, SIN, COSIN..
      //           COMPO : (BASIC/ADV)*
      // Also define input generator. For now we can use the random number 
      // generator. Later this can be changed to accept multiple sources.
      
      int randomData = random.nextInt();
      int randomDataCube = randomData * randomData * randomData;
      double randomDataCubeRoot = Math.cbrt(randomData);
      returnValue = Math.log(Math.tan(randomDataCubeRoot 
                                      * Math.exp(randomDataCube)) 
                             * Math.sqrt(randomData));
    }
    
    /**
     * This will calibrate the algorithm such that a single invocation of
     * {@link #compute()} emulates roughly 1% of the total desired resource 
     * usage value.
     */
    public void calibrate(ResourceCalculatorPlugin monitor, 
                          long totalCpuUsage) {
      long initTime = monitor.getCumulativeCpuTime();
      
      long defaultLoopSize = 0;
      long finalTime = initTime;
      
      //TODO Make this configurable
      while (finalTime - initTime < 100) { // 100 ms
        ++defaultLoopSize;
        performUnitComputation(); //perform unit computation
        finalTime = monitor.getCumulativeCpuTime();
      }
      
      long referenceRuntime = finalTime - initTime;
      
      // time for one loop = (final-time - init-time) / total-loops
      float timePerLoop = ((float)referenceRuntime) / defaultLoopSize;
      
      // compute the 1% of the total CPU usage desired
      //TODO Make this configurable
      long onePercent = totalCpuUsage / 100;
      
      // num-iterations for 1% = (total-desired-usage / 100) / time-for-one-loop
      numIterations = Math.max(1, (int)((float)onePercent/timePerLoop));
      
      System.out.println("Calibration done. Basic computation runtime : " 
          + timePerLoop + " milliseconds. Optimal number of iterations (1%): " 
          + numIterations);
    }
  }
  
  public CumulativeCpuUsageEmulatorPlugin() {
    this(new DefaultCpuUsageEmulator());
  }
  
  /**
   * For testing.
   */
  public CumulativeCpuUsageEmulatorPlugin(CpuUsageEmulatorCore core) {
    emulatorCore = core;
  }
  
  // Note that this weighing function uses only the current progress. In future,
  // this might depend on progress, emulation-interval and expected target.
  private float getWeightForProgressInterval(float progress) {
    // we want some kind of exponential growth function that gives less weight
    // on lower progress boundaries but high (exact emulation) near progress 
    // value of 1.
    // so here is how the current growth function looks like
    //    progress    weight
    //      0.1       0.0001
    //      0.2       0.0016
    //      0.3       0.0081
    //      0.4       0.0256
    //      0.5       0.0625
    //      0.6       0.1296
    //      0.7       0.2401
    //      0.8       0.4096
    //      0.9       0.6561
    //      1.0       1.000
    
    return progress * progress * progress * progress;
  }
  
  private synchronized long getCurrentCPUUsage() {
    return monitor.getCumulativeCpuTime();

  }
  
  @Override
  public float getProgress() {
    return enabled 
           ? Math.min(1f, ((float)getCurrentCPUUsage())/targetCpuUsage)
           : 1.0f;
  }
  
  @Override
  //TODO Multi-threading for speedup?
  public void emulate() throws IOException, InterruptedException {
    if (enabled) {
      float currentProgress = progress.getProgress();
      if (lastSeenProgress < currentProgress 
          && ((currentProgress - lastSeenProgress) >= emulationInterval
              || currentProgress == 1)) {
        // Estimate the final cpu usage
        //
        //   Consider the following
        //     Cl/Cc/Cp : Last/Current/Projected Cpu usage
        //     Pl/Pc/Pp : Last/Current/Projected progress
        //   Then
        //     (Cp-Cc)/(Pp-Pc) = (Cc-Cl)/(Pc-Pl)
        //   Solving this for Cp, we get
        //     Cp = Cc + (1-Pc)*(Cc-Cl)/Pc-Pl)
        //   Note that (Cc-Cl)/(Pc-Pl) is termed as 'rate' in the following 
        //   section
        
        long currentCpuUsage = getCurrentCPUUsage();
        // estimate the cpu usage rate
        float rate = (currentCpuUsage - lastSeenCpuUsage)
                     / (currentProgress - lastSeenProgress);
        long projectedUsage = 
          currentCpuUsage + (long)((1 - currentProgress) * rate);
        
        if (projectedUsage < targetCpuUsage) {
          // determine the correction factor between the current usage and the
          // expected usage and add some weight to the target
          long currentWeighedTarget = 
            (long)(targetCpuUsage 
                   * getWeightForProgressInterval(currentProgress));
          
          while (getCurrentCPUUsage() < currentWeighedTarget) {
            emulatorCore.compute();
            // sleep for 100ms
            try {
              Thread.sleep(100);
            } catch (InterruptedException ie) {
              String message = 
                "CumulativeCpuUsageEmulatorPlugin got interrupted. Exiting.";
              throw new RuntimeException(message);
            }
          }
        }
        
        // set the last seen progress
        lastSeenProgress = progress.getProgress();
        // set the last seen usage
        lastSeenCpuUsage = getCurrentCPUUsage();
      }
    }
  }

  @Override
  public void initialize(Configuration conf, ResourceUsageMetrics metrics,
                         ResourceCalculatorPlugin monitor,
                         Progressive progress) {
    this.monitor = monitor;
    this.progress = progress;
    
    // get the target CPU usage
    targetCpuUsage = metrics.getCumulativeCpuUsage();
    if (targetCpuUsage <= 0 ) {
      enabled = false;
      return;
    } else {
      enabled = true;
    }
    
    emulationInterval =  conf.getFloat(CPU_EMULATION_PROGRESS_INTERVAL, 
                                       DEFAULT_EMULATION_FREQUENCY);
    
    // calibrate the core cpu-usage utility
    emulatorCore.calibrate(monitor, targetCpuUsage);
    
    // initialize the states
    lastSeenProgress = 0;
    lastSeenCpuUsage = 0;
  }
}
