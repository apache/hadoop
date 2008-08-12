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

import java.util.concurrent.atomic.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class responsible for modeling the resource consumption of running tasks.
 * 
 * For now, we just do temp space for maps
 * 
 * There is one ResourceEstimator per JobInProgress
 *
 */
public class ResourceEstimator {

  //Log with JobInProgress
  private static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.ResourceEstimator");


  /**
   * Estimated ratio of output to input size for map tasks. 
   */
  private double mapBlowupRatio;
  private double estimateWeight;
  private JobInProgress job;

  //guess a factor of two blowup due to temp space for merge
  public static final double INITIAL_BLOWUP_GUESS = 1; 

  //initial estimate is weighted as much as this fraction of the real datapoints
  static final double INITIAL_EST_WEIGHT_PERCENT = 0.05; 


  public ResourceEstimator(JobInProgress job) {
    mapBlowupRatio = INITIAL_BLOWUP_GUESS;
    this.job = job;
    estimateWeight = INITIAL_EST_WEIGHT_PERCENT * job.desiredMaps();
  }


  /**
   * Have private access methods to abstract away synchro.
   * @return
   */
  private synchronized double getBlowupRatio() {
    return mapBlowupRatio;
  }

  private synchronized void setBlowupRatio(double b)  {
    mapBlowupRatio = b;
  }



  public void updateWithCompletedTask(TaskStatus ts, TaskInProgress tip) {

    //-1 indicates error, which we don't average in.
    if(tip.isMapTask() &&  ts.getOutputSize() != -1)  {
      double blowupOnThisTask = ts.getOutputSize() / 
        (double) tip.getMapInputSize();
      
      LOG.info("measured blowup on " + tip.getTIPId() + " was " +
          ts.getOutputSize() + "/" +tip.getMapInputSize() + " = " 
          + blowupOnThisTask);
      
      double newEstimate = blowupOnThisTask / estimateWeight + 
          ((estimateWeight - 1) / estimateWeight) * getBlowupRatio();
      estimateWeight++; 
      setBlowupRatio(newEstimate);
    }
  }

  /**
   * 
   * @return estimated length of this job's average map output
   * @throws IOException if the split's getLength() does.
   */
  public long getEstimatedMapOutputSize()  {
    double blowup =getBlowupRatio();
    long estimate =  
      (long) (job.getInputLength() * blowup / job.desiredMaps() * 2.0);
    LOG.info("estimate map will take " + estimate +
        " bytes. (blowup = 2*" + blowup + ")");
    return estimate;
  }


  //estimate that each reduce gets an equal share of total map output
  public long getEstimatedReduceInputSize() {
    return 
       getEstimatedMapOutputSize() * job.desiredMaps() / job.desiredReduces();
  }
  

}
