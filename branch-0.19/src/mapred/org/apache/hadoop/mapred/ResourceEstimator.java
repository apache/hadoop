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
class ResourceEstimator {

  //Log with JobInProgress
  private static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.ResourceEstimator");


  /**
   * Estimated ratio of output to (input size+1) for map tasks. 
   */
  private double mapBlowupRatio;
  
  /**
   * How much relative weight to put on the current estimate.
   * Each completed map has unit weight.
   */
  private double estimateWeight;
  final private JobInProgress job;
  final private int threshholdToUse;

  public ResourceEstimator(JobInProgress job) {
    this.job = job;
    threshholdToUse = job.desiredMaps()/ 10;
    mapBlowupRatio = 0;
    estimateWeight = 1;
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

  void updateWithCompletedTask(TaskStatus ts, TaskInProgress tip) {
    
    //-1 indicates error, which we don't average in.
    if(tip.isMapTask() &&  ts.getOutputSize() != -1)  {
      double blowupOnThisTask = ts.getOutputSize() / 
        ((double) tip.getMapInputSize() + 1);
      
      LOG.info("measured blowup on " + tip.getTIPId() + " was " +
          ts.getOutputSize() + "/" +(tip.getMapInputSize()+1) + " = " 
          + blowupOnThisTask);
      
      double newEstimate;
      synchronized(this) {
        newEstimate = blowupOnThisTask / estimateWeight + 
            ((estimateWeight - 1) / estimateWeight) * getBlowupRatio();
        estimateWeight++; 
      }
      setBlowupRatio(newEstimate);
      
      LOG.info("new estimate is blowup = " + newEstimate);
    }
  }

  /**
   * @return estimated length of this job's total map output
   */
  protected long getEstimatedTotalMapOutputSize()  {
    double estWeight;
    synchronized(this) {
      estWeight = this.estimateWeight;
    }
    
    if(estWeight < threshholdToUse) {
      return 0;
    } else {
      double blowup =getBlowupRatio();
      long inputSize = job.getInputLength() + job.desiredMaps(); 
      //add desiredMaps() so that randomwriter case doesn't blow up
      long estimate = Math.round(inputSize * blowup * 2.0);
  
      LOG.debug("estimate total map output will be " + estimate +
          " bytes. (blowup = 2*" + blowup + ")");
      return estimate;
    }
  }
  
  /**
   * @return estimated length of this job's average map output
   */
  long getEstimatedMapOutputSize() {
    long estimate = getEstimatedTotalMapOutputSize()  / job.desiredMaps();
    return estimate;
  }

  /**
   * 
   * @return estimated length of this job's average reduce input
   */
  long getEstimatedReduceInputSize() {
    if(job.desiredReduces() == 0) {//no reduce output, so no size
      return 0;
    } else {
      return getEstimatedTotalMapOutputSize() / job.desiredReduces();
      //estimate that each reduce gets an equal share of total map output
    }
  }
  

}
