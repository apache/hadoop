/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.resourcemanager.TransitionToActiveStandbyRunner.TransitionToActiveStandbyResult;

import java.util.concurrent.Callable;

/**
 * The TransitionToActiveStandbyRunner is an abstract class applied in the HA
 * for performing ToActive and ToStandby tasks.
 */
public abstract class TransitionToActiveStandbyRunner implements
    Callable<TransitionToActiveStandbyResult> {

  private static final Logger LOG = LoggerFactory.getLogger(
      TransitionToActiveStandbyRunner.class);

  //ClusterTimeStamp is epoch tag
  private final long clusterTimeStamp;

  public TransitionToActiveStandbyRunner(long clusterTimeStamp) {
    this.clusterTimeStamp = clusterTimeStamp;
  }

  @Override
  public TransitionToActiveStandbyResult call() {

    Long startTime = System.currentTimeMillis();

    try {
      LOG.info("The {} task starts", this.getClass().getSimpleName());
      doTransaction();
      Long entTime = System.currentTimeMillis() - startTime;
      LOG.info("The {} task finished, took {}ms", this.getClass().getSimpleName(), entTime);
    } catch (Exception ex) {
      LOG.warn("The {} task failed.", this.getClass().getSimpleName(), ex);
      return new TransitionToActiveStandbyResult(false, ex);
    }

    return new TransitionToActiveStandbyResult(true);
  }

  public abstract void doTransaction() throws Exception;

  public static class TransitionToActiveStandbyResult {
    private boolean isSuccess;
    private Exception ex;

    public TransitionToActiveStandbyResult(boolean isSuccess, Exception ex) {
      this.isSuccess = isSuccess;
      this.ex = ex;
    }

    public TransitionToActiveStandbyResult(boolean isSuccess) {
      this.isSuccess = isSuccess;
    }

    public boolean isSuccess() {
      return isSuccess;
    }

    public Exception getException() {
      return ex;
    }
  }

  public long getClusterTimeStamp() {
    return clusterTimeStamp;
  }
}
