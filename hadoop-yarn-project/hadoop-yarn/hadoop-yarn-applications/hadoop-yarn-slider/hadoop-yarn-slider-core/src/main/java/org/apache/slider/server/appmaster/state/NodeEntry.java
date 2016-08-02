/*
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

package org.apache.slider.server.appmaster.state;

import com.google.common.annotations.VisibleForTesting;
import org.apache.slider.api.types.NodeEntryInformation;

/**
 * Information about the state of a role on a specific node instance.
 * No fields are synchronized; sync on the instance to work with it
 * <p>
 * The two fields `releasing` and `requested` are used to track the ongoing
 * state of YARN requests; they do not need to be persisted across stop/start
 * cycles. They may be relevant across AM restart, but without other data
 * structures in the AM, not enough to track what the AM was up to before
 * it was restarted. The strategy will be to ignore unexpected allocation
 * responses (which may come from pre-restart) requests, while treating
 * unexpected container release responses as failures.
 * <p>
 * The `active` counter is only decremented after a container release response
 * has been received.
 * <p>
 *
 */
public class NodeEntry implements Cloneable {
  
  public final int rolePriority;

  public NodeEntry(int rolePriority) {
    this.rolePriority = rolePriority;
  }

  /**
   * instance explicitly requested on this node: it's OK if an allocation
   * comes in that has not been (and when that happens, this count should 
   * not drop).
   */
  private int requested;

  /** number of starting instances */
  private int starting;

  /** incrementing counter of instances that failed to start */
  private int startFailed;

  /** incrementing counter of instances that failed */
  private int failed;

  /**
   * Counter of "failed recently" events. These are all failures
   * which have happened since it was last reset.
   */
  private int failedRecently;

  /** incrementing counter of instances that have been pre-empted. */
  private int preempted;

  /**
   * Number of live nodes. 
   */
  private int live;

  /** number of containers being released off this node */
  private int releasing;

  /** timestamp of last use */
  private long lastUsed;

  /**
   * Is the node available for assignments? That is, it is
   * not running any instances of this type, nor are there
   * any requests oustanding for it.
   * @return true if a new request could be issued without taking
   * the number of instances &gt; 1.
   */
  public synchronized boolean isAvailable() {
    return live + requested + starting - releasing <= 0;
  }

  /**
   * Are the anti-affinity constraints held. That is, zero or one
   * node running or starting
   * @return true if the constraint holds.
   */
  public synchronized boolean isAntiAffinityConstraintHeld() {
    return (live - releasing + starting) <= 1;
  }

  /**
   * return no of active instances -those that could be released as they
   * are live and not already being released
   * @return a number, possibly 0
   */
  public synchronized int getActive() {
    return (live - releasing);
  }

  /**
   * Return true if the node is not busy, and it
   * has not been used since the absolute time
   * @param absoluteTime time
   * @return true if the node could be cleaned up
   */
  public synchronized boolean notUsedSince(long absoluteTime) {
    return isAvailable() && lastUsed < absoluteTime;
  }

  public synchronized int getLive() {
    return live;
  }

  public int getStarting() {
    return starting;
  }

  /**
   * Set the live value directly -used on AM restart
   * @param v value
   */
  public synchronized void setLive(int v) {
    live = v;
  }
  
  private synchronized void incLive() {
    ++live;
  }

  private synchronized void decLive() {
    live = RoleHistoryUtils.decToFloor(live);
  }
  
  public synchronized void onStarting() {
    ++starting;
  }

  private void decStarting() {
    starting = RoleHistoryUtils.decToFloor(starting);
  }

  public synchronized void onStartCompleted() {
    decStarting();
    incLive();
  }
  
    /**
   * start failed -decrement the starting flag.
   * @return true if the node is now available
   */
  public synchronized boolean onStartFailed() {
    decStarting();
    ++startFailed;
    return containerCompleted(false, ContainerOutcome.Failed);
  }
  
  /**
   * no of requests made of this role of this node. If it goes above
   * 1 there's a problem
   */
  public synchronized  int getRequested() {
    return requested;
  }

  /**
   * request a node: 
   */
  public synchronized void request() {
    ++requested;
  }

  /**
   * A request made explicitly to this node has completed
   */
  public synchronized void requestCompleted() {
    requested = RoleHistoryUtils.decToFloor(requested);
  }

  /**
   * No of instances in release state
   */
  public synchronized int getReleasing() {
    return releasing;
  }

  /**
   * Release an instance -which is no longer marked as active
   */
  public synchronized void release() {
    releasing++;
  }

  /**
   * completion event, which can be a planned or unplanned
   * planned: dec our release count
   * unplanned: dec our live count
   * @param wasReleased true if this was planned
   * @param outcome
   * @return true if this node is now available
   */
  public synchronized boolean containerCompleted(boolean wasReleased, ContainerOutcome outcome) {
    if (wasReleased) {
      releasing = RoleHistoryUtils.decToFloor(releasing);
    } else {
      // for the node, we use the outcome of the faiure to decide
      // whether this is potentially "node-related"
      switch(outcome) {
        // general "any reason" app failure
        case Failed:
        // specific node failure
        case Node_failure:

          ++failed;
          ++failedRecently;
          break;

        case Preempted:
          preempted++;
          break;

          // failures which are node-independent
        case Failed_limits_exceeded:
        case Completed:
        default:
          break;
      }
    }
    decLive();
    return isAvailable();
  }

  /**
   * Time last used.
   */
  public synchronized long getLastUsed() {
    return lastUsed;
  }

  public synchronized void setLastUsed(long lastUsed) {
    this.lastUsed = lastUsed;
  }

  public synchronized int getStartFailed() {
    return startFailed;
  }

  public synchronized int getFailed() {
    return failed;
  }

  public synchronized int getFailedRecently() {
    return failedRecently;
  }

  @VisibleForTesting
  public synchronized void setFailedRecently(int failedRecently) {
    this.failedRecently = failedRecently;
  }

  public synchronized int getPreempted() {
    return preempted;
  }


  /**
   * Reset the failed recently count.
   */
  public synchronized void resetFailedRecently() {
    failedRecently = 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NodeEntry{");
    sb.append("priority=").append(rolePriority);
    sb.append(", requested=").append(requested);
    sb.append(", starting=").append(starting);
    sb.append(", live=").append(live);
    sb.append(", releasing=").append(releasing);
    sb.append(", lastUsed=").append(lastUsed);
    sb.append(", failedRecently=").append(failedRecently);
    sb.append(", preempted=").append(preempted);
    sb.append(", startFailed=").append(startFailed);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Produced a serialized form which can be served up as JSON
   * @return a summary of the current role status.
   */
  public synchronized NodeEntryInformation serialize() {
    NodeEntryInformation info = new NodeEntryInformation();
    info.priority = rolePriority;
    info.requested = requested;
    info.releasing = releasing;
    info.starting = starting;
    info.startFailed = startFailed;
    info.failed = failed;
    info.failedRecently = failedRecently;
    info.preempted = preempted;
    info.live = live;
    info.lastUsed = lastUsed;
    return info;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
