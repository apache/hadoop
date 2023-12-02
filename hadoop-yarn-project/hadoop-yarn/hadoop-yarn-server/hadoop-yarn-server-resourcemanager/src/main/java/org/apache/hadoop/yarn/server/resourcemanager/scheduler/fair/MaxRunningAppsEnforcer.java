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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;

/**
 * Handles tracking and enforcement for user and queue maxRunningApps
 * constraints
 */
public class MaxRunningAppsEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(
      MaxRunningAppsEnforcer.class);
  
  private final FairScheduler scheduler;

  // Tracks the number of running applications by user.
  private final Map<String, Integer> usersNumRunnableApps;
  @VisibleForTesting
  final ListMultimap<String, FSAppAttempt> usersNonRunnableApps;

  public MaxRunningAppsEnforcer(FairScheduler scheduler) {
    this.scheduler = scheduler;
    this.usersNumRunnableApps = new HashMap<String, Integer>();
    this.usersNonRunnableApps = ArrayListMultimap.create();
  }

  /**
   * Checks whether making the application runnable would exceed any
   * maxRunningApps limits.
   *
   * @param queue the current queue
   * @param attempt the app attempt being checked
   * @return true if the application is runnable; false otherwise
   */
  public boolean canAppBeRunnable(FSQueue queue, FSAppAttempt attempt) {
    boolean ret = true;
    if (exceedUserMaxApps(attempt.getUser())) {
      attempt.updateAMContainerDiagnostics(AMState.INACTIVATED,
          "The user \"" + attempt.getUser() + "\" has reached the maximum limit"
              + " of runnable applications.");
      ret = false;
    } else if (exceedQueueMaxRunningApps(queue)) {
      attempt.updateAMContainerDiagnostics(AMState.INACTIVATED,
          "The queue \"" + queue.getName() + "\" has reached the maximum limit"
              + " of runnable applications.");
      ret = false;
    }

    return ret;
  }

  /**
   * Checks whether the number of user runnable apps exceeds the limitation.
   *
   * @param user the user name
   * @return true if the number hits the limit; false otherwise
   */
  public boolean exceedUserMaxApps(String user) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    Integer userNumRunnable = usersNumRunnableApps.get(user);
    if (userNumRunnable == null) {
      userNumRunnable = 0;
    }
    if (userNumRunnable >= allocConf.getUserMaxApps(user)) {
      return true;
    }

    return false;
  }

  /**
   * Recursively checks whether the number of queue runnable apps exceeds the
   * limitation.
   *
   * @param queue the current queue
   * @return true if the number hits the limit; false otherwise
   */
  public boolean exceedQueueMaxRunningApps(FSQueue queue) {
    // Check queue and all parent queues
    while (queue != null) {
      if (queue.getNumRunnableApps() >= queue.getMaxRunningApps()) {
        return true;
      }
      queue = queue.getParent();
    }

    return false;
  }

  /**
   * Tracks the given new runnable app for purposes of maintaining max running
   * app limits.
   *
   * @param app FSAppAttempt, the given application.
   */
  public void trackRunnableApp(FSAppAttempt app) {
    String user = app.getUser();
    FSLeafQueue queue = app.getQueue();
    // Increment running counts for all parent queues
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.incrementRunnableApps();
      parent = parent.getParent();
    }

    Integer userNumRunnable = usersNumRunnableApps.get(user);
    usersNumRunnableApps.put(user, (userNumRunnable == null ? 0
        : userNumRunnable) + 1);
  }

  /**
   * Tracks the given new non runnable app so that it can be made runnable when
   * it would not violate max running app limits.
   *
   * @param app FSAppAttempt, the given application.
   */
  public void trackNonRunnableApp(FSAppAttempt app) {
    String user = app.getUser();
    usersNonRunnableApps.put(user, app);
  }

  /**
   * This is called after reloading the allocation configuration when the
   * scheduler is reinitialized
   *
   * Checks to see whether any non-runnable applications become runnable
   * now that the max running apps of given queue has been changed
   *
   * Runs in O(n) where n is the number of apps that are non-runnable and in
   * the queues that went from having no slack to having slack.
   */
  public void updateRunnabilityOnReload() {
    FSParentQueue rootQueue = scheduler.getQueueManager().getRootQueue();
    List<List<FSAppAttempt>> appsNowMaybeRunnable =
        new ArrayList<List<FSAppAttempt>>();

    gatherPossiblyRunnableAppLists(rootQueue, appsNowMaybeRunnable);

    updateAppsRunnability(appsNowMaybeRunnable, Integer.MAX_VALUE);
  }

  /**
   * Checks to see whether any other applications runnable now that the given
   * application has been removed from the given queue.  And makes them so.
   * 
   * Runs in O(n log(n)) where n is the number of queues that are under the
   * highest queue that went from having no slack to having slack.
   *
   * @param queue FSLeafQueue, the given queue.
   * @param app FSAppAttempt, the given application.
   */
  public void updateRunnabilityOnAppRemoval(FSAppAttempt app, FSLeafQueue queue) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    // childqueueX might have no pending apps itself, but if a queue higher up
    // in the hierarchy parentqueueY has a maxRunningApps set, an app completion
    // in childqueueX could allow an app in some other distant child of
    // parentqueueY to become runnable.
    // An app removal will only possibly allow another app to become runnable if
    // the queue was already at its max before the removal.
    // Thus we find the ancestor queue highest in the tree for which the app
    // that was at its maxRunningApps before the removal.
    FSQueue highestQueueWithAppsNowRunnable = (queue.getNumRunnableApps() ==
        queue.getMaxRunningApps() - 1) ? queue : null;
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      if (parent.getNumRunnableApps() == parent.getMaxRunningApps() - 1) {
        highestQueueWithAppsNowRunnable = parent;
      }
      parent = parent.getParent();
    }

    List<List<FSAppAttempt>> appsNowMaybeRunnable =
        new ArrayList<List<FSAppAttempt>>();

    // Compile lists of apps which may now be runnable
    // We gather lists instead of building a set of all non-runnable apps so
    // that this whole operation can be O(number of queues) instead of
    // O(number of apps)
    if (highestQueueWithAppsNowRunnable != null) {
      gatherPossiblyRunnableAppLists(highestQueueWithAppsNowRunnable,
          appsNowMaybeRunnable);
    }
    String user = app.getUser();
    Integer userNumRunning = usersNumRunnableApps.get(user);
    if (userNumRunning == null) {
      userNumRunning = 0;
    }
    if (userNumRunning == allocConf.getUserMaxApps(user) - 1) {
      List<FSAppAttempt> userWaitingApps = usersNonRunnableApps.get(user);
      if (userWaitingApps != null) {
        appsNowMaybeRunnable.add(userWaitingApps);
      }
    }

    updateAppsRunnability(appsNowMaybeRunnable,
        appsNowMaybeRunnable.size());
  }

  /**
   * Checks to see whether applications are runnable now by iterating
   * through each one of them and check if the queue and user have slack
   *
   * if we know how many apps can be runnable, there is no need to iterate
   * through all apps, maxRunnableApps is used to break out of the iteration
   */
  private void updateAppsRunnability(List<List<FSAppAttempt>>
      appsNowMaybeRunnable, int maxRunnableApps) {
    // Scan through and check whether this means that any apps are now runnable
    Iterator<FSAppAttempt> iter = new MultiListStartTimeIterator(
        appsNowMaybeRunnable);
    FSAppAttempt prev = null;
    List<FSAppAttempt> noLongerPendingApps = new ArrayList<FSAppAttempt>();
    while (iter.hasNext()) {
      FSAppAttempt next = iter.next();
      if (next == prev) {
        continue;
      }

      if (canAppBeRunnable(next.getQueue(), next)) {
        trackRunnableApp(next);
        FSAppAttempt appSched = next;
        next.getQueue().addApp(appSched, true);
        noLongerPendingApps.add(appSched);

        if (noLongerPendingApps.size() >= maxRunnableApps) {
          break;
        }
      }

      prev = next;
    }
    
    // We remove the apps from their pending lists afterwards so that we don't
    // pull them out from under the iterator.  If they are not in these lists
    // in the first place, there is a bug.
    for (FSAppAttempt appSched : noLongerPendingApps) {
      if (!appSched.getQueue().removeNonRunnableApp(appSched)) {
        LOG.error("Can't make app runnable that does not already exist in queue"
            + " as non-runnable: " + appSched + ". This should never happen.");
      }
      
      if (!usersNonRunnableApps.remove(appSched.getUser(), appSched)) {
        LOG.error("Waiting app " + appSched + " expected to be in "
            + "usersNonRunnableApps, but was not. This should never happen.");
      }
    }
  }
  /**
   * Updates the relevant tracking variables after a runnable app with the given
   * queue and user has been removed.
   *
   * @param app FSAppAttempt.
   */
  public void untrackRunnableApp(FSAppAttempt app) {
    // Update usersRunnableApps
    String user = app.getUser();
    int newUserNumRunning = usersNumRunnableApps.get(user) - 1;
    if (newUserNumRunning == 0) {
      usersNumRunnableApps.remove(user);
    } else {
      usersNumRunnableApps.put(user, newUserNumRunning);
    }
    
    // Update runnable app bookkeeping for queues
    FSLeafQueue queue = app.getQueue();
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.decrementRunnableApps();
      parent = parent.getParent();
    }
  }
  
  /**
   * Stops tracking the given non-runnable app.
   *
   * @param app FSAppAttempt.
   */
  public void untrackNonRunnableApp(FSAppAttempt app) {
    usersNonRunnableApps.remove(app.getUser(), app);
  }

  /**
   * Traverses the queue hierarchy under the given queue to gather all lists
   * of non-runnable applications.
   *
   * @param queue FSQueue
   * @param appLists FSAppAttempt List.
   */
  private void gatherPossiblyRunnableAppLists(FSQueue queue,
      List<List<FSAppAttempt>> appLists) {
    if (queue.getNumRunnableApps() < queue.getMaxRunningApps()) {
      if (queue instanceof FSLeafQueue) {
        appLists.add(
            ((FSLeafQueue)queue).getCopyOfNonRunnableAppSchedulables());
      } else {
        for (FSQueue child : queue.getChildQueues()) {
          gatherPossiblyRunnableAppLists(child, appLists);
        }
      }
    }
  }

  /**
   * Takes a list of lists, each of which is ordered by start time, and returns
   * their elements in order of start time.
   * 
   * We maintain positions in each of the lists.  Each next() call advances
   * the position in one of the lists.  We maintain a heap that orders lists
   * by the start time of the app in the current position in that list.
   * This allows us to pick which list to advance in O(log(num lists)) instead
   * of O(num lists) time.
   */
  static class MultiListStartTimeIterator implements
      Iterator<FSAppAttempt> {

    private List<FSAppAttempt>[] appLists;
    private int[] curPositionsInAppLists;
    private PriorityQueue<IndexAndTime> appListsByCurStartTime;

    @SuppressWarnings("unchecked")
    public MultiListStartTimeIterator(List<List<FSAppAttempt>> appListList) {
      appLists = appListList.toArray(new List[appListList.size()]);
      curPositionsInAppLists = new int[appLists.length];
      appListsByCurStartTime = new PriorityQueue<IndexAndTime>();
      for (int i = 0; i < appLists.length; i++) {
        long time = appLists[i].isEmpty() ? Long.MAX_VALUE : appLists[i].get(0)
            .getStartTime();
        appListsByCurStartTime.add(new IndexAndTime(i, time));
      }
    }

    @Override
    public boolean hasNext() {
      return !appListsByCurStartTime.isEmpty()
          && appListsByCurStartTime.peek().time != Long.MAX_VALUE;
    }

    @Override
    public FSAppAttempt next() {
      IndexAndTime indexAndTime = appListsByCurStartTime.remove();
      int nextListIndex = indexAndTime.index;
      FSAppAttempt next = appLists[nextListIndex]
          .get(curPositionsInAppLists[nextListIndex]);
      curPositionsInAppLists[nextListIndex]++;

      if (curPositionsInAppLists[nextListIndex] < appLists[nextListIndex].size()) {
        indexAndTime.time = appLists[nextListIndex]
            .get(curPositionsInAppLists[nextListIndex]).getStartTime();
      } else {
        indexAndTime.time = Long.MAX_VALUE;
      }
      appListsByCurStartTime.add(indexAndTime);

      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }

    private static class IndexAndTime implements Comparable<IndexAndTime> {
      public int index;
      public long time;

      public IndexAndTime(int index, long time) {
        this.index = index;
        this.time = time;
      }

      @Override
      public int compareTo(IndexAndTime o) {
        return time < o.time ? -1 : (time > o.time ? 1 : 0);
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof IndexAndTime)) {
          return false;
        }
        IndexAndTime other = (IndexAndTime)o;
        return other.time == time;
      }

      @Override
      public int hashCode() {
        return (int)time;
      }
    }
  }
}
