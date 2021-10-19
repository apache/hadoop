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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Handles tracking and enforcement for user and queue maxRunningApps
 * constraints.
 */
public class CSMaxRunningAppsEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(
      CSMaxRunningAppsEnforcer.class);

  private final CapacityScheduler scheduler;

  // Tracks the number of running applications by user.
  private final Map<String, Integer> usersNumRunnableApps;

  private final ListMultimap<String, FiCaSchedulerApp> usersNonRunnableApps;

  public CSMaxRunningAppsEnforcer(CapacityScheduler scheduler) {
    this.scheduler = scheduler;
    this.usersNumRunnableApps = new HashMap<String, Integer>();
    this.usersNonRunnableApps = ArrayListMultimap.create();
  }

  /**
   * Checks whether making the application runnable would exceed any
   * maxRunningApps limits. Also sets the "runnable" flag on the
   * attempt.
   *
   * @param attempt the app attempt being checked
   * @return true if the application is runnable; false otherwise
   */
  public boolean checkRunnabilityWithUpdate(
      FiCaSchedulerApp attempt) {
    boolean attemptCanRun = !exceedUserMaxParallelApps(attempt.getUser())
        && !exceedQueueMaxParallelApps(attempt.getCSLeafQueue());

    attempt.setRunnable(attemptCanRun);

    return attemptCanRun;
  }

  /**
   * Checks whether the number of user runnable apps exceeds the limitation.
   *
   * @param user the user name
   * @return true if the number hits the limit; false otherwise
   */
  private boolean exceedUserMaxParallelApps(String user) {
    Integer userNumRunnable = usersNumRunnableApps.get(user);
    if (userNumRunnable == null) {
      userNumRunnable = 0;
    }
    if (userNumRunnable >= getUserMaxParallelApps(user)) {
      LOG.info("Maximum runnable apps exceeded for user {}", user);
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
  private boolean exceedQueueMaxParallelApps(AbstractCSQueue queue) {
    // Check queue and all parent queues
    while (queue != null) {
      if (queue.getNumRunnableApps() >= queue.getMaxParallelApps()) {
        LOG.info("Maximum runnable apps exceeded for queue {}",
            queue.getQueuePath());
        return true;
      }
      queue = (AbstractCSQueue) queue.getParent();
    }

    return false;
  }

  public void trackApp(FiCaSchedulerApp app) {
    if (app.isRunnable()) {
      trackRunnableApp(app);
    } else {
      trackNonRunnableApp(app);
    }
  }
  /**
   * Tracks the given new runnable app for purposes of maintaining max running
   * app limits.
   */
  private void trackRunnableApp(FiCaSchedulerApp app) {
    String user = app.getUser();
    AbstractCSQueue queue = (AbstractCSQueue) app.getQueue();
    // Increment running counts for all parent queues
    ParentQueue parent = (ParentQueue) queue.getParent();
    while (parent != null) {
      parent.incrementRunnableApps();
      parent = (ParentQueue) parent.getParent();
    }

    Integer userNumRunnable = usersNumRunnableApps.get(user);
    usersNumRunnableApps.put(user, (userNumRunnable == null ? 0
        : userNumRunnable) + 1);
  }

  /**
   * Tracks the given new non runnable app so that it can be made runnable when
   * it would not violate max running app limits.
   */
  private void trackNonRunnableApp(FiCaSchedulerApp app) {
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
    ParentQueue rootQueue = (ParentQueue) scheduler.getRootQueue();
    List<List<FiCaSchedulerApp>> appsNowMaybeRunnable =
        new ArrayList<List<FiCaSchedulerApp>>();

    gatherPossiblyRunnableAppLists(rootQueue, appsNowMaybeRunnable);

    updateAppsRunnability(appsNowMaybeRunnable, Integer.MAX_VALUE);
  }

  /**
   * Checks to see whether any other applications runnable now that the given
   * application has been removed from the given queue.  And makes them so.
   *
   * Runs in O(n log(n)) where n is the number of queues that are under the
   * highest queue that went from having no slack to having slack.
   */
  public void updateRunnabilityOnAppRemoval(FiCaSchedulerApp app) {
    // childqueueX might have no pending apps itself, but if a queue higher up
    // in the hierarchy parentqueueY has a maxRunningApps set, an app completion
    // in childqueueX could allow an app in some other distant child of
    // parentqueueY to become runnable.
    // An app removal will only possibly allow another app to become runnable if
    // the queue was already at its max before the removal.
    // Thus we find the ancestor queue highest in the tree for which the app
    // that was at its maxRunningApps before the removal.
    LeafQueue queue = app.getCSLeafQueue();
    AbstractCSQueue highestQueueWithAppsNowRunnable =
        (queue.getNumRunnableApps() == queue.getMaxParallelApps() - 1)
        ? queue : null;

    ParentQueue parent = (ParentQueue) queue.getParent();
    while (parent != null) {
      if (parent.getNumRunnableApps() == parent.getMaxParallelApps() - 1) {
        highestQueueWithAppsNowRunnable = parent;
      }
      parent = (ParentQueue) parent.getParent();
    }

    List<List<FiCaSchedulerApp>> appsNowMaybeRunnable =
        new ArrayList<List<FiCaSchedulerApp>>();

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
    if (userNumRunning == getUserMaxParallelApps(user) - 1) {
      List<FiCaSchedulerApp> userWaitingApps = usersNonRunnableApps.get(user);
      if (userWaitingApps != null) {
        appsNowMaybeRunnable.add(userWaitingApps);
      }
    }

    updateAppsRunnability(appsNowMaybeRunnable,
        appsNowMaybeRunnable.size());
  }

  /**
   * Checks to see whether applications are runnable now by iterating
   * through each one of them and check if the queue and user have slack.
   *
   * if we know how many apps can be runnable, there is no need to iterate
   * through all apps, maxRunnableApps is used to break out of the iteration.
   */
  private void updateAppsRunnability(List<List<FiCaSchedulerApp>>
      appsNowMaybeRunnable, int maxRunnableApps) {
    // Scan through and check whether this means that any apps are now runnable
    Iterator<FiCaSchedulerApp> iter = new MultiListStartTimeIterator(
        appsNowMaybeRunnable);
    FiCaSchedulerApp prev = null;
    List<FiCaSchedulerApp> noLongerPendingApps = new ArrayList<>();
    while (iter.hasNext()) {
      FiCaSchedulerApp next = iter.next();
      if (next == prev) {
        continue;
      }

      if (checkRunnabilityWithUpdate(next)) {
        LeafQueue nextQueue = next.getCSLeafQueue();
        LOG.info("{} is now runnable in {}",
            next.getApplicationAttemptId(), nextQueue);
        trackRunnableApp(next);
        FiCaSchedulerApp appSched = next;
        nextQueue.submitApplicationAttempt(next, next.getUser());
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
    for (FiCaSchedulerApp appSched : noLongerPendingApps) {
      if (!(appSched.getCSLeafQueue().removeNonRunnableApp(appSched))) {
        LOG.error("Can't make app runnable that does not already exist in queue"
            + " as non-runnable: {}. This should never happen.",
            appSched.getApplicationAttemptId());
      }

      if (!usersNonRunnableApps.remove(appSched.getUser(), appSched)) {
        LOG.error("Waiting app {} expected to be in "
            + "usersNonRunnableApps, but was not. This should never happen.",
            appSched.getApplicationAttemptId());
      }
    }
  }

  public void untrackApp(FiCaSchedulerApp app) {
    if (app.isRunnable()) {
      untrackRunnableApp(app);
    } else {
      untrackNonRunnableApp(app);
    }
  }

  /**
   * Updates the relevant tracking variables after a runnable app with the given
   * queue and user has been removed.
   */
  private void untrackRunnableApp(FiCaSchedulerApp app) {
    // Update usersRunnableApps
    String user = app.getUser();
    int newUserNumRunning = usersNumRunnableApps.get(user) - 1;
    if (newUserNumRunning == 0) {
      usersNumRunnableApps.remove(user);
    } else {
      usersNumRunnableApps.put(user, newUserNumRunning);
    }

    // Update runnable app bookkeeping for queues
    AbstractCSQueue queue = (AbstractCSQueue) app.getQueue();
    ParentQueue parent = (ParentQueue) queue.getParent();
    while (parent != null) {
      parent.decrementRunnableApps();
      parent = (ParentQueue) parent.getParent();
    }
  }

  /**
   * Stops tracking the given non-runnable app.
   */
  private void untrackNonRunnableApp(FiCaSchedulerApp app) {
    usersNonRunnableApps.remove(app.getUser(), app);
  }

  /**
   * Traverses the queue hierarchy under the given queue to gather all lists
   * of non-runnable applications.
   */
  private void gatherPossiblyRunnableAppLists(AbstractCSQueue queue,
      List<List<FiCaSchedulerApp>> appLists) {
    if (queue.getNumRunnableApps() < queue.getMaxParallelApps()) {
      if (queue instanceof LeafQueue) {
        appLists.add(
            ((LeafQueue)queue).getCopyOfNonRunnableAppSchedulables());
      } else {
        for (CSQueue child : queue.getChildQueues()) {
          gatherPossiblyRunnableAppLists((AbstractCSQueue) child, appLists);
        }
      }
    }
  }

  private int getUserMaxParallelApps(String user) {
    CapacitySchedulerConfiguration conf = scheduler.getConfiguration();
    if (conf == null) {
      return Integer.MAX_VALUE;
    }

    int userMaxParallelApps = conf.getMaxParallelAppsForUser(user);

    return userMaxParallelApps;
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
      Iterator<FiCaSchedulerApp> {

    private List<FiCaSchedulerApp>[] appLists;
    private int[] curPositionsInAppLists;
    private PriorityQueue<IndexAndTime> appListsByCurStartTime;

    @SuppressWarnings("unchecked")
    MultiListStartTimeIterator(List<List<FiCaSchedulerApp>> appListList) {
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
    public FiCaSchedulerApp next() {
      IndexAndTime indexAndTime = appListsByCurStartTime.remove();
      int nextListIndex = indexAndTime.index;
      FiCaSchedulerApp next = appLists[nextListIndex]
          .get(curPositionsInAppLists[nextListIndex]);
      curPositionsInAppLists[nextListIndex]++;

      if (curPositionsInAppLists[nextListIndex] <
          appLists[nextListIndex].size()) {
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
      private int index;
      private long time;

      IndexAndTime(int index, long time) {
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
