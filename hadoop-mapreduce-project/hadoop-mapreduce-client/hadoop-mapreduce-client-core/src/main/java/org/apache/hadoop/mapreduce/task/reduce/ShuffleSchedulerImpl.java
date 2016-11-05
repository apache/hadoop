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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.reduce.MapHost.State;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Time;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ShuffleSchedulerImpl<K,V> implements ShuffleScheduler<K,V> {
  private static final ThreadLocal<Long> SHUFFLE_START =
      new ThreadLocal<Long>() {
    protected Long initialValue() {
      return 0L;
    }
  };

  private static final Log LOG = LogFactory.getLog(ShuffleSchedulerImpl.class);
  private static final int MAX_MAPS_AT_ONCE = 20;
  private static final long INITIAL_PENALTY = 10000;
  private static final float PENALTY_GROWTH_RATE = 1.3f;
  private final static int REPORT_FAILURE_LIMIT = 10;
  private static final float BYTES_PER_MILLIS_TO_MBS = 1000f / 1024 / 1024;
  
  private final boolean[] finishedMaps;

  private final int totalMaps;
  private int remainingMaps;
  private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  private Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();

  private final TaskAttemptID reduceId;
  private final Random random = new Random();
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee = new Referee();
  private final Map<TaskAttemptID,IntWritable> failureCounts =
    new HashMap<TaskAttemptID,IntWritable>();
  private final Map<String,IntWritable> hostFailures =
    new HashMap<String,IntWritable>();
  private final TaskStatus status;
  private final ExceptionReporter reporter;
  private final int abortFailureLimit;
  private final Progress progress;
  private final Counters.Counter shuffledMapsCounter;
  private final Counters.Counter reduceShuffleBytes;
  private final Counters.Counter failedShuffleCounter;

  private final long startTime;
  private long lastProgressTime;

  private final CopyTimeTracker copyTimeTracker;

  private volatile int maxMapRuntime = 0;
  private final int maxFailedUniqueFetches;
  private final int maxFetchFailuresBeforeReporting;

  private long totalBytesShuffledTillNow = 0;
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final boolean reportReadErrorImmediately;
  private long maxPenalty = MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY;
  private int maxHostFailures;

  public ShuffleSchedulerImpl(JobConf job, TaskStatus status,
                          TaskAttemptID reduceId,
                          ExceptionReporter reporter,
                          Progress progress,
                          Counters.Counter shuffledMapsCounter,
                          Counters.Counter reduceShuffleBytes,
                          Counters.Counter failedShuffleCounter) {
    totalMaps = job.getNumMapTasks();
    abortFailureLimit = Math.max(30, totalMaps / 10);
    copyTimeTracker = new CopyTimeTracker();
    remainingMaps = totalMaps;
    finishedMaps = new boolean[remainingMaps];
    this.reporter = reporter;
    this.status = status;
    this.reduceId = reduceId;
    this.progress = progress;
    this.shuffledMapsCounter = shuffledMapsCounter;
    this.reduceShuffleBytes = reduceShuffleBytes;
    this.failedShuffleCounter = failedShuffleCounter;
    this.startTime = Time.monotonicNow();
    lastProgressTime = startTime;
    referee.start();
    this.maxFailedUniqueFetches = Math.min(totalMaps, 5);
    this.maxFetchFailuresBeforeReporting = job.getInt(
        MRJobConfig.SHUFFLE_FETCH_FAILURES, REPORT_FAILURE_LIMIT);
    this.reportReadErrorImmediately = job.getBoolean(
        MRJobConfig.SHUFFLE_NOTIFY_READERROR, true);

    this.maxPenalty = job.getLong(MRJobConfig.MAX_SHUFFLE_FETCH_RETRY_DELAY,
        MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY);
    this.maxHostFailures = job.getInt(
        MRJobConfig.MAX_SHUFFLE_FETCH_HOST_FAILURES,
        MRJobConfig.DEFAULT_MAX_SHUFFLE_FETCH_HOST_FAILURES);
  }

  @Override
  public void resolve(TaskCompletionEvent event) {
    switch (event.getTaskStatus()) {
    case SUCCEEDED:
      URI u = getBaseURI(reduceId, event.getTaskTrackerHttp());
      addKnownMapOutput(u.getHost() + ":" + u.getPort(),
          u.toString(),
          event.getTaskAttemptId());
      maxMapRuntime = Math.max(maxMapRuntime, event.getTaskRunTime());
      break;
    case FAILED:
    case KILLED:
    case OBSOLETE:
      obsoleteMapOutput(event.getTaskAttemptId());
      LOG.info("Ignoring obsolete output of " + event.getTaskStatus() +
          " map-task: '" + event.getTaskAttemptId() + "'");
      break;
    case TIPFAILED:
      tipFailed(event.getTaskAttemptId().getTaskID());
      LOG.info("Ignoring output of failed map TIP: '" +
          event.getTaskAttemptId() + "'");
      break;
    }
  }

  static URI getBaseURI(TaskAttemptID reduceId, String url) {
    StringBuffer baseUrl = new StringBuffer(url);
    if (!url.endsWith("/")) {
      baseUrl.append("/");
    }
    baseUrl.append("mapOutput?job=");
    baseUrl.append(reduceId.getJobID());
    baseUrl.append("&reduce=");
    baseUrl.append(reduceId.getTaskID().getId());
    baseUrl.append("&map=");
    URI u = URI.create(baseUrl.toString());
    return u;
  }

  public synchronized void copySucceeded(TaskAttemptID mapId,
                                         MapHost host,
                                         long bytes,
                                         long startMillis,
                                         long endMillis,
                                         MapOutput<K,V> output
                                         ) throws IOException {
    failureCounts.remove(mapId);
    hostFailures.remove(host.getHostName());
    int mapIndex = mapId.getTaskID().getId();

    if (!finishedMaps[mapIndex]) {
      output.commit();
      finishedMaps[mapIndex] = true;
      shuffledMapsCounter.increment(1);
      if (--remainingMaps == 0) {
        notifyAll();
      }

      // update single copy task status
      long copyMillis = (endMillis - startMillis);
      if (copyMillis == 0) copyMillis = 1;
      float bytesPerMillis = (float) bytes / copyMillis;
      float transferRate = bytesPerMillis * BYTES_PER_MILLIS_TO_MBS;
      String individualProgress = "copy task(" + mapId + " succeeded"
          + " at " + mbpsFormat.format(transferRate) + " MB/s)";
      // update the aggregated status
      copyTimeTracker.add(startMillis, endMillis);

      totalBytesShuffledTillNow += bytes;
      updateStatus(individualProgress);
      reduceShuffleBytes.increment(bytes);
      lastProgressTime = Time.monotonicNow();
      LOG.debug("map " + mapId + " done " + status.getStateString());
    }
  }

  private synchronized void updateStatus(String individualProgress) {
    int mapsDone = totalMaps - remainingMaps;
    long totalCopyMillis = copyTimeTracker.getCopyMillis();
    if (totalCopyMillis == 0) totalCopyMillis = 1;
    float bytesPerMillis = (float) totalBytesShuffledTillNow / totalCopyMillis;
    float transferRate = bytesPerMillis * BYTES_PER_MILLIS_TO_MBS;
    progress.set((float) mapsDone / totalMaps);
    String statusString = mapsDone + " / " + totalMaps + " copied.";
    status.setStateString(statusString);

    if (individualProgress != null) {
      progress.setStatus(individualProgress + " Aggregated copy rate(" + 
          mapsDone + " of " + totalMaps + " at " + 
      mbpsFormat.format(transferRate) + " MB/s)");
    } else {
      progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
          + mbpsFormat.format(transferRate) + " MB/s)");
    }
  }
  
  private void updateStatus() {
    updateStatus(null);
  }

  public synchronized void hostFailed(String hostname) {
    if (hostFailures.containsKey(hostname)) {
      IntWritable x = hostFailures.get(hostname);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostname, new IntWritable(1));
    }
  }

  @VisibleForTesting
  synchronized int hostFailureCount(String hostname) {
    int failures = 0;
    if (hostFailures.containsKey(hostname)) {
      failures = hostFailures.get(hostname).get();
    }
    return failures;
  }

  @VisibleForTesting
  synchronized int fetchFailureCount(TaskAttemptID mapId) {
    int failures = 0;
    if (failureCounts.containsKey(mapId)) {
      failures = failureCounts.get(mapId).get();
    }
    return failures;
  }

  public synchronized void copyFailed(TaskAttemptID mapId, MapHost host,
      boolean readError, boolean connectExcpt) {
    int failures = 1;
    if (failureCounts.containsKey(mapId)) {
      IntWritable x = failureCounts.get(mapId);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(mapId, new IntWritable(1));
    }
    String hostname = host.getHostName();
    IntWritable hostFailedNum = hostFailures.get(hostname);
    // MAPREDUCE-6361: hostname could get cleanup from hostFailures in another
    // thread with copySucceeded.
    // In this case, add back hostname to hostFailures to get rid of NPE issue.
    if (hostFailedNum == null) {
      hostFailures.put(hostname, new IntWritable(1));
    }
    //report failure if already retried maxHostFailures times
    boolean hostFail = hostFailures.get(hostname).get() >
        getMaxHostFailures() ? true : false;

    if (failures >= abortFailureLimit) {
      try {
        throw new IOException(failures + " failures downloading " + mapId);
      } catch (IOException ie) {
        reporter.reportException(ie);
      }
    }

    checkAndInformMRAppMaster(failures, mapId, readError, connectExcpt,
        hostFail);

    checkReducerHealth();

    long delay = (long) (INITIAL_PENALTY *
        Math.pow(PENALTY_GROWTH_RATE, failures));
    penalize(host, Math.min(delay, maxPenalty));

    failedShuffleCounter.increment(1);
  }

  /**
   * Ask the shuffle scheduler to penalize a given host for a given amount
   * of time before it reassigns a new fetcher to fetch from the host.
   * @param host The host to penalize.
   * @param delay The time to wait for before retrying
   */
  void penalize(MapHost host, long delay) {
    host.penalize();
    penalties.add(new Penalty(host, delay));
  }

  public void reportLocalError(IOException ioe) {
    try {
      LOG.error("Shuffle failed : local error on this node: "
          + InetAddress.getLocalHost());
    } catch (UnknownHostException e) {
      LOG.error("Shuffle failed : local error on this node");
    }
    reporter.reportException(ioe);
  }

  // Notify the MRAppMaster
  // after every read error, if 'reportReadErrorImmediately' is true or
  // after every 'maxFetchFailuresBeforeReporting' failures
  private void checkAndInformMRAppMaster(
      int failures, TaskAttemptID mapId, boolean readError,
      boolean connectExcpt, boolean hostFailed) {
    if (connectExcpt || (reportReadErrorImmediately && readError)
        || ((failures % maxFetchFailuresBeforeReporting) == 0) || hostFailed) {
      LOG.info("Reporting fetch failure for " + mapId + " to MRAppMaster.");
      status.addFetchFailedMap((org.apache.hadoop.mapred.TaskAttemptID) mapId);
    }
  }

  private void checkReducerHealth() {
    final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;
    final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;
    final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

    long totalFailures = failedShuffleCounter.getValue();
    int doneMaps = totalMaps - remainingMaps;

    boolean reducerHealthy =
      (((float)totalFailures / (totalFailures + doneMaps))
          < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);

    // check if the reducer has progressed enough
    boolean reducerProgressedEnough =
      (((float)doneMaps / totalMaps)
          >= MIN_REQUIRED_PROGRESS_PERCENT);

    // check if the reducer is stalled for a long time
    // duration for which the reducer is stalled
    int stallDuration =
      (int)(Time.monotonicNow() - lastProgressTime);

    // duration for which the reducer ran with progress
    int shuffleProgressDuration =
      (int)(lastProgressTime - startTime);

    // min time the reducer should run without getting killed
    int minShuffleRunDuration =
      Math.max(shuffleProgressDuration, maxMapRuntime);

    boolean reducerStalled =
      (((float)stallDuration / minShuffleRunDuration)
          >= MAX_ALLOWED_STALL_TIME_PERCENT);

    // kill if not healthy and has insufficient progress
    if ((failureCounts.size() >= maxFailedUniqueFetches ||
        failureCounts.size() == (totalMaps - doneMaps))
        && !reducerHealthy
        && (!reducerProgressedEnough || reducerStalled)) {
      LOG.fatal("Shuffle failed with too many fetch failures " +
      "and insufficient progress!");
      String errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
      reporter.reportException(new IOException(errorMsg));
    }

  }

  public synchronized void tipFailed(TaskID taskId) {
    if (!finishedMaps[taskId.getId()]) {
      finishedMaps[taskId.getId()] = true;
      if (--remainingMaps == 0) {
        notifyAll();
      }
      updateStatus();
    }
  }

  public synchronized void addKnownMapOutput(String hostName,
                                             String hostUrl,
                                             TaskAttemptID mapId) {
    MapHost host = mapLocations.get(hostName);
    if (host == null) {
      host = new MapHost(hostName, hostUrl);
      mapLocations.put(hostName, host);
    }
    host.addKnownMap(mapId);

    // Mark the host as pending
    if (host.getState() == State.PENDING) {
      pendingHosts.add(host);
      notifyAll();
    }
  }


  public synchronized void obsoleteMapOutput(TaskAttemptID mapId) {
    obsoleteMaps.add(mapId);
  }

  public synchronized void putBackKnownMapOutput(MapHost host,
                                                 TaskAttemptID mapId) {
    host.addKnownMap(mapId);
  }


  public synchronized MapHost getHost() throws InterruptedException {
      while(pendingHosts.isEmpty()) {
        wait();
      }

      MapHost host = null;
      Iterator<MapHost> iter = pendingHosts.iterator();
      int numToPick = random.nextInt(pendingHosts.size());
      for (int i=0; i <= numToPick; ++i) {
        host = iter.next();
      }

      pendingHosts.remove(host);
      host.markBusy();

      LOG.debug("Assigning " + host + " with " + host.getNumKnownMapOutputs() +
               " to " + Thread.currentThread().getName());
      SHUFFLE_START.set(Time.monotonicNow());

      return host;
  }

  public synchronized List<TaskAttemptID> getMapsForHost(MapHost host) {
    List<TaskAttemptID> list = host.getAndClearKnownMaps();
    Iterator<TaskAttemptID> itr = list.iterator();
    List<TaskAttemptID> result = new ArrayList<TaskAttemptID>();
    int includedMaps = 0;
    int totalSize = list.size();
    // find the maps that we still need, up to the limit
    while (itr.hasNext()) {
      TaskAttemptID id = itr.next();
      if (!obsoleteMaps.contains(id) && !finishedMaps[id.getTaskID().getId()]) {
        result.add(id);
        if (++includedMaps >= MAX_MAPS_AT_ONCE) {
          break;
        }
      }
    }
    // put back the maps left after the limit
    while (itr.hasNext()) {
      TaskAttemptID id = itr.next();
      if (!obsoleteMaps.contains(id) && !finishedMaps[id.getTaskID().getId()]) {
        host.addKnownMap(id);
      }
    }
    LOG.debug("assigned " + includedMaps + " of " + totalSize + " to " +
             host + " to " + Thread.currentThread().getName());
    return result;
  }

  public synchronized void freeHost(MapHost host) {
    if (host.getState() != State.PENALIZED) {
      if (host.markAvailable() == State.PENDING) {
        pendingHosts.add(host);
        notifyAll();
      }
    }
    LOG.info(host + " freed by " + Thread.currentThread().getName() + " in " +
             (Time.monotonicNow()-SHUFFLE_START.get()) + "ms");
  }

  public synchronized void resetKnownMaps() {
    mapLocations.clear();
    obsoleteMaps.clear();
    pendingHosts.clear();
  }

  /**
   * Wait until the shuffle finishes or until the timeout.
   * @param millis maximum wait time
   * @return true if the shuffle is done
   * @throws InterruptedException
   */
  @Override
  public synchronized boolean waitUntilDone(int millis
                                            ) throws InterruptedException {
    if (remainingMaps > 0) {
      wait(millis);
      return remainingMaps == 0;
    }
    return true;
  }

  /**
   * A structure that records the penalty for a host.
   */
  private static class Penalty implements Delayed {
    MapHost host;
    private long endTime;

    Penalty(MapHost host, long delay) {
      this.host = host;
      this.endTime = Time.monotonicNow() + delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long remainingTime = endTime - Time.monotonicNow();
      return unit.convert(remainingTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      long other = ((Penalty) o).endTime;
      return endTime == other ? 0 : (endTime < other ? -1 : 1);
    }

  }

  /**
   * A thread that takes hosts off of the penalty list when the timer expires.
   */
  private class Referee extends Thread {
    public Referee() {
      setName("ShufflePenaltyReferee");
      setDaemon(true);
    }

    public void run() {
      try {
        while (true) {
          // take the first host that has an expired penalty
          MapHost host = penalties.take().host;
          synchronized (ShuffleSchedulerImpl.this) {
            if (host.markAvailable() == MapHost.State.PENDING) {
              pendingHosts.add(host);
              ShuffleSchedulerImpl.this.notifyAll();
            }
          }
        }
      } catch (InterruptedException ie) {
        return;
      } catch (Throwable t) {
        reporter.reportException(t);
      }
    }
  }

  @Override
  public void close() throws InterruptedException {
    referee.interrupt();
    referee.join();
  }

  public int getMaxHostFailures() {
    return maxHostFailures;
  }

  private static class CopyTimeTracker {
    List<Interval> intervals;
    long copyMillis;
    public CopyTimeTracker() {
      intervals = Collections.emptyList();
      copyMillis = 0;
    }
    public void add(long s, long e) {
      Interval interval = new Interval(s, e);
      copyMillis = getTotalCopyMillis(interval);
    }
  
    public long getCopyMillis() {
      return copyMillis;
    }
    // This method captures the time during which any copy was in progress 
    // each copy time period is record in the Interval list
    private long getTotalCopyMillis(Interval newInterval) {
      if (newInterval == null) {
        return copyMillis;
      }
      List<Interval> result = new ArrayList<Interval>(intervals.size() + 1);
      for (Interval interval: intervals) {
        if (interval.end < newInterval.start) {
          result.add(interval);
        } else if (interval.start > newInterval.end) {
          result.add(newInterval);
          newInterval = interval;        
        } else {
          newInterval = new Interval(
              Math.min(interval.start, newInterval.start),
              Math.max(newInterval.end, interval.end));
        }
      }
      result.add(newInterval);
      intervals = result;
      
      //compute total millis
      long length = 0;
      for (Interval interval : intervals) {
        length += interval.getIntervalLength();
      }
      return length;
    }
    
    private static class Interval {
      final long start;
      final long end;
      public Interval(long s, long e) {
        start = s;
        end = e;
      }
      
      public long getIntervalLength() {
        return end - start;
      }
    }
  }
}
