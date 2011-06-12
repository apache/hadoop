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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Shuffle<K, V> implements ExceptionReporter {
  private static final Log LOG = LogFactory.getLog(Shuffle.class);
  private static final int PROGRESS_FREQUENCY = 2000;
  
  private final TaskAttemptID reduceId;
  private final JobConf jobConf;
  private final Reporter reporter;
  private final ShuffleClientMetrics metrics;
  private final TaskUmbilicalProtocol umbilical;
  
  private final ShuffleScheduler<K,V> scheduler;
  private final MergeManager<K, V> merger;
  private Throwable throwable = null;
  private String throwingThreadName = null;
  private final Progress copyPhase;
  private final TaskStatus taskStatus;
  private final Task reduceTask; //Used for status updates
  
  public Shuffle(TaskAttemptID reduceId, JobConf jobConf, FileSystem localFS,
                 TaskUmbilicalProtocol umbilical,
                 LocalDirAllocator localDirAllocator,  
                 Reporter reporter,
                 CompressionCodec codec,
                 Class<? extends Reducer> combinerClass,
                 CombineOutputCollector<K,V> combineCollector,
                 Counters.Counter spilledRecordsCounter,
                 Counters.Counter reduceCombineInputCounter,
                 Counters.Counter shuffledMapsCounter,
                 Counters.Counter reduceShuffleBytes,
                 Counters.Counter failedShuffleCounter,
                 Counters.Counter mergedMapOutputsCounter,
                 TaskStatus status,
                 Progress copyPhase,
                 Progress mergePhase,
                 Task reduceTask) {
    this.reduceId = reduceId;
    this.jobConf = jobConf;
    this.umbilical = umbilical;
    this.reporter = reporter;
    this.metrics = new ShuffleClientMetrics(reduceId, jobConf);
    this.copyPhase = copyPhase;
    this.taskStatus = status;
    this.reduceTask = reduceTask;
    
    scheduler = 
      new ShuffleScheduler<K,V>(jobConf, status, this, copyPhase, 
                                shuffledMapsCounter, 
                                reduceShuffleBytes, failedShuffleCounter);
    merger = new MergeManager<K, V>(reduceId, jobConf, localFS, 
                                    localDirAllocator, reporter, codec, 
                                    combinerClass, combineCollector, 
                                    spilledRecordsCounter, 
                                    reduceCombineInputCounter, 
                                    mergedMapOutputsCounter, 
                                    this, mergePhase);
  }

  @SuppressWarnings("unchecked")
  public RawKeyValueIterator run() throws IOException, InterruptedException {
    // Start the map-completion events fetcher thread
    final EventFetcher<K,V> eventFetcher = 
      new EventFetcher<K,V>(reduceId, umbilical, scheduler, this);
    eventFetcher.start();
    
    // Start the map-output fetcher threads
    final int numFetchers = jobConf.getInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 5);
    Fetcher<K,V>[] fetchers = new Fetcher[numFetchers];
    for (int i=0; i < numFetchers; ++i) {
      fetchers[i] = new Fetcher<K,V>(jobConf, reduceId, scheduler, merger, 
                                     reporter, metrics, this, 
                                     reduceTask.getJobTokenSecret());
      fetchers[i].start();
    }
    
    // Wait for shuffle to complete successfully
    while (!scheduler.waitUntilDone(PROGRESS_FREQUENCY)) {
      reporter.progress();
      
      synchronized (this) {
        if (throwable != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
                                 throwable);
        }
      }
    }

    // Stop the event-fetcher thread
    eventFetcher.interrupt();
    try {
      eventFetcher.join();
    } catch(Throwable t) {
      LOG.info("Failed to stop " + eventFetcher.getName(), t);
    }
    
    // Stop the map-output fetcher threads
    for (Fetcher<K,V> fetcher : fetchers) {
      fetcher.interrupt();
    }
    for (Fetcher<K,V> fetcher : fetchers) {
      fetcher.join();
    }
    fetchers = null;
    
    // stop the scheduler
    scheduler.close();

    copyPhase.complete(); // copy is already complete
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);

    // Finish the on-going merges...
    RawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge " , e);
    }

    // Sanity check
    synchronized (this) {
      if (throwable != null) {
        throw new ShuffleError("error in shuffle in " + throwingThreadName,
                               throwable);
      }
    }
    
    return kvIter;
  }

  public synchronized void reportException(Throwable t) {
    if (throwable == null) {
      throwable = t;
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the 
      // exception immediately.
      synchronized (scheduler) {
        scheduler.notifyAll();
      }
    }
  }
  
  public static class ShuffleError extends IOException {
    private static final long serialVersionUID = 5753909320586607881L;

    ShuffleError(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
