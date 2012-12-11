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

import java.io.IOException;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * ShuffleConsumerPlugin for serving Reducers.  It may shuffle MOF files from
 * either the built-in ShuffleHandler or from a 3rd party AuxiliaryService.
 *
 */
@InterfaceAudience.LimitedPrivate("mapreduce")
@InterfaceStability.Unstable
public interface ShuffleConsumerPlugin<K, V> {

  public void init(Context<K, V> context);

  public RawKeyValueIterator run() throws IOException, InterruptedException;

  public void close();

  @InterfaceAudience.LimitedPrivate("mapreduce")
  @InterfaceStability.Unstable
  public static class Context<K,V> {
    private final org.apache.hadoop.mapreduce.TaskAttemptID reduceId;
    private final JobConf jobConf;
    private final FileSystem localFS;
    private final TaskUmbilicalProtocol umbilical;
    private final LocalDirAllocator localDirAllocator;
    private final Reporter reporter;
    private final CompressionCodec codec;
    private final Class<? extends Reducer> combinerClass;
    private final CombineOutputCollector<K, V> combineCollector;
    private final Counters.Counter spilledRecordsCounter;
    private final Counters.Counter reduceCombineInputCounter;
    private final Counters.Counter shuffledMapsCounter;
    private final Counters.Counter reduceShuffleBytes;
    private final Counters.Counter failedShuffleCounter;
    private final Counters.Counter mergedMapOutputsCounter;
    private final TaskStatus status;
    private final Progress copyPhase;
    private final Progress mergePhase;
    private final Task reduceTask;
    private final MapOutputFile mapOutputFile;

    public Context(org.apache.hadoop.mapreduce.TaskAttemptID reduceId,
                   JobConf jobConf, FileSystem localFS,
                   TaskUmbilicalProtocol umbilical,
                   LocalDirAllocator localDirAllocator,
                   Reporter reporter, CompressionCodec codec,
                   Class<? extends Reducer> combinerClass,
                   CombineOutputCollector<K,V> combineCollector,
                   Counters.Counter spilledRecordsCounter,
                   Counters.Counter reduceCombineInputCounter,
                   Counters.Counter shuffledMapsCounter,
                   Counters.Counter reduceShuffleBytes,
                   Counters.Counter failedShuffleCounter,
                   Counters.Counter mergedMapOutputsCounter,
                   TaskStatus status, Progress copyPhase, Progress mergePhase,
                   Task reduceTask, MapOutputFile mapOutputFile) {
      this.reduceId = reduceId;
      this.jobConf = jobConf;
      this.localFS = localFS;
      this. umbilical = umbilical;
      this.localDirAllocator = localDirAllocator;
      this.reporter = reporter;
      this.codec = codec;
      this.combinerClass = combinerClass;
      this.combineCollector = combineCollector;
      this.spilledRecordsCounter = spilledRecordsCounter;
      this.reduceCombineInputCounter = reduceCombineInputCounter;
      this.shuffledMapsCounter = shuffledMapsCounter;
      this.reduceShuffleBytes = reduceShuffleBytes;
      this.failedShuffleCounter = failedShuffleCounter;
      this.mergedMapOutputsCounter = mergedMapOutputsCounter;
      this.status = status;
      this.copyPhase = copyPhase;
      this.mergePhase = mergePhase;
      this.reduceTask = reduceTask;
      this.mapOutputFile = mapOutputFile;
    }

    public org.apache.hadoop.mapreduce.TaskAttemptID getReduceId() {
      return reduceId;
    }
    public JobConf getJobConf() {
      return jobConf;
    }
    public FileSystem getLocalFS() {
      return localFS;
    }
    public TaskUmbilicalProtocol getUmbilical() {
      return umbilical;
    }
    public LocalDirAllocator getLocalDirAllocator() {
      return localDirAllocator;
    }
    public Reporter getReporter() {
      return reporter;
    }
    public CompressionCodec getCodec() {
      return codec;
    }
    public Class<? extends Reducer> getCombinerClass() {
      return combinerClass;
    }
    public CombineOutputCollector<K, V> getCombineCollector() {
      return combineCollector;
    }
    public Counters.Counter getSpilledRecordsCounter() {
      return spilledRecordsCounter;
    }
    public Counters.Counter getReduceCombineInputCounter() {
      return reduceCombineInputCounter;
    }
    public Counters.Counter getShuffledMapsCounter() {
      return shuffledMapsCounter;
    }
    public Counters.Counter getReduceShuffleBytes() {
      return reduceShuffleBytes;
    }
    public Counters.Counter getFailedShuffleCounter() {
      return failedShuffleCounter;
    }
    public Counters.Counter getMergedMapOutputsCounter() {
      return mergedMapOutputsCounter;
    }
    public TaskStatus getStatus() {
      return status;
    }
    public Progress getCopyPhase() {
      return copyPhase;
    }
    public Progress getMergePhase() {
      return mergePhase;
    }
    public Task getReduceTask() {
      return reduceTask;
    }
    public MapOutputFile getMapOutputFile() {
      return mapOutputFile;
    }
  } // end of public static class Context<K,V>

}
