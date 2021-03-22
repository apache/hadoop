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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalFetcher is used by LocalJobRunner to perform a local filesystem
 * fetch.
 */
class LocalFetcher<K,V> extends Fetcher<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFetcher.class);

  private static final MapHost LOCALHOST = new MapHost("local", "local");

  private JobConf job;
  private Map<TaskAttemptID, MapOutputFile> localMapFiles;

  public LocalFetcher(JobConf job, TaskAttemptID reduceId,
                 ShuffleSchedulerImpl<K, V> scheduler,
                 MergeManager<K,V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter,
                 SecretKey shuffleKey,
                 Map<TaskAttemptID, MapOutputFile> localMapFiles) {
    super(job, reduceId, scheduler, merger, reporter, metrics,
        exceptionReporter, shuffleKey);

    this.job = job;
    this.localMapFiles = localMapFiles;

    setName("localfetcher#" + id);
    setDaemon(true);
  }

  public void run() {
    // Create a worklist of task attempts to work over.
    Set<TaskAttemptID> maps = new HashSet<TaskAttemptID>();
    for (TaskAttemptID map : localMapFiles.keySet()) {
      maps.add(map);
    }

    while (maps.size() > 0) {
      try {
        // If merge is on, block
        merger.waitForResource();
        metrics.threadBusy();

        // Copy as much as is possible.
        doCopy(maps);
        metrics.threadFree();
      } catch (InterruptedException ie) {
      } catch (Throwable t) {
        exceptionReporter.reportException(t);
      }
    }
  }

  /**
   * The crux of the matter...
   */
  private void doCopy(Set<TaskAttemptID> maps) throws IOException {
    Iterator<TaskAttemptID> iter = maps.iterator();
    while (iter.hasNext()) {
      TaskAttemptID map = iter.next();
      LOG.debug("LocalFetcher " + id + " going to fetch: " + map);
      if (copyMapOutput(map)) {
        // Successful copy. Remove this from our worklist.
        iter.remove();
      } else {
        // We got back a WAIT command; go back to the outer loop
        // and block for InMemoryMerge.
        break;
      }
    }
  }

  /**
   * Retrieve the map output of a single map task
   * and send it to the merger.
   */
  private boolean copyMapOutput(TaskAttemptID mapTaskId) throws IOException {
    // Figure out where the map task stored its output.
    Path mapOutputFileName = localMapFiles.get(mapTaskId).getOutputFile();
    Path indexFileName = mapOutputFileName.suffix(".index");

    // Read its index to determine the location of our split
    // and its size.
    SpillRecord sr = new SpillRecord(indexFileName, job);
    IndexRecord ir = sr.getIndex(reduce);

    long compressedLength = ir.partLength;
    long decompressedLength = ir.rawLength;

    compressedLength -= CryptoUtils.cryptoPadding(job);
    decompressedLength -= CryptoUtils.cryptoPadding(job);

    // Get the location for the map output - either in-memory or on-disk
    MapOutput<K, V> mapOutput = merger.reserve(mapTaskId, decompressedLength,
        id);

    // Check if we can shuffle *now* ...
    if (mapOutput == null) {
      LOG.info("fetcher#" + id + " - MergeManager returned Status.WAIT ...");
      return false;
    }

    // Go!
    LOG.info("localfetcher#" + id + " about to shuffle output of map " + 
             mapOutput.getMapId() + " decomp: " +
             decompressedLength + " len: " + compressedLength + " to " +
             mapOutput.getDescription());

    // now read the file, seek to the appropriate section, and send it.
    FileSystem localFs = FileSystem.getLocal(job).getRaw();
    FSDataInputStream inStream = localFs.open(mapOutputFileName);
    try {
      inStream.seek(ir.startOffset);
      inStream =
          IntermediateEncryptedStream.wrapIfNecessary(job, inStream,
              mapOutputFileName);
      mapOutput.shuffle(LOCALHOST, inStream, compressedLength,
          decompressedLength, metrics, reporter);
    } finally {
      IOUtils.cleanupWithLogger(LOG, inStream);
    }

    scheduler.copySucceeded(mapTaskId, LOCALHOST, compressedLength, 0, 0,
        mapOutput);
    return true; // successful fetch.
  }
}

