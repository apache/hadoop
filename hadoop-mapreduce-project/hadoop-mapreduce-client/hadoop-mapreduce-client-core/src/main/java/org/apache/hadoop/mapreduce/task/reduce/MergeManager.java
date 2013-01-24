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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

import java.io.IOException;

/**
 * An interface for a reduce side merge that works with the default Shuffle
 * implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface MergeManager<K, V> {
  /**
   * To wait until merge has some freed resources available so that it can
   * accept shuffled data.  This will be called before a network connection is
   * established to get the map output.
   */
  public void waitForResource() throws InterruptedException;

  /**
   * To reserve resources for data to be shuffled.  This will be called after
   * a network connection is made to shuffle the data.
   * @param mapId mapper from which data will be shuffled.
   * @param requestedSize size in bytes of data that will be shuffled.
   * @param fetcher id of the map output fetcher that will shuffle the data.
   * @return a MapOutput object that can be used by shuffle to shuffle data.  If
   * required resources cannot be reserved immediately, a null can be returned.
   */
  public MapOutput<K, V> reserve(TaskAttemptID mapId, long requestedSize,
                                 int fetcher) throws IOException;

  /**
   * Called at the end of shuffle.
   * @return a key value iterator object.
   */
  public RawKeyValueIterator close() throws Throwable;
}
