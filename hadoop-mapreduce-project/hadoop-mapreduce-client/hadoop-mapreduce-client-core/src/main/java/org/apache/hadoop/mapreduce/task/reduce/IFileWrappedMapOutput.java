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
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Common code for allowing MapOutput classes to handle streams.
 *
 * @param <K> key type for map output
 * @param <V> value type for map output
 */
public abstract class IFileWrappedMapOutput<K, V> extends MapOutput<K, V> {
  private final Configuration conf;
  private final MergeManagerImpl<K, V> merger;

  public IFileWrappedMapOutput(
      Configuration c, MergeManagerImpl<K, V> m, TaskAttemptID mapId,
      long size, boolean primaryMapOutput) {
    super(mapId, size, primaryMapOutput);
    conf = c;
    merger = m;
  }

  /**
   * @return the merger
   */
  protected MergeManagerImpl<K, V> getMerger() {
    return merger;
  }

  protected abstract void doShuffle(
      MapHost host, IFileInputStream iFileInputStream,
      long compressedLength, long decompressedLength,
      ShuffleClientMetrics metrics, Reporter reporter) throws IOException;

  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    doShuffle(host, new IFileInputStream(input, compressedLength, conf),
        compressedLength, decompressedLength, metrics, reporter);
  }
}
