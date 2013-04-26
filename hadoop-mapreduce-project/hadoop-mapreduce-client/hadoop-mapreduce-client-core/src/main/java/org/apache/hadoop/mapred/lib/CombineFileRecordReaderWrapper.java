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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A wrapper class for a record reader that handles a single file split. It
 * delegates most of the methods to the wrapped instance. A concrete subclass
 * needs to provide a constructor that calls this parent constructor with the
 * appropriate input format. The subclass constructor must satisfy the specific
 * constructor signature that is required by
 * <code>CombineFileRecordReader</code>.
 *
 * Subclassing is needed to get a concrete record reader wrapper because of the
 * constructor requirement.
 *
 * @see CombineFileRecordReader
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileRecordReaderWrapper<K,V>
  implements RecordReader<K,V> {
  private final RecordReader<K,V> delegate;

  protected CombineFileRecordReaderWrapper(FileInputFormat<K,V> inputFormat,
    CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx)
    throws IOException {
    FileSplit fileSplit = new FileSplit(split.getPath(idx),
      split.getOffset(idx),
      split.getLength(idx),
      split.getLocations());

    delegate = inputFormat.getRecordReader(fileSplit, (JobConf)conf, reporter);
  }

  public boolean next(K key, V value) throws IOException {
    return delegate.next(key, value);
  }

  public K createKey() {
    return delegate.createKey();
  }

  public V createValue() {
    return delegate.createValue();
  }

  public long getPos() throws IOException {
    return delegate.getPos();
  }

  public void close() throws IOException {
    delegate.close();
  }

  public float getProgress() throws IOException {
    return delegate.getProgress();
  }
}