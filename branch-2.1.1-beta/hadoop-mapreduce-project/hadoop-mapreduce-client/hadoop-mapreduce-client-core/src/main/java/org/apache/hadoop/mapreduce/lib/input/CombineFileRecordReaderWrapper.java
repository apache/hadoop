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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
  extends RecordReader<K,V> {
  private final FileSplit fileSplit;
  private final RecordReader<K,V> delegate;

  protected CombineFileRecordReaderWrapper(FileInputFormat<K,V> inputFormat,
    CombineFileSplit split, TaskAttemptContext context, Integer idx)
    throws IOException, InterruptedException {
    fileSplit = new FileSplit(split.getPath(idx),
      split.getOffset(idx),
      split.getLength(idx),
      split.getLocations());

    delegate = inputFormat.createRecordReader(fileSplit, context);
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    // it really should be the same file split at the time the wrapper instance
    // was created
    assert fileSplitIsValid(context);

    delegate.initialize(fileSplit, context);
  }

  private boolean fileSplitIsValid(TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    long offset = conf.getLong(MRJobConfig.MAP_INPUT_START, 0L);
    if (fileSplit.getStart() != offset) {
      return false;
    }
    long length = conf.getLong(MRJobConfig.MAP_INPUT_PATH, 0L);
    if (fileSplit.getLength() != length) {
      return false;
    }
    String path = conf.get(MRJobConfig.MAP_INPUT_FILE);
    if (!fileSplit.getPath().toString().equals(path)) {
      return false;
    }
    return true;
  }

  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  public void close() throws IOException {
    delegate.close();
  }
}
