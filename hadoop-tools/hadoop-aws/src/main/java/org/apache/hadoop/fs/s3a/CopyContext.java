/*
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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.Copy;


/**
 * A wrapper around a {@link Copy} object, the source key for the copy, the destination for the copy, the length of the
 * key copied, and the {@link ProgressListener.ExceptionReporter} for the copy.
 */
class CopyContext {

  private final Copy copy;
  private final String srcKey;
  private final String destKey;
  private final long length;
  private final ProgressListener.ExceptionReporter progressListener;

  CopyContext(Copy copy, String srcKey, String destKey, long length,
              ProgressListener.ExceptionReporter progressListener) {
    this.copy = copy;
    this.srcKey = srcKey;
    this.destKey = destKey;
    this.length = length;
    this.progressListener = progressListener;
  }

  Copy getCopy() {
    return copy;
  }

  String getSrcKey() {
    return srcKey;
  }

  String getDestKey() {
    return destKey;
  }

  long getLength() {
    return length;
  }

  ProgressListener.ExceptionReporter getProgressListener() {
    return progressListener;
  }
}
