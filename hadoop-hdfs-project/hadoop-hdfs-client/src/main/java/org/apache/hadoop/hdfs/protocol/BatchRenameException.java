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

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

/**
 * Thrown when break during a batch operation .
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BatchRenameException extends IOException {
  private static final long serialVersionUID = -1881850913029509889L;
  private static final String TAG_INDEX = "index";
  private static final String TAG_TOTAL = "total";
  private static final String TAG_REASON = "reason";

  /**
   * Used by RemoteException to instantiate an BatchRenameException.
   */
  public BatchRenameException(String msg) {
    super(msg);
  }

  public BatchRenameException(long index, long total, Throwable cause) {
    this(index, total, (cause != null) ? cause.getClass().getName() + ": " +
        cause.getMessage() : "Unknown reason");
  }

  public BatchRenameException(long index, long total,
                              String cause) {
    super("Batch operation partial success. " +
        getTagHeader(TAG_INDEX) + index + getTagTailer(TAG_INDEX) +
        getTagHeader(TAG_TOTAL) + total + getTagTailer(TAG_TOTAL) +
        getTagHeader(TAG_REASON) + cause + getTagTailer(TAG_REASON));
  }

  public long getIndex() {
    return Long.parseLong(getValue(TAG_INDEX));
  }

  public long getTotal() {
    return Long.parseLong(getValue(TAG_TOTAL));
  }

  public String getReason() {
    return getValue(TAG_REASON);
  }

  private static String getTagHeader(String tag) {
    return "<"+tag + ">";
  }

  private static String getTagTailer(String tag) {
    return "</"+tag + ">";
  }

  private String getValue(String target) {
    String msg = getMessage();
    String header = getTagHeader(target);
    String tailer = getTagTailer(target);
    int pos1 = msg.indexOf(header) + header.length();
    int pos2 = msg.indexOf(tailer, pos1);

    assert pos2 > pos1;
    return msg.substring(pos1, pos2);
  }
}
