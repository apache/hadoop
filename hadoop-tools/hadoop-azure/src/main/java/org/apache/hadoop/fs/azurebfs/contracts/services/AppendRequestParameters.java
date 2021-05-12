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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.apache.hadoop.fs.azurebfs.services.AbfsLease;

/**
 * Saves the different request parameters for append
 */
public class AppendRequestParameters {
  public enum Mode {
    APPEND_MODE,
    FLUSH_MODE,
    FLUSH_CLOSE_MODE
  }

  private final long position;
  private final int offset;
  private final int length;
  private final Mode mode;
  private final boolean isAppendBlob;
  private AbfsLease lease;

  public AppendRequestParameters(final long position,
      final int offset,
      final int length,
      final Mode mode,
      final boolean isAppendBlob,
      AbfsLease lease) {
    this.position = position;
    this.offset = offset;
    this.length = length;
    this.mode = mode;
    this.isAppendBlob = isAppendBlob;
    this.lease = lease;
  }

  public long getPosition() {
    return this.position;
  }

  public int getoffset() {
    return this.offset;
  }

  public int getLength() {
    return this.length;
  }

  public Mode getMode() {
    return this.mode;
  }

  public boolean isAppendBlob() {
    return this.isAppendBlob;
  }

  public AbfsLease getLease() {
    return this.lease;
  }
}
