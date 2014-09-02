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

package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MissingEventsException extends Exception {
  private static final long serialVersionUID = 1L;

  private long expectedTxid;
  private long actualTxid;

  public MissingEventsException() {}

  public MissingEventsException(long expectedTxid, long actualTxid) {
    this.expectedTxid = expectedTxid;
    this.actualTxid = actualTxid;
  }

  public long getExpectedTxid() {
    return expectedTxid;
  }

  public long getActualTxid() {
    return actualTxid;
  }

  @Override
  public String toString() {
    return "We expected the next batch of events to start with transaction ID "
        + expectedTxid + ", but it instead started with transaction ID " +
        actualTxid + ". Most likely the intervening transactions were cleaned "
        + "up as part of checkpointing.";
  }
}
