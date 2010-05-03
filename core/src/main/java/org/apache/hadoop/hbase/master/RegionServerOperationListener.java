/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HServerInfo;

/**
 * Listener for regionserver events in master.
 * @see HMaster#registerRegionServerOperationListener(RegionServerOperationListener)
 * @see HMaster#unregisterRegionServerOperationListener(RegionServerOperationListener)
 */
public interface RegionServerOperationListener {
  /**
   * Called for each message passed the master.  Most of the messages that come
   * in here will go on to become {@link #process(RegionServerOperation)}s but
   * others like {@linke HMsg.Type#MSG_REPORT_PROCESS_OPEN} go no further;
   * only in here can you see them come in.
   * @param serverInfo Server we got the message from.
   * @param incomingMsg The message received.
   * @return True to continue processing, false to skip.
   */
  public boolean process(final HServerInfo serverInfo,
      final HMsg incomingMsg);

  /**
   * Called before processing <code>op</code>
   * @param op
   * @return True if we are to proceed w/ processing.
   * @exception IOException
   */
  public boolean process(final RegionServerOperation op) throws IOException;

  /**
   * Called after <code>op</code> has been processed.
   * @param op The operation that just completed.
   */
  public void processed(final RegionServerOperation op);
}
