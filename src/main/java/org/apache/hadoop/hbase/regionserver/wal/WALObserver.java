/*
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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Get notification of {@link HLog}/WAL log events. The invocations are inline
 * so make sure your implementation is fast else you'll slow hbase.
 */
public interface WALObserver {
  /**
   * The WAL was rolled.
   * @param newFile the path to the new hlog
   */
  public void logRolled(Path newFile);

  /**
   * A request was made that the WAL be rolled.
   */
  public void logRollRequested();

  /**
   * The WAL is about to close.
   */
  public void logCloseRequested();

  /**
  * Called before each write.
  * @param info
  * @param logKey
  * @param logEdit
  */
 public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
   WALEdit logEdit);

  /**
   *
   * @param htd
   * @param logKey
   * @param logEdit
   */
 public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey,
   WALEdit logEdit);

}
