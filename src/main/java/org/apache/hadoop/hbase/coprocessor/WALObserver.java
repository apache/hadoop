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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * It's provided to have a way for coprocessors to observe, rewrite,
 * or skip WALEdits as they are being written to the WAL.
 *
 * {@link org.apache.hadoop.hbase.coprocessor.RegionObserver} provides
 * hooks for adding logic for WALEdits in the region context during reconstruction,
 *
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.regionserver.wal.HLog}.
 */
public interface WALObserver extends Coprocessor {

  /**
   * Called before a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * @param ctx
   * @param info
   * @param logKey
   * @param logEdit
   * @return true if default behavior should be bypassed, false otherwise
   * @throws IOException
   */
  boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException;

  /**
   * Called after a {@link org.apache.hadoop.hbase.regionserver.wal.WALEdit}
   * is writen to WAL.
   *
   * @param ctx
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException;
}
