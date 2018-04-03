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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeResponse;

/**
 * Interface of managing the Router state.
 */
public interface RouterStateManager {
  /**
   * Enter safe mode and change Router state to RouterServiceState#SAFEMODE.
   */
  EnterSafeModeResponse enterSafeMode(EnterSafeModeRequest request)
      throws IOException;

  /**
   * Leave safe mode and change Router state to RouterServiceState#RUNNING.
   */
  LeaveSafeModeResponse leaveSafeMode(LeaveSafeModeRequest request)
      throws IOException;

  /**
   * Verify if current Router state is safe mode.
   */
  GetSafeModeResponse getSafeMode(GetSafeModeRequest request)
      throws IOException;
}
