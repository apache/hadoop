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
package org.apache.hadoop.hdfs.server.namenode;

import java.net.InetAddress;
import java.security.Principal;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

/**
 * Interface defining an audit logger.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AuditLogger {

  /**
   * Called during initialization of the logger.
   *
   * @param conf The configuration object.
   */
  void initialize(Configuration conf);

  /**
   * Called to log an audit event.
   * <p>
   * This method must return as quickly as possible, since it's called
   * in a critical section of the NameNode's operation.
   *
   * @param succeeded Whether authorization succeeded.
   * @param userName Name of the user executing the request.
   * @param addr Remote address of the request.
   * @param cmd The requested command.
   * @param src Path of affected source file.
   * @param dst Path of affected destination file (if any).
   * @param stat File information for operations that change the file's
   *             metadata (permissions, owner, times, etc).
   */
  void logAuditEvent(boolean succeeded, String userName,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat);

}
