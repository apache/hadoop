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
package org.apache.hadoop.tracing;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol interface that provides tracing.
 */
@KerberosInfo(
    serverPrincipal=CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TraceAdminProtocol {
  public static final long versionID = 1L;

  /**
   * List the currently active trace span receivers.
   * 
   * @throws IOException        On error.
   */
  @Idempotent
  public SpanReceiverInfo[] listSpanReceivers() throws IOException;

  /**
   * Add a new trace span receiver.
   * 
   * @param desc                The span receiver description.
   * @return                    The ID of the new trace span receiver.
   *
   * @throws IOException        On error.
   */
  @AtMostOnce
  public long addSpanReceiver(SpanReceiverInfo desc) throws IOException;

  /**
   * Remove a trace span receiver.
   *
   * @param spanReceiverId      The id of the span receiver to remove.
   * @throws IOException        On error.
   */
  @AtMostOnce
  public void removeSpanReceiver(long spanReceiverId) throws IOException;
}
