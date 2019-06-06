/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request;

import java.util.Map;

import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Interface for OM Requests to convert to audit objects.
 */
public interface RequestAuditor {

  /**
   * Build AuditMessage.
   * @param op
   * @param auditMap
   * @param throwable
   * @param userInfo
   * @return
   */
  AuditMessage buildAuditMessage(AuditAction op,
      Map<String, String> auditMap, Throwable throwable,
      OzoneManagerProtocolProtos.UserInfo userInfo);

  /**
   * Build auditMap with specified volume.
   * @param volume
   * @return auditMap.
   */
  Map<String, String> buildVolumeAuditMap(String volume);
}
