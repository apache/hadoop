/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.protocol;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.security.KerberosInfo;

/**
 * The protocol used to perform security related operations with SCM.
 */
@KerberosInfo(
    serverPrincipal = ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface SCMSecurityProtocol {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param dataNodeDetails - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  String getDataNodeCertificate(
      DatanodeDetailsProto dataNodeDetails,
      String certSignReq) throws IOException;

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails       - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException;

  /**
   * Get SCM signed certificate for given certificate serial id if it exists.
   * Throws exception if it's not found.
   *
   * @param certSerialId    - Certificate serial id.
   * @return String         - pem encoded SCM signed
   *                          certificate with given cert id if it
   *                          exists.
   */
  String getCertificate(String certSerialId) throws IOException;

  /**
   * Get CA certificate.
   *
   * @return String         - pem encoded CA certificate.
   */
  String getCACertificate() throws IOException;

}
