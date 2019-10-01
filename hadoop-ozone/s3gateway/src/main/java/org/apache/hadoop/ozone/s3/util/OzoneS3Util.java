/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.s3.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.security.SecurityUtil;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * Ozone util for S3 related operations.
 */
public final class OzoneS3Util {

  private OzoneS3Util() {
  }

  public static String getVolumeName(String userName) {
    Objects.requireNonNull(userName);
    return DigestUtils.md5Hex(userName);
  }

  /**
   * Generate service Name for token.
   * @param configuration
   * @param serviceId - ozone manager service ID
   * @param omNodeIds - list of node ids for the given OM service.
   * @return service Name.
   */
  public static String buildServiceNameForToken(
      @Nonnull OzoneConfiguration configuration, @Nonnull String serviceId,
      @Nonnull Collection<String> omNodeIds) {
    StringBuilder rpcAddress = new StringBuilder();

    int nodesLength = omNodeIds.size();
    int counter = 0;
    for (String nodeId : omNodeIds) {
      counter++;
      String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          serviceId, nodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(configuration, rpcAddrKey);
      if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
        throw new IllegalArgumentException("Could not find rpcAddress for " +
            OZONE_OM_ADDRESS_KEY + "." + serviceId + "." + nodeId);
      }

      if (counter != nodesLength) {
        rpcAddress.append(SecurityUtil.buildTokenService(
            NetUtils.createSocketAddr(rpcAddrStr)) + ",");
      } else {
        rpcAddress.append(SecurityUtil.buildTokenService(
            NetUtils.createSocketAddr(rpcAddrStr)));
      }
    }
    return rpcAddress.toString();
  }
}
