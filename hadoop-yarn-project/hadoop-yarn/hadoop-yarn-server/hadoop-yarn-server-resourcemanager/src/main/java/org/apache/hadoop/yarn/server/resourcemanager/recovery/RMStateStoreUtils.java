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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Utility methods for {@link RMStateStore} and subclasses.
 */
@Private
@Unstable
public class RMStateStoreUtils {

  public static final Logger LOG =
      LoggerFactory.getLogger(RMStateStoreUtils.class);

  /**
   * Returns the RM Delegation Token data from the {@link DataInputStream} as a
   * {@link RMDelegationTokenIdentifierData}.  It can handle both the current
   * and old (non-protobuf) formats.
   *
   * @param fsIn The {@link DataInputStream} containing RM Delegation Token data
   * @return An {@link RMDelegationTokenIdentifierData} containing the read in
   * RM Delegation Token
   */
  public static RMDelegationTokenIdentifierData
      readRMDelegationTokenIdentifierData(DataInputStream fsIn)
      throws IOException {
    RMDelegationTokenIdentifierData identifierData =
        new RMDelegationTokenIdentifierData();
    try {
      identifierData.readFields(fsIn);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Recovering old formatted token");
      fsIn.reset();
      YARNDelegationTokenIdentifier identifier =
          new RMDelegationTokenIdentifier();
      identifier.readFieldsInOldFormat(fsIn);
      identifierData.setIdentifier(identifier);
      identifierData.setRenewDate(fsIn.readLong());
    }
    return identifierData;
  }
}
