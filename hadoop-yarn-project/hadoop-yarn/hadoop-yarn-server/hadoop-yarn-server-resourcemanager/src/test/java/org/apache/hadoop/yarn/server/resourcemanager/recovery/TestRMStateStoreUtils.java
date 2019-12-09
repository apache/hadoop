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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestRMStateStoreUtils {

  @Test
  public void testReadRMDelegationTokenIdentifierData()
      throws Exception {
    testReadRMDelegationTokenIdentifierData(false);
  }

  @Test
  public void testReadRMDelegationTokenIdentifierDataOldFormat()
      throws Exception {
    testReadRMDelegationTokenIdentifierData(true);
  }

  public void testReadRMDelegationTokenIdentifierData(boolean oldFormat)
      throws Exception {
    RMDelegationTokenIdentifier token = new RMDelegationTokenIdentifier(
        new Text("alice"), new Text("bob"), new Text("colin"));
    token.setIssueDate(123);
    token.setMasterKeyId(321);
    token.setMaxDate(314);
    token.setSequenceNumber(12345);
    DataInputBuffer inBuf = new DataInputBuffer();
    if (oldFormat) {
      DataOutputBuffer outBuf = new DataOutputBuffer();
      token.writeInOldFormat(outBuf);
      outBuf.writeLong(42);   // renewDate
      inBuf.reset(outBuf.getData(), 0, outBuf.getLength());
    } else {
      RMDelegationTokenIdentifierData tokenIdentifierData
          = new RMDelegationTokenIdentifierData(token, 42);
      byte[] data = tokenIdentifierData.toByteArray();
      inBuf.reset(data, 0, data.length);
    }

    RMDelegationTokenIdentifierData identifierData
        = RMStateStoreUtils.readRMDelegationTokenIdentifierData(inBuf);
    assertEquals("Found unexpected data still in the InputStream",
        -1, inBuf.read());

    RMDelegationTokenIdentifier identifier
        = identifierData.getTokenIdentifier();
    assertEquals("alice", identifier.getUser().getUserName());
    assertEquals(new Text("bob"), identifier.getRenewer());
    assertEquals("colin", identifier.getUser().getRealUser().getUserName());
    assertEquals(123, identifier.getIssueDate());
    assertEquals(321, identifier.getMasterKeyId());
    assertEquals(314, identifier.getMaxDate());
    assertEquals(12345, identifier.getSequenceNumber());
    assertEquals(42, identifierData.getRenewDate());
  }
}
