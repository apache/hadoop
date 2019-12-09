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

package org.apache.hadoop.yarn.server.timeline.recovery.records;

import org.apache.hadoop.yarn.proto.YarnServerTimelineServerRecoveryProtos.TimelineDelegationTokenIdentifierDataProto;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class TimelineDelegationTokenIdentifierData {
  TimelineDelegationTokenIdentifierDataProto.Builder builder =
      TimelineDelegationTokenIdentifierDataProto.newBuilder();

  public TimelineDelegationTokenIdentifierData() {
  }

  public TimelineDelegationTokenIdentifierData(
      TimelineDelegationTokenIdentifier identifier, long renewdate) {
    builder.setTokenIdentifier(identifier.getProto());
    builder.setRenewDate(renewdate);
  }

  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }

  public byte[] toByteArray() throws IOException {
    return builder.build().toByteArray();
  }

  public TimelineDelegationTokenIdentifier getTokenIdentifier()
      throws IOException {
    ByteArrayInputStream in =
        new ByteArrayInputStream(builder.getTokenIdentifier().toByteArray());
    TimelineDelegationTokenIdentifier identifer =
        new TimelineDelegationTokenIdentifier();
    identifer.readFields(new DataInputStream(in));
    return identifer;
  }

  public long getRenewDate() {
    return builder.getRenewDate();
  }
}
