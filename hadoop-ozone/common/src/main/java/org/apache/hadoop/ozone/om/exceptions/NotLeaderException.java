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

package org.apache.hadoop.ozone.om.exceptions;

import java.io.IOException;

/**
 * Exception thrown by
 * {@link org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB} when
 * a read request is received by a non leader OM node.
 */
public class NotLeaderException extends IOException {

  private final String currentPeerId;
  private final String leaderPeerId;

  public NotLeaderException(String currentPeerIdStr) {
    super("OM " + currentPeerIdStr + " is not the leader. Could not " +
        "determine the leader node.");
    this.currentPeerId = currentPeerIdStr;
    this.leaderPeerId = null;
  }

  public NotLeaderException(String currentPeerIdStr,
      String suggestedLeaderPeerIdStr) {
    super("OM " + currentPeerIdStr + " is not the leader. Suggested leader is "
        + suggestedLeaderPeerIdStr);
    this.currentPeerId = currentPeerIdStr;
    this.leaderPeerId = suggestedLeaderPeerIdStr;
  }

  public String getSuggestedLeaderNodeId() {
    return leaderPeerId;
  }
}
