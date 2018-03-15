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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock.protocolPB;

import org.apache.hadoop.cblock.protocol.proto.CBlockClientServerProtocolProtos;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * This is the protocol CBlock client uses to talk to CBlock server.
 * CBlock client is the mounting point of a volume. When a user mounts a
 * volume, the cBlock client running on the local node will use this protocol
 * to talk to CBlock server to mount the volume.
 */
@ProtocolInfo(protocolName =
    "org.apache.hadoop.cblock.protocolPB.CBlockClientServerProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
public interface CBlockClientServerProtocolPB extends
    CBlockClientServerProtocolProtos
        .CBlockClientServerProtocolService.BlockingInterface {
}
