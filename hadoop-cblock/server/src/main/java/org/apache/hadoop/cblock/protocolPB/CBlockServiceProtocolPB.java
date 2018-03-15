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

import org.apache.hadoop.cblock.protocol.proto.CBlockServiceProtocolProtos;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * Users use a independent command line tool to talk to CBlock server for
 * volume operations (create/delete/info/list). This is the protocol used by
 * the the command line tool to send these requests to CBlock server.
 */
@ProtocolInfo(protocolName =
    "org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocol",
    protocolVersion = 1)
@InterfaceAudience.Private
public interface CBlockServiceProtocolPB extends
    CBlockServiceProtocolProtos.CBlockServiceProtocolService.BlockingInterface {
}
