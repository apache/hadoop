/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

/**
 * Handler to handle OM requests in OM HA.
 */
public interface OzoneManagerHARequestHandler extends RequestHandler {

  /**
   * Handle start Transaction Requests from OzoneManager StateMachine.
   * @param omRequest
   * @return OMRequest - New OM Request which will be applied during apply
   * Transaction
   * @throws IOException
   */
  OMRequest handleStartTransaction(OMRequest omRequest) throws IOException;

  /**
   * Handle Apply Transaction Requests from OzoneManager StateMachine.
   * @param omRequest
   * @return OMResponse
   */
  OMResponse handleApplyTransaction(OMRequest omRequest);

}
