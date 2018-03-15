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
package org.apache.hadoop.cblock.jscsiHelper;

import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;

import java.io.IOException;
import java.util.List;

/**
 * This class is the handler of CBlockManager used by target server
 * to communicate with CBlockManager.
 *
 * More specifically, this class will expose local methods to target
 * server, and make RPC calls to CBlockManager accordingly
 */
public class CBlockManagerHandler {

  private final CBlockClientProtocolClientSideTranslatorPB handler;

  public CBlockManagerHandler(
      CBlockClientProtocolClientSideTranslatorPB handler) {
    this.handler = handler;
  }

  public MountVolumeResponse mountVolume(
      String userName, String volumeName) throws IOException {
    return handler.mountVolume(userName, volumeName);
  }

  public List<VolumeInfo> listVolumes() throws IOException {
    return handler.listVolumes();
  }
}
