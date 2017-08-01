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
package org.apache.hadoop.cblock.proto;

import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.util.List;

/**
 * CBlock uses a separate command line tool to send volume management
 * operations to CBlock server, including create/delete/info/list volumes. This
 * is the protocol used by the command line tool to send these requests and get
 * responses from CBlock server.
 */
@InterfaceAudience.Private
public interface CBlockServiceProtocol {

  void createVolume(String userName, String volumeName,
      long volumeSize, int blockSize) throws IOException;

  void deleteVolume(String userName, String volumeName,
      boolean force) throws IOException;

  VolumeInfo infoVolume(String userName,
      String volumeName) throws IOException;

  List<VolumeInfo> listVolume(String userName) throws IOException;
}
