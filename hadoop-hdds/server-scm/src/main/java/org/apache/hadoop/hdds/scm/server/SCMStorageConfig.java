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
package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.common.Storage;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;
import static org.apache.hadoop.ozone.OzoneConsts.STORAGE_DIR;

/**
 * SCMStorageConfig is responsible for management of the
 * StorageDirectories used by the SCM.
 */
public class SCMStorageConfig extends Storage {

  /**
   * Construct SCMStorageConfig.
   * @throws IOException if any directories are inaccessible.
   */
  public SCMStorageConfig(OzoneConfiguration conf) throws IOException {
    super(NodeType.SCM, ServerUtils.getScmDbDir(conf), STORAGE_DIR);
  }

  public void setScmId(String scmId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("SCM is already initialized.");
    } else {
      getStorageInfo().setProperty(SCM_ID, scmId);
    }
  }

  /**
   * Retrieves the SCM ID from the version file.
   * @return SCM_ID
   */
  public String getScmId() {
    return getStorageInfo().getProperty(SCM_ID);
  }

  @Override
  protected Properties getNodeProperties() {
    String scmId = getScmId();
    if (scmId == null) {
      scmId = UUID.randomUUID().toString();
    }
    Properties scmProperties = new Properties();
    scmProperties.setProperty(SCM_ID, scmId);
    return scmProperties;
  }

}
