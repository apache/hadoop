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
package org.apache.hadoop.ozone.ksm;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.NodeType;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;
import static org.apache.hadoop.ozone.web.util.ServerUtils.getOzoneMetaDirPath;

/**
 * KSMStorage is responsible for management of the StorageDirectories used by
 * the KSM.
 */
public class KSMStorage extends Storage {

  public static final String STORAGE_DIR = "ksm";
  public static final String KSM_ID = "ksmUuid";

  /**
   * Construct KSMStorage.
   * @throws IOException if any directories are inaccessible.
   */
  public KSMStorage(OzoneConfiguration conf) throws IOException {
    super(NodeType.KSM, getOzoneMetaDirPath(conf), STORAGE_DIR);
  }

  public void setScmId(String scmId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("KSM is already initialized.");
    } else {
      getStorageInfo().setProperty(SCM_ID, scmId);
    }
  }

  public void setKsmId(String ksmId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("KSM is already initialized.");
    } else {
      getStorageInfo().setProperty(KSM_ID, ksmId);
    }
  }

  /**
   * Retrieves the SCM ID from the version file.
   * @return SCM_ID
   */
  public String getScmId() {
    return getStorageInfo().getProperty(SCM_ID);
  }

  /**
   * Retrieves the KSM ID from the version file.
   * @return KSM_ID
   */
  public String getKsmId() {
    return getStorageInfo().getProperty(KSM_ID);
  }

  @Override
  protected Properties getNodeProperties() {
    String ksmId = getKsmId();
    if (ksmId == null) {
      ksmId = UUID.randomUUID().toString();
    }
    Properties ksmProperties = new Properties();
    ksmProperties.setProperty(KSM_ID, ksmId);
    return ksmProperties;
  }
}