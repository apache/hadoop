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
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.common.Storage;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;

/**
 * OMStorage is responsible for management of the StorageDirectories used by
 * the Ozone Manager.
 */
public class OMStorage extends Storage {

  public static final String STORAGE_DIR = "om";
  public static final String OM_ID = "omUuid";
  public static final String OM_CERT_SERIAL_ID = "omCertSerialId";

  /**
   * Construct OMStorage.
   * @throws IOException if any directories are inaccessible.
   */
  public OMStorage(OzoneConfiguration conf) throws IOException {
    super(NodeType.OM, OmUtils.getOmDbDir(conf), STORAGE_DIR);
  }

  public void setScmId(String scmId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("OM is already initialized.");
    } else {
      getStorageInfo().setProperty(SCM_ID, scmId);
    }
  }

  public void setOmCertSerialId(String certSerialId) throws IOException {
    getStorageInfo().setProperty(OM_CERT_SERIAL_ID, certSerialId);
  }

  public void setOmId(String omId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException("OM is already initialized.");
    } else {
      getStorageInfo().setProperty(OM_ID, omId);
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
   * Retrieves the OM ID from the version file.
   * @return OM_ID
   */
  public String getOmId() {
    return getStorageInfo().getProperty(OM_ID);
  }

  /**
   * Retrieves the serial id of certificate issued by SCM.
   * @return OM_ID
   */
  public String getOmCertSerialId() {
    return getStorageInfo().getProperty(OM_CERT_SERIAL_ID);
  }

  @Override
  protected Properties getNodeProperties() {
    String omId = getOmId();
    if (omId == null) {
      omId = UUID.randomUUID().toString();
    }
    Properties omProperties = new Properties();
    omProperties.setProperty(OM_ID, omId);

    if (getOmCertSerialId() != null) {
      omProperties.setProperty(OM_CERT_SERIAL_ID, getOmCertSerialId());
    }
    return omProperties;
  }
}