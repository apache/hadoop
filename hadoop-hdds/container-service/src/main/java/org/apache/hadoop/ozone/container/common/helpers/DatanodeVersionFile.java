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

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.ozone.OzoneConsts;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

/**
 * This is a utility class which helps to create the version file on datanode
 * and also validate the content of the version file.
 */
public class DatanodeVersionFile {

  private final String storageId;
  private final String clusterId;
  private final String datanodeUuid;
  private final long cTime;
  private final int layOutVersion;

  public DatanodeVersionFile(String storageId, String clusterId,
      String datanodeUuid, long cTime, int layOutVersion) {
    this.storageId = storageId;
    this.clusterId = clusterId;
    this.datanodeUuid = datanodeUuid;
    this.cTime = cTime;
    this.layOutVersion = layOutVersion;
  }

  private Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(OzoneConsts.STORAGE_ID, storageId);
    properties.setProperty(OzoneConsts.CLUSTER_ID, clusterId);
    properties.setProperty(OzoneConsts.DATANODE_UUID, datanodeUuid);
    properties.setProperty(OzoneConsts.CTIME, String.valueOf(cTime));
    properties.setProperty(OzoneConsts.LAYOUTVERSION, String.valueOf(
        layOutVersion));
    return properties;
  }

  /**
   * Creates a version File in specified path.
   * @param path
   * @throws IOException
   */
  public void createVersionFile(File path) throws
      IOException {
    try (RandomAccessFile file = new RandomAccessFile(path, "rws");
         FileOutputStream out = new FileOutputStream(file.getFD())) {
      file.getChannel().truncate(0);
      Properties properties = createProperties();
      /*
       * If server is interrupted before this line,
       * the version file will remain unchanged.
       */
      properties.store(out, null);
    }
  }


  /**
   * Creates a property object from the specified file content.
   * @param  versionFile
   * @return Properties
   * @throws IOException
   */
  public static Properties readFrom(File versionFile) throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(versionFile, "rws");
         FileInputStream in = new FileInputStream(file.getFD())) {
      Properties props = new Properties();
      props.load(in);
      return props;
    }
  }
}
