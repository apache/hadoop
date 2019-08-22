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
package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Implementation of the OzoneFileSystem calls.
 */
public class OzoneClientAdapterImpl extends BasicOzoneClientAdapterImpl {

  private OzoneFSStorageStatistics storageStatistics;

  public OzoneClientAdapterImpl(String volumeStr, String bucketStr,
      OzoneFSStorageStatistics storageStatistics)
      throws IOException {
    super(volumeStr, bucketStr);
    this.storageStatistics = storageStatistics;
  }

  public OzoneClientAdapterImpl(
      OzoneConfiguration conf, String volumeStr, String bucketStr,
      OzoneFSStorageStatistics storageStatistics)
      throws IOException {
    super(conf, volumeStr, bucketStr);
    this.storageStatistics = storageStatistics;
  }

  public OzoneClientAdapterImpl(String omHost, int omPort,
      Configuration hadoopConf, String volumeStr, String bucketStr,
      OzoneFSStorageStatistics storageStatistics)
      throws IOException {
    super(omHost, omPort, hadoopConf, volumeStr, bucketStr);
    this.storageStatistics = storageStatistics;
  }

  @Override
  protected void incrementCounter(Statistic objectsRead) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(objectsRead, 1);
    }
  }
}
