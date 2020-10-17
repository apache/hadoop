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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;

/**
 * This class is to be used as a builder for {@link FsVolumeImpl} objects.
 */
public class FsVolumeImplBuilder {

  private FsDatasetImpl dataset;
  private String storageID;
  private StorageDirectory sd;
  private Configuration conf;
  private FileIoProvider fileIoProvider;
  private DF usage;

  public FsVolumeImplBuilder() {
    dataset = null;
    storageID = null;
    sd = null;
    conf = null;
    usage = null;
  }

  FsVolumeImplBuilder setDataset(FsDatasetImpl dataset) {
    this.dataset = dataset;
    return this;
  }

  FsVolumeImplBuilder setStorageID(String id) {
    this.storageID = id;
    return this;
  }

  FsVolumeImplBuilder setStorageDirectory(StorageDirectory sd) {
    this.sd = sd;
    return this;
  }

  FsVolumeImplBuilder setConf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  FsVolumeImplBuilder setFileIoProvider(FileIoProvider fileIoProvider) {
    this.fileIoProvider = fileIoProvider;
    return this;
  }

  @VisibleForTesting
  FsVolumeImplBuilder setUsage(DF newUsage) {
    this.usage = newUsage;
    return this;
  }

  FsVolumeImpl build() throws IOException {
    if (sd.getStorageLocation().getStorageType() == StorageType.PROVIDED) {
      return new ProvidedVolumeImpl(dataset, storageID, sd,
          fileIoProvider != null ? fileIoProvider :
            new FileIoProvider(null, null), conf);
    }
    if (null == usage) {
      // set usage unless overridden by unit tests
      usage = new DF(sd.getCurrentDir().getParentFile(), conf);
    }
    return new FsVolumeImpl(
        dataset, storageID, sd,
        fileIoProvider != null ? fileIoProvider :
            new FileIoProvider(null, null), conf, usage);
  }
}
