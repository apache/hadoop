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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.StorageType;

/**
 * Volume for storing replicas in memory. These can be deleted at any time
 * to make space for new replicas and there is no persistence guarantee.
 *
 * The backing store for these replicas is expected to be RAM_DISK.
 * The backing store may be disk when testing.
 *
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
@VisibleForTesting
public class FsTransientVolumeImpl extends FsVolumeImpl {


  FsTransientVolumeImpl(FsDatasetImpl dataset, String storageID, File currentDir,
      Configuration conf, StorageType storageType)
          throws IOException {
    super(dataset, storageID, currentDir, conf, storageType);
  }

  @Override
  protected ThreadPoolExecutor initializeCacheExecutor(File parent) {
    // Can't 'cache' replicas already in RAM.
    return null;
  }

  @Override
  public boolean isTransientStorage() {
    return true;
  }
}
