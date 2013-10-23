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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.StorageType;

/**
 * This is an interface for the underlying volume.
 */
public interface FsVolumeSpi {
  /** @return the StorageUuid of the volume */
  public String getStorageID();

  /** @return a list of block pools. */
  public String[] getBlockPoolList();

  /** @return the available storage space in bytes. */
  public long getAvailable() throws IOException;

  /** @return the base path to the volume */
  public String getBasePath();

  /** @return the path to the volume */
  public String getPath(String bpid) throws IOException;

  /** @return the directory for the finalized blocks in the block pool. */
  public File getFinalizedDir(String bpid) throws IOException;
  
  public StorageType getStorageType();
}