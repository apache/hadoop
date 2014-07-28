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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * Class captures information of a datanode and its storages.
 */
public class DatanodeStorageReport {
  final DatanodeInfo datanodeInfo;
  final StorageReport[] storageReports;

  public DatanodeStorageReport(DatanodeInfo datanodeInfo,
      StorageReport[] storageReports) {
    this.datanodeInfo = datanodeInfo;
    this.storageReports = storageReports;
  }

  public DatanodeInfo getDatanodeInfo() {
    return datanodeInfo;
  }

  public StorageReport[] getStorageReports() {
    return storageReports;
  }
}