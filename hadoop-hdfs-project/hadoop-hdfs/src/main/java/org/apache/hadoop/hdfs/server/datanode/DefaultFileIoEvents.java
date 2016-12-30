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

package org.apache.hadoop.hdfs.server.datanode;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider.OPERATION;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import javax.annotation.Nullable;

/**
 * The default implementation of {@link FileIoEvents} that do nothing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DefaultFileIoEvents extends FileIoEvents {
  @Override
  public long beforeMetadataOp(
      @Nullable FsVolumeSpi volume, OPERATION op) {
    return 0;
  }

  @Override
  public void afterMetadataOp(
      @Nullable FsVolumeSpi volume, OPERATION op, long begin) {
  }

  @Override
  public long beforeFileIo(
      @Nullable FsVolumeSpi volume, OPERATION op, long len) {
    return 0;
  }

  @Override
  public void afterFileIo(
      @Nullable FsVolumeSpi volume, OPERATION op, long begin, long len) {
  }

  @Override
  public void onFailure(
      @Nullable FsVolumeSpi volume, OPERATION op, Exception e, long begin) {
  }

  @Override
  public @Nullable String getStatistics() {
    // null is valid JSON.
    return null;
  }
}
