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
 * The following hooks can be implemented for instrumentation/fault
 * injection.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FileIoEvents {

  /**
   * Invoked before a filesystem metadata operation.
   *
   * @param volume  target volume for the operation. Null if unavailable.
   * @param op  type of operation.
   * @return  timestamp at which the operation was started. 0 if
   *          unavailable.
   */
  abstract long beforeMetadataOp(@Nullable FsVolumeSpi volume, OPERATION op);

  /**
   * Invoked after a filesystem metadata operation has completed.
   *
   * @param volume  target volume for the operation.  Null if unavailable.
   * @param op  type of operation.
   * @param begin  timestamp at which the operation was started. 0
   *               if unavailable.
   */
  abstract void afterMetadataOp(@Nullable FsVolumeSpi volume, OPERATION op,
                                long begin);

  /**
   * Invoked before a read/write/flush/channel transfer operation.
   *
   * @param volume  target volume for the operation. Null if unavailable.
   * @param op  type of operation.
   * @param len  length of the file IO. 0 for flush.
   * @return  timestamp at which the operation was started. 0 if
   *          unavailable.
   */
  abstract long beforeFileIo(@Nullable FsVolumeSpi volume, OPERATION op,
                             long len);


  /**
   * Invoked after a read/write/flush/channel transfer operation
   * has completed.
   *
   * @param volume  target volume for the operation. Null if unavailable.
   * @param op  type of operation.
   * @param len   of the file IO. 0 for flush.
   * @return  timestamp at which the operation was started. 0 if
   *          unavailable.
   */
  abstract void afterFileIo(@Nullable FsVolumeSpi volume, OPERATION op,
                            long begin, long len);

  /**
   * Invoked if an operation fails with an exception.
   * @param volume  target volume for the operation. Null if unavailable.
   * @param op  type of operation.
   * @param e  Exception encountered during the operation.
   * @param begin  time at which the operation was started.
   */
  abstract void onFailure(
      @Nullable FsVolumeSpi volume, OPERATION op, Exception e, long begin);

  /**
   * Invoked by FileIoProvider if an operation fails with an exception.
   * @param datanode datanode that runs volume check upon volume io failure
   * @param volume  target volume for the operation. Null if unavailable.
   * @param op  type of operation.
   * @param e  Exception encountered during the operation.
   * @param begin  time at which the operation was started.
   */
  void onFailure(DataNode datanode,
      @Nullable FsVolumeSpi volume, OPERATION op, Exception e, long begin) {
    onFailure(volume, op, e, begin);
    if (datanode != null && volume != null) {
      datanode.checkDiskErrorAsync(volume);
    }
  }

  /**
   * Return statistics as a JSON string.
   * @return
   */
  @Nullable abstract String getStatistics();
}
