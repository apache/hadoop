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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injects faults in the metadata and data related operations on datanode
 * volumes.
 */
@InterfaceAudience.Private
public class FaultInjectorFileIoEvents {

  public static final class InjectedFileIOFaultException extends Exception {

    private static final long serialVersionUID = 1L;

    private InjectedFileIOFaultException() {
      super("Fault injected by configuration");
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(
      FaultInjectorFileIoEvents.class);

  private final boolean isEnabled;
  private final Set<FileIoProvider.OPERATION> configuredOps;
  private final int faultRangeMax;

  public FaultInjectorFileIoEvents(@Nullable Configuration conf) {
    if (conf != null) {
      isEnabled = conf.getBoolean(
          DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY,
          DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_DEFAULT);
    } else {
      isEnabled = false;
    }
    configuredOps = new HashSet<>();
    if (isEnabled) {
      String ops = conf.get(
          DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY);
      if (ops != null) {
        String[] parts = ops.split(",");
        for (String part : parts) {
          String opName = part.trim().toUpperCase();
          try {
            configuredOps.add(FileIoProvider.OPERATION.valueOf(opName));
          } catch (IllegalArgumentException e) {
            LOG.warn("Value '{}' is not valid FileIoProvider.OPERATION, "
                + "ignoring...", opName);
          }
        }
      }
      int faultPercentagePropVal = Math.min(conf.getInt(
          DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY, 0), 100);
      faultRangeMax = (int) ((double) faultPercentagePropVal / 100 *
          Integer.MAX_VALUE);
      LOG.warn("FaultInjectorFileIoEvents is enabled and will fail the "
          + "following operations: {}", configuredOps);
      LOG.warn(" *** DO NOT USE IN PRODUCTION!!! ***");
    } else {
      faultRangeMax = 0;
    }
  }

  @VisibleForTesting
  boolean isEnabled() {
    return isEnabled;
  }

  @VisibleForTesting
  Set<FileIoProvider.OPERATION> getOperations() {
    return configuredOps;
  }

  @VisibleForTesting
  int getFaultRangeMax() {
    return faultRangeMax;
  }

  private void fault(FileIoProvider.OPERATION op)
      throws InjectedFileIOFaultException {
    if (isEnabled && faultRangeMax > 0 && configuredOps.contains(op)
        && ThreadLocalRandom.current().nextInt() < faultRangeMax) {
      LOG.error("Throwing fault for operation: " + op);
      throw new InjectedFileIOFaultException();
    }
  }

  public void beforeMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op) throws InjectedFileIOFaultException {
    fault(op);
  }

  public void beforeFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long len)
          throws InjectedFileIOFaultException {
    fault(op);
  }
}
