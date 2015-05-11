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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.ipc.RemoteException;

/**
 * ReportBadBlockAction is an instruction issued by {{BPOfferService}} to
 * {{BPServiceActor}} to report bad block to namenode
 *
 */
public class ReportBadBlockAction implements BPServiceActorAction {

  private final ExtendedBlock block;
  private final String storageUuid;
  private final StorageType storageType;

  public ReportBadBlockAction(ExtendedBlock block, String storageUuid, 
      StorageType storageType) {
    this.block = block;
    this.storageUuid = storageUuid;
    this.storageType = storageType;
  }

  @Override
  public void reportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode, 
    DatanodeRegistration bpRegistration) throws BPServiceActorActionException {
    if (bpRegistration == null) {
      return;
    }
    DatanodeInfo[] dnArr = { new DatanodeInfo(bpRegistration) };
    String[] uuids = { storageUuid };
    StorageType[] types = { storageType };
    LocatedBlock[] locatedBlock = { new LocatedBlock(block,
        dnArr, uuids, types) };

    try {
      bpNamenode.reportBadBlocks(locatedBlock);
    } catch (RemoteException re) {
      DataNode.LOG.info("reportBadBlock encountered RemoteException for "
          + "block:  " + block , re);
    } catch (IOException e) {
      throw new BPServiceActorActionException("Failed to report bad block "
          + block + " to namenode: ");
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((block == null) ? 0 : block.hashCode());
    result = prime * result
        + ((storageType == null) ? 0 : storageType.hashCode());
    result = prime * result
        + ((storageUuid == null) ? 0 : storageUuid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !(obj instanceof ReportBadBlockAction)) {
      return false;
    }
    ReportBadBlockAction other = (ReportBadBlockAction) obj;
    if (block == null) {
      if (other.block != null) {
        return false;
      }
    } else if (!block.equals(other.block)) {
      return false;
    }
    if (storageType != other.storageType) {
      return false;
    }
    if (storageUuid == null) {
      if (other.storageUuid != null) {
        return false;
      }
    } else if (!storageUuid.equals(other.storageUuid)) {
      return false;
    }
    return true;
  }
}
