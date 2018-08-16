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
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class OzoneTestUtils {

  /**
   * Close containers which contain the blocks listed in
   * omKeyLocationInfoGroups.
   *
   * @param omKeyLocationInfoGroups locationInfos for a key.
   * @param scm StorageContainerManager instance.
   * @return true if close containers is successful.
   * @throws IOException
   */
  public static boolean closeContainers(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups,
      StorageContainerManager scm) throws IOException {
    return performOperationOnKeyContainers((blockID) -> {
      try {
        scm.getScmContainerManager()
            .updateContainerState(blockID.getContainerID(),
                HddsProtos.LifeCycleEvent.FINALIZE);
        scm.getScmContainerManager()
            .updateContainerState(blockID.getContainerID(),
                HddsProtos.LifeCycleEvent.CLOSE);
        Assert.assertFalse(scm.getScmContainerManager()
            .getContainerWithPipeline(blockID.getContainerID())
            .getContainerInfo().isContainerOpen());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }, omKeyLocationInfoGroups);
  }

  /**
   * Performs the provided consumer on containers which contain the blocks
   * listed in omKeyLocationInfoGroups.
   *
   * @param consumer Consumer which accepts BlockID as argument.
   * @param omKeyLocationInfoGroups locationInfos for a key.
   * @return true if consumer is successful.
   * @throws IOException
   */
  public static boolean performOperationOnKeyContainers(
      Consumer<BlockID> consumer,
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws IOException {

    try {
      for (OmKeyLocationInfoGroup omKeyLocationInfoGroup :
          omKeyLocationInfoGroups) {
        List<OmKeyLocationInfo> omKeyLocationInfos =
            omKeyLocationInfoGroup.getLocationList();
        for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfos) {
          BlockID blockID = omKeyLocationInfo.getBlockID();
          consumer.accept(blockID);
        }
      }
    } catch (Error e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

}
