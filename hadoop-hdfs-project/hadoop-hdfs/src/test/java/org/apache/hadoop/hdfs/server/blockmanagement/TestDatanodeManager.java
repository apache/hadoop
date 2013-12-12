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

package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.junit.Test;
import org.mockito.Mockito;
import org.mortbay.log.Log;

import static org.junit.Assert.*;

public class TestDatanodeManager {

  //The number of times the registration / removal of nodes should happen
  final int NUM_ITERATIONS = 500;

  /**
   * This test sends a random sequence of node registrations and node removals
   * to the DatanodeManager (of nodes with different IDs and versions), and
   * checks that the DatanodeManager keeps a correct count of different software
   * versions at all times.
   */
  @Test
  public void testNumVersionsReportedCorrect() throws IOException {
    //Create the DatanodeManager which will be tested
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    Mockito.when(fsn.hasWriteLock()).thenReturn(true);
    DatanodeManager dm = new DatanodeManager(Mockito.mock(BlockManager.class),
      fsn, new Configuration());

    //Seed the RNG with a known value so test failures are easier to reproduce
    Random rng = new Random();
    int seed = rng.nextInt();
    rng = new Random(seed);
    Log.info("Using seed " + seed + " for testing");

    //A map of the Storage IDs to the DN registration it was registered with
    HashMap <String, DatanodeRegistration> sIdToDnReg =
      new HashMap<String, DatanodeRegistration>();

    for(int i=0; i<NUM_ITERATIONS; ++i) {

      //If true, remove a node for every 3rd time (if there's one)
      if(rng.nextBoolean() && i%3 == 0 && sIdToDnReg.size()!=0) {
        //Pick a random node.
        int randomIndex = rng.nextInt() % sIdToDnReg.size();
        //Iterate to that random position 
        Iterator<Map.Entry<String, DatanodeRegistration>> it =
          sIdToDnReg.entrySet().iterator();
        for(int j=0; j<randomIndex-1; ++j) {
          it.next();
        }
        DatanodeRegistration toRemove = it.next().getValue();
        Log.info("Removing node " + toRemove.getDatanodeUuid() + " ip " +
        toRemove.getXferAddr() + " version : " + toRemove.getSoftwareVersion());

        //Remove that random node
        dm.removeDatanode(toRemove);
        it.remove();
      }

      // Otherwise register a node. This node may be a new / an old one
      else {
        //Pick a random storageID to register.
        String storageID = "someStorageID" + rng.nextInt(5000);

        DatanodeRegistration dr = Mockito.mock(DatanodeRegistration.class);
        Mockito.when(dr.getDatanodeUuid()).thenReturn(storageID);

        //If this storageID had already been registered before
        if(sIdToDnReg.containsKey(storageID)) {
          dr = sIdToDnReg.get(storageID);
          //Half of the times, change the IP address
          if(rng.nextBoolean()) {
            dr.setIpAddr(dr.getIpAddr() + "newIP");
          }
        } else { //This storageID has never been registered
          //Ensure IP address is unique to storageID
          String ip = "someIP" + storageID;
          Mockito.when(dr.getIpAddr()).thenReturn(ip);
          Mockito.when(dr.getXferAddr()).thenReturn(ip + ":9000");
          Mockito.when(dr.getXferPort()).thenReturn(9000);
        }
        //Pick a random version to register with
        Mockito.when(dr.getSoftwareVersion()).thenReturn(
          "version" + rng.nextInt(5));

        Log.info("Registering node storageID: " + dr.getDatanodeUuid() +
          ", version: " + dr.getSoftwareVersion() + ", IP address: "
          + dr.getXferAddr());

        //Register this random node
        dm.registerDatanode(dr);
        sIdToDnReg.put(storageID, dr);
      }

      //Verify DatanodeManager still has the right count
      Map<String, Integer> mapToCheck = dm.getDatanodesSoftwareVersions();

      //Remove counts from versions and make sure that after removing all nodes
      //mapToCheck is empty
      for(Entry<String, DatanodeRegistration> it: sIdToDnReg.entrySet()) {
        String ver = it.getValue().getSoftwareVersion();
        if(!mapToCheck.containsKey(ver)) {
          throw new AssertionError("The correct number of datanodes of a "
            + "version was not found on iteration " + i);
        }
        mapToCheck.put(ver, mapToCheck.get(ver) - 1);
        if(mapToCheck.get(ver) == 0) {
          mapToCheck.remove(ver);
        }
      }
      for(Entry <String, Integer> entry: mapToCheck.entrySet()) {
        Log.info("Still in map: " + entry.getKey() + " has "
          + entry.getValue());
      }
      assertEquals("The map of version counts returned by DatanodeManager was"
        + " not what it was expected to be on iteration " + i, 0,
        mapToCheck.size());
    }
  }

}
