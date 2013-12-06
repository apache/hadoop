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
package org.apache.hadoop.nfs.nfs3;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.nfs.nfs3.IdUserGroup.DuplicateNameOrIdException;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class TestIdUserGroup {

  @Test
  public void testDuplicates() throws IOException {
    String GET_ALL_USERS_CMD = "echo \"root:x:0:0:root:/root:/bin/bash\n"
        + "hdfs:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
        + "hdfs:x:11502:10788:Grid Distributed File System:/home/hdfs:/bin/bash\""
        + " | cut -d: -f1,3";
    String GET_ALL_GROUPS_CMD = "echo \"hdfs:*:11501:hrt_hdfs\n"
        + "mapred:x:497\n" + "mapred2:x:497\"" + " | cut -d: -f1,3";
    // Maps for id to name map
    BiMap<Integer, String> uMap = HashBiMap.create();
    BiMap<Integer, String> gMap = HashBiMap.create();

    try {
      IdUserGroup.updateMapInternal(uMap, "user", GET_ALL_USERS_CMD, ":");
      fail("didn't detect the duplicate name");
    } catch (DuplicateNameOrIdException e) {
    }
    
    try {
      IdUserGroup.updateMapInternal(gMap, "group", GET_ALL_GROUPS_CMD, ":");
      fail("didn't detect the duplicate id");
    } catch (DuplicateNameOrIdException e) {
    }
  }
}
