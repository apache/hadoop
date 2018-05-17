/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.hadoop.ozone.web;

import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.response.VolumeOwner;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test Ozone Volume info structure.
 */
public class TestVolumeStructs {

  @Test
  public void testVolumeInfoParse() throws IOException {
    VolumeInfo volInfo =
        new VolumeInfo("testvol", "Thu, Apr 9, 2015 10:23:45 GMT", "gandalf");
    VolumeOwner owner = new VolumeOwner("bilbo");
    volInfo.setOwner(owner);
    String jString = volInfo.toJsonString();
    VolumeInfo newVollInfo = VolumeInfo.parse(jString);
    String one = volInfo.toJsonString();
    String two = newVollInfo.toJsonString();

    assertEquals(volInfo.toJsonString(), newVollInfo.toJsonString());
  }

  @Test
  public void testVolumeInfoValue() throws IOException {
    String createdOn = "Thu, Apr 9, 2015 10:23:45 GMT";
    String createdBy = "gandalf";
    VolumeInfo volInfo = new VolumeInfo("testvol", createdOn, createdBy);
    assertEquals(volInfo.getCreatedBy(), createdBy);
    assertEquals(volInfo.getCreatedOn(), createdOn);
  }


  @Test
  public void testVolumeListParse() throws IOException {
    ListVolumes list = new ListVolumes();
    for (int x = 0; x < 100; x++) {
      VolumeInfo volInfo = new VolumeInfo("testvol" + Integer.toString(x),
          "Thu, Apr 9, 2015 10:23:45 GMT", "gandalf");
      list.addVolume(volInfo);
    }
    list.sort();
    String listString = list.toJsonString();
    ListVolumes newList = ListVolumes.parse(listString);
    assertEquals(list.toJsonString(), newList.toJsonString());
  }
}
