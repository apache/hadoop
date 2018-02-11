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

package org.apache.hadoop.cblock.kubernetes;

import io.kubernetes.client.JSON;
import io.kubernetes.client.models.V1PersistentVolume;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_IP;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Test the resource generation of Dynamic Provisioner.
 */
public class TestDynamicProvisioner {

  @Test
  public void persitenceVolumeBuilder() throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStrings(DFS_CBLOCK_ISCSI_ADVERTISED_IP, "1.2.3.4");

    DynamicProvisioner provisioner =
        new DynamicProvisioner(conf, null);

    String pvc = new String(Files.readAllBytes(
        Paths.get(getClass().getResource(
                "/dynamicprovisioner/input1-pvc.json").toURI())));

    String pv = new String(Files.readAllBytes(
        Paths.get(getClass().getResource(
            "/dynamicprovisioner/expected1-pv.json").toURI())));

    JSON json = new io.kubernetes.client.JSON();

    V1PersistentVolumeClaim claim =
        json.getGson().fromJson(pvc, V1PersistentVolumeClaim.class);

    String volumeName = provisioner.createVolumeName(claim);

    V1PersistentVolume volume =
        provisioner.persitenceVolumeBuilder(claim, volumeName);

    //remove the data which should not been compared
    V1PersistentVolume expectedVolume =
        json.getGson().fromJson(pv, V1PersistentVolume.class);


    Assert.assertEquals(expectedVolume, volume);
  }

}