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
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Test TrashProcedure.
 */
public class TestTrashProcedure {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static String nnUri;

  @BeforeClass
  public static void beforeClass() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    nnUri = FileSystem.getDefaultUri(conf).toString();
  }

  @AfterClass
  public static void afterClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testTrashProcedure() throws Exception {
    Path src = new Path("/" + getMethodName() + "-src");
    Path dst = new Path("/" + getMethodName() + "-dst");
    FileSystem fs = cluster.getFileSystem();
    fs.mkdirs(src);
    fs.mkdirs(new Path(src, "dir"));
    assertTrue(fs.exists(src));

    FedBalanceContext context =
        new FedBalanceContext.Builder(src, dst, TestDistCpProcedure.MOUNT, conf)
            .setMapNum(10).setBandwidthLimit(1).setTrash(TrashOption.TRASH)
            .build();
    TrashProcedure trashProcedure =
        new TrashProcedure("trash-procedure", null, 1000, context);
    trashProcedure.moveToTrash();
    assertFalse(fs.exists(src));
  }

  @Test
  public void testSeDeserialize() throws Exception {
    Path src = new Path("/" + getMethodName() + "-src");
    Path dst = new Path("/" + getMethodName() + "-dst");
    FedBalanceContext context =
        new FedBalanceContext.Builder(src, dst, TestDistCpProcedure.MOUNT, conf)
            .setMapNum(10).setBandwidthLimit(1).setTrash(TrashOption.TRASH)
            .build();
    TrashProcedure trashProcedure =
        new TrashProcedure("trash-procedure", null, 1000, context);
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutput dataOut = new DataOutputStream(bao);
    trashProcedure.write(dataOut);
    trashProcedure = new TrashProcedure();
    trashProcedure.readFields(
        new DataInputStream(new ByteArrayInputStream(bao.toByteArray())));
    assertEquals(context, trashProcedure.getContext());
  }
}
