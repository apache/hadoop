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

package org.apache.hadoop.ozone.tools;

import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests Corona, with MiniOzoneCluster.
 */
public class TestCorona {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .numDataNodes(5).build();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void defaultTest() throws Exception {
    List<String> args = new ArrayList<>();
    args.add("-numOfVolumes");
    args.add("2");
    args.add("-numOfBuckets");
    args.add("5");
    args.add("-numOfKeys");
    args.add("10");
    Corona corona = new Corona(conf);
    int res = ToolRunner.run(conf, corona,
        args.toArray(new String[0]));
    Assert.assertEquals(2, corona.getNumberOfVolumesCreated());
    Assert.assertEquals(10, corona.getNumberOfBucketsCreated());
    Assert.assertEquals(100, corona.getNumberOfKeysAdded());
    Assert.assertEquals(10240 - 36, corona.getKeyValueLength());
    Assert.assertEquals(0, res);
  }

  @Test
  public void validateWriteTest() throws Exception {
    PrintStream originalStream = System.out;
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outStream));
    List<String> args = new ArrayList<>();
    args.add("-validateWrites");
    args.add("-numOfVolumes");
    args.add("2");
    args.add("-numOfBuckets");
    args.add("5");
    args.add("-numOfKeys");
    args.add("10");
    Corona corona = new Corona(conf);
    int res = ToolRunner.run(conf, corona,
        args.toArray(new String[0]));
    Assert.assertEquals(0, res);
    Assert.assertEquals(2, corona.getNumberOfVolumesCreated());
    Assert.assertEquals(10, corona.getNumberOfBucketsCreated());
    Assert.assertEquals(100, corona.getNumberOfKeysAdded());
    Assert.assertTrue(corona.getValidateWrites());
    Assert.assertNotEquals(0, corona.getTotalKeysValidated());
    Assert.assertNotEquals(0, corona.getSuccessfulValidationCount());
    Assert.assertEquals(0, corona.getUnsuccessfulValidationCount());
    System.setOut(originalStream);
  }

  @Test
  public void multiThread() throws Exception {
    List<String> args = new ArrayList<>();
    args.add("-numOfVolumes");
    args.add("10");
    args.add("-numOfBuckets");
    args.add("1");
    args.add("-numOfKeys");
    args.add("10");
    args.add("-numOfThread");
    args.add("10");
    args.add("-keySize");
    args.add("10240");
    Corona corona = new Corona(conf);
    int res = ToolRunner.run(conf, corona,
        args.toArray(new String[0]));
    Assert.assertEquals(10, corona.getNumberOfVolumesCreated());
    Assert.assertEquals(10, corona.getNumberOfBucketsCreated());
    Assert.assertEquals(100, corona.getNumberOfKeysAdded());
    Assert.assertEquals(0, res);
  }

  @Test
  public void ratisTest3() throws Exception {
    List<String> args = new ArrayList<>();
    args.add("-numOfVolumes");
    args.add("10");
    args.add("-numOfBuckets");
    args.add("1");
    args.add("-numOfKeys");
    args.add("10");
    args.add("-ratis");
    args.add("3");
    args.add("-numOfThread");
    args.add("10");
    args.add("-keySize");
    args.add("10240");
    Corona corona = new Corona(conf);
    int res = ToolRunner.run(conf, corona,
        args.toArray(new String[0]));
    Assert.assertEquals(10, corona.getNumberOfVolumesCreated());
    Assert.assertEquals(10, corona.getNumberOfBucketsCreated());
    Assert.assertEquals(100, corona.getNumberOfKeysAdded());
    Assert.assertEquals(0, res);
  }
}
