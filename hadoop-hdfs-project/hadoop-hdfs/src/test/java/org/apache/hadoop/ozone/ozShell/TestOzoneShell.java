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
package org.apache.hadoop.ozone.ozShell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * This test class specified for testing Ozone shell command.
 */
public class TestOzoneShell {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  private static String url;
  private static File baseDir;
  private static OzoneConfiguration conf = null;
  private static MiniOzoneCluster cluster = null;
  private static Shell shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  /**
   * Create a MiniDFSCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init()
      throws IOException, URISyntaxException, OzoneException {
    conf = new OzoneConfiguration();

    String path = GenericTestUtils.getTempPath(
        TestOzoneShell.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);

    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
    conf.setQuietMode(false);
    shell = new Shell();
    shell.setConf(conf);

    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    DataNode dataNode = cluster.getDataNodes().get(0);
    final int port = dataNode.getInfoPort();
    url = String.format("http://localhost:%d", port);
  }

  /**
   * shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }
  }

  @Test
  public void testGetKeyInfo() throws Exception {
    // create a volume
    String volume = "volume" + RandomStringUtils.randomNumeric(5);
    String[] args = new String[] {"-createVolume", url + "/" + volume, "-user",
        "bilbo", "-root"};
    assertEquals(0, ToolRunner.run(shell, args));

    // create a bucket
    String bucket = "bucket" + RandomStringUtils.randomNumeric(5);
    args = new String[] {"-createBucket", url + "/" + volume + "/" + bucket};
    assertEquals(0, ToolRunner.run(shell, args));

    // write a new file that used for putting key
    String key = "key" + RandomStringUtils.randomNumeric(5);
    File tmpFile = new File(baseDir, "/testfile");
    FileOutputStream randFile = new FileOutputStream(tmpFile);
    Random r = new Random();
    for (int x = 0; x < 10; x++) {
      char c = (char) (r.nextInt(26) + 'a');
      randFile.write(c);
    }
    randFile.close();

    // create the key in above bucket
    args = new String[] {"-putKey",
        url + "/" + volume + "/" + bucket + "/" + key, "-file",
        tmpFile.getAbsolutePath()};
    assertEquals(0, ToolRunner.run(shell, args));

    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
    args = new String[] {"-infoKey",
        url + "/" + volume + "/" + bucket + "/" + key};

    // verify the response output
    assertEquals(0, ToolRunner.run(shell, args));
    assertTrue(out.toString().contains(key));

    // reset stream
    out.reset();
    err.reset();

    // get the info of a non-exist key
    args = new String[] {"-infoKey",
        url + "/" + volume + "/" + bucket + "/invalid-key"};

    // verify the response output
    // get the non-exist key info should be failed
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(Status.KEY_NOT_FOUND.toString()));
  }
}
