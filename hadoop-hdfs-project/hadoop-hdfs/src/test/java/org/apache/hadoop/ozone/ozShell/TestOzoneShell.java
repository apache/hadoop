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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.web.client.OzoneRestClient;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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
  private static OzoneRestClient client = null;
  private static Shell shell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

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
    client = new OzoneRestClient(String.format("http://localhost:%d", port));
    client.setUserAuth(OzoneConsts.OZONE_SIMPLE_HDFS_USER);
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

  @Before
  public void setup() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  @After
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  @Test
  public void testCreateVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    String[] args = new String[] {"-createVolume", url + "/" + volumeName,
        "-user", userName, "-root"};

    assertEquals(0, ToolRunner.run(shell, args));
    OzoneVolume volumeInfo = client.getVolume(volumeName);
    assertEquals(volumeName, volumeInfo.getVolumeName());
    assertEquals(userName, volumeInfo.getOwnerName());
  }

  @Test
  public void testDeleteVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    OzoneVolume vol = client.createVolume(volumeName, "bilbo", "100TB");
    assertNotNull(vol);

    String[] args = new String[] {"-deleteVolume", url + "/" + volumeName,
        "-root"};
    assertEquals(0, ToolRunner.run(shell, args));

    // verify if volume has been deleted
    try {
      client.getVolume(volumeName);
      fail("Get volume call should have thrown.");
    } catch (OzoneException e) {
      GenericTestUtils.assertExceptionContains(
          "Info Volume failed, error:VOLUME_NOT_FOUND", e);
    }
  }

  @Test
  public void testInfoVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    client.createVolume(volumeName, "bilbo", "100TB");

    String[] args = new String[] {"-infoVolume", url + "/" + volumeName,
        "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    assertTrue(out.toString().contains(volumeName));

    // get info for non-exist volume
    args = new String[] {"-infoVolume", url + "/invalid-volume", "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Info Volume failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testUpdateVolume() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String userName = "bilbo";
    OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
    assertEquals(userName, vol.getOwnerName());
    assertEquals(100, vol.getQuota().getSize(), 100);
    assertEquals(OzoneQuota.Units.TB, vol.getQuota().getUnit());

    String[] args = new String[] {"-updateVolume", url + "/" + volumeName,
        "-quota", "500MB", "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    vol = client.getVolume(volumeName);
    assertEquals(userName, vol.getOwnerName());
    assertEquals(500, vol.getQuota().getSize(), 500);
    assertEquals(OzoneQuota.Units.MB, vol.getQuota().getUnit());

    String newUser = "new-user";
    args = new String[] {"-updateVolume", url + "/" + volumeName,
        "-user", newUser, "-root"};
    assertEquals(0, ToolRunner.run(shell, args));
    vol = client.getVolume(volumeName);
    assertEquals(newUser, vol.getOwnerName());

    // test error conditions
    args = new String[] {"-updateVolume", url + "/invalid-volume",
        "-user", newUser, "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Volume owner change failed, error:VOLUME_NOT_FOUND"));

    err.reset();
    args = new String[] {"-updateVolume", url + "/invalid-volume",
        "-quota", "500MB", "-root"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "Volume quota change failed, error:VOLUME_NOT_FOUND"));
  }

  @Test
  public void testListVolumes() throws Exception {
    final int volCount = 20;
    final String user1 = "test-user-a";
    final String user2 = "test-user-b";

    // Create 20 volumes, 10 for user1 and another 10 for user2.
    for (int x = 0; x < volCount; x++) {
      String volumeName;
      String userName;

      if (x % 2 == 0) {
        // create volume [test-vol0, test-vol2, ..., test-vol18] for user1
        userName = user1;
        volumeName = "test-vol" + x;
      } else {
        // create volume [test-vol1, test-vol3, ..., test-vol19] for user2
        userName = user2;
        volumeName = "test-vol" + x;
      }
      OzoneVolume vol = client.createVolume(volumeName, userName, "100TB");
      assertNotNull(vol);
    }

    // test -length option
    String[] args = new String[] {"-listVolume", url + "/", "-user",
        user1, "-length", "100"};
    assertEquals(0, ToolRunner.run(shell, args));

    List<String> names = getValueLines("name", out.toString());
    List<String> volumes = getValueLines("volumeName", out.toString());
    assertEquals(10, volumes.size());
    assertEquals(10, names.size());
    for (String user : names) {
      assertTrue(user.contains(user1));
    }

    out.reset();
    args = new String[] {"-listVolume", url + "/", "-user",
        user1, "-length", "2"};
    assertEquals(0, ToolRunner.run(shell, args));

    volumes = getValueLines("volumeName", out.toString());
    assertEquals(2, volumes.size());

    // test -prefix option
    out.reset();
    args = new String[] {"-listVolume", url + "/", "-user",
        user1, "-length", "100", "-prefix", "test-vol1"};
    assertEquals(0, ToolRunner.run(shell, args));

    names = getValueLines("name", out.toString());
    volumes = getValueLines("volumeName", out.toString());
    assertEquals(5, volumes.size());
    assertEquals(5, names.size());
    // return volume names should be [test-vol10, test-vol12, ..., test-vol18]
    for (int i = 0; i < volumes.size(); i++) {
      assertTrue(volumes.get(i).contains("test-vol" + ((i + 5) * 2)));
      assertTrue(names.get(i).contains(user1));
    }

    // test -start option
    out.reset();
    args = new String[] {"-listVolume", url + "/", "-user",
        user2, "-length", "100", "-start", "test-vol15"};
    assertEquals(0, ToolRunner.run(shell, args));

    names = getValueLines("name", out.toString());
    volumes = getValueLines("volumeName", out.toString());
    assertEquals(2, volumes.size());
    assertTrue(volumes.get(0).contains("test-vol17")
        && volumes.get(1).contains("test-vol19"));
    assertTrue(names.get(0).contains(user2)
        && names.get(1).contains(user2));

    // test error conditions
    err.reset();
    args  = new String[] {"-listVolume", url + "/", "-user",
        user2, "-length", "-1"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be a positive number"));

    err.reset();
    args  = new String[] {"-listVolume", url + "/", "-user",
        user2, "-length", "invalid-length"};
    assertEquals(1, ToolRunner.run(shell, args));
    assertTrue(err.toString().contains(
        "the vaule should be digital"));
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

  /**
   * Extract lines from output string that contains specified key name.
   * @param keyName Key name that line should contained.
   * @param outputStr Response output content.
   * @return List of string.
   */
  private List<String> getValueLines(String keyName, String outputStr) {
    List<String> nameLines = new ArrayList<>();
    String[] lines = outputStr.split("\n");

    for (String line : lines) {
      if (line.contains(keyName)) {
        nameLines.add(line);
      }
    }

    return nameLines;
  }
}
