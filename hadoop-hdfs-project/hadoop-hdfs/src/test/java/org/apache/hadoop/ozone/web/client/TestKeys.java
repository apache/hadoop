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
package org.apache.hadoop.ozone.web.client;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestKeys {
  static MiniDFSCluster cluster = null;
  static int port = 0;
  static private String path;
  private static OzoneClient client = null;


  /**
   * Create a MiniDFSCluster for testing.
   *
   * Ozone is made active by setting DFS_OBJECTSTORE_ENABLED_KEY = true and
   * DFS_STORAGE_HANDLER_TYPE_KEY = "local" , which uses a local
   * directory to emulate Ozone backend.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init()
      throws IOException, OzoneException, URISyntaxException {
    OzoneConfiguration conf = new OzoneConfiguration();

    URL p = conf.getClass().getResource("");
    path = p.getPath().concat(TestKeys.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
                            OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);

    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");

    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_TRACE_ENABLED_KEY, true);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);


    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DataNode dataNode = cluster.getDataNodes().get(0);
    port = dataNode.getInfoPort();
    client = new OzoneClient(String.format("http://localhost:%d", port));
  }

  /**
   * shutdown MiniDFSCluster
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Creates a file with Random Data
   *
   * @return File.
   */
  private File createRandomDataFile(String fileName, long size) {
    File tmpDir = new File(path);
    tmpDir.mkdirs();
    File tmpFile = new File(path + "/" + fileName);
    try {
      FileOutputStream randFile = new FileOutputStream(tmpFile);
      Random r = new Random();
      for (int x = 0; x < size; x++) {
        char c = (char) (r.nextInt(26) + 'a');
        randFile.write(c);
      }

    } catch (IOException e) {
      fail(e.getMessage());
    }
    return tmpFile;
  }


  private class PutHelper {
    OzoneVolume vol;
    OzoneBucket bucket;
    File file;

    public OzoneVolume getVol() {
      return vol;
    }

    public OzoneBucket getBucket() {
      return bucket;
    }

    public File getFile() {
      return file;
    }
    /**
     * This function is reused in all other tests.
     *
     * @return Returns the name of the new key that was created.
     * @throws OzoneException
     */
    private String putKey() throws
        OzoneException {
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      client.setUserAuth("hdfs");

      vol = client.createVolume(volumeName, "bilbo", "100TB");
      String[] acls = {"user:frodo:rw", "user:samwise:rw"};

      String bucketName = OzoneUtils.getRequestID().toLowerCase();
      bucket = vol.createBucket(bucketName, acls, StorageType.DEFAULT);

      String keyName = OzoneUtils.getRequestID().toLowerCase();
      file = createRandomDataFile(keyName, 1024);

      bucket.putKey(keyName, file);
      return keyName;
    }

  }

  @Test
  public void testPutKey() throws OzoneException {
    PutHelper helper  = new PutHelper();
    helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());
  }


  @Test
  public void testPutAndGetKey() throws OzoneException, IOException {

    PutHelper helper  = new PutHelper();
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    String newFileName =  path + "/" +OzoneUtils.getRequestID().toLowerCase();
    Path newPath = Paths.get(newFileName);
    helper.getBucket().getKey(keyName, newPath);

    FileInputStream original = new FileInputStream(helper.getFile());
    FileInputStream downloaded = new FileInputStream(newPath.toFile());


    String originalHash = DigestUtils.sha256Hex(original);
    String downloadedHash = DigestUtils.sha256Hex(downloaded);

    assertEquals(
        "Sha256 does not match between original file and downloaded file.",
        originalHash, downloadedHash);

  }

  @Test
  public void testPutAndDeleteKey() throws OzoneException, IOException {

    PutHelper helper  = new PutHelper();
    String keyName = helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());
    helper.getBucket().deleteKey(keyName);

    try {
      helper.getBucket().getKey(keyName);
      fail("Get Key on a deleted key should have thrown");
    } catch (OzoneException ex) {
      assertEquals(ex.getShortMessage(),
          ErrorTable.INVALID_KEY.getShortMessage());
    }
  }


  @Test
  public void testPutAndListKey() throws OzoneException, IOException {
    PutHelper helper  = new PutHelper();
    helper.putKey();
    assertNotNull(helper.getBucket());
    assertNotNull(helper.getFile());

    for (int x = 0; x < 10; x++) {
      String newkeyName =   OzoneUtils.getRequestID().toLowerCase();
      helper.getBucket().putKey(newkeyName, helper.getFile());
    }

    List<OzoneKey> keyList = helper.getBucket().listKeys();
    Assert.assertEquals(keyList.size(), 11);
  }
}
