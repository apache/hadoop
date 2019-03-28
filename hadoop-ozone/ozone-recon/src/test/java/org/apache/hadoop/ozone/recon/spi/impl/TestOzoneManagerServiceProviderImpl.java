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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.utils.db.DBCheckpoint;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Class to test Ozone Manager Service Provider Implementation.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
@PrepareForTest(ReconUtils.class)
public class TestOzoneManagerServiceProviderImpl extends
    AbstractOMMetadataManagerTest {

  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private Injector injector;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager();
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          initializeNewOmMetadataManager();
          writeDataToOm(omMetadataManager, "key_one");
          bind(OzoneConfiguration.class).toInstance(
              getTestOzoneConfiguration());
          reconOMMetadataManager = getTestMetadataManager(omMetadataManager);
          bind(ReconOMMetadataManager.class).toInstance(reconOMMetadataManager);
          ozoneManagerServiceProvider = new OzoneManagerServiceProviderImpl(
              getTestOzoneConfiguration());
          bind(OzoneManagerServiceProvider.class)
              .toInstance(ozoneManagerServiceProvider);
        } catch (IOException e) {
          Assert.fail();
        }
      }
    });
  }

  @Test
  public void testInit() throws Exception {

    Assert.assertNotNull(reconOMMetadataManager.getKeyTable()
        .get("/sampleVol/bucketOne/key_one"));
    Assert.assertNull(reconOMMetadataManager.getKeyTable()
        .get("/sampleVol/bucketOne/key_two"));

    writeDataToOm(omMetadataManager, "key_two");
    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    File tarFile = OmUtils.createTarFile(checkpoint.getCheckpointLocation());
    InputStream inputStream = new FileInputStream(tarFile);
    PowerMockito.stub(PowerMockito.method(ReconUtils.class,
        "makeHttpCall",
        CloseableHttpClient.class, String.class))
        .toReturn(inputStream);

    ozoneManagerServiceProvider.init();

    Assert.assertNotNull(reconOMMetadataManager.getKeyTable()
        .get("/sampleVol/bucketOne/key_one"));
    Assert.assertNotNull(reconOMMetadataManager.getKeyTable()
        .get("/sampleVol/bucketOne/key_two"));
  }

  @Test
  public void testGetOMMetadataManagerInstance() throws Exception {
    OMMetadataManager omMetaMgr = ozoneManagerServiceProvider
        .getOMMetadataManagerInstance();
    assertNotNull(omMetaMgr);
  }

  @Test
  public void testGetOzoneManagerDBSnapshot() throws Exception {

    File reconOmSnapshotDbDir = temporaryFolder.newFolder();

    File checkpointDir = Paths.get(reconOmSnapshotDbDir.getAbsolutePath(),
        "testGetOzoneManagerDBSnapshot").toFile();
    checkpointDir.mkdir();

    File file1 = Paths.get(checkpointDir.getAbsolutePath(), "file1")
        .toFile();
    String str = "File1 Contents";
    BufferedWriter writer = new BufferedWriter(new FileWriter(
        file1.getAbsolutePath()));
    writer.write(str);
    writer.close();

    File file2 = Paths.get(checkpointDir.getAbsolutePath(), "file2")
        .toFile();
    str = "File2 Contents";
    writer = new BufferedWriter(new FileWriter(file2.getAbsolutePath()));
    writer.write(str);
    writer.close();

    //Create test tar file.
    File tarFile = OmUtils.createTarFile(checkpointDir.toPath());

    InputStream fileInputStream = new FileInputStream(tarFile);
    PowerMockito.stub(PowerMockito.method(ReconUtils.class,
        "makeHttpCall",
        CloseableHttpClient.class, String.class))
        .toReturn(fileInputStream);

    DBCheckpoint checkpoint = ozoneManagerServiceProvider
        .getOzoneManagerDBSnapshot();
    assertNotNull(checkpoint);
    assertTrue(checkpoint.getCheckpointLocation().toFile().isDirectory());
    assertTrue(checkpoint.getCheckpointLocation().toFile()
        .listFiles().length == 2);
  }

  /**
   * Get Test OzoneConfiguration instance.
   * @return OzoneConfiguration
   * @throws IOException ioEx.
   */
  private OzoneConfiguration getTestOzoneConfiguration() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    return configuration;
  }

}