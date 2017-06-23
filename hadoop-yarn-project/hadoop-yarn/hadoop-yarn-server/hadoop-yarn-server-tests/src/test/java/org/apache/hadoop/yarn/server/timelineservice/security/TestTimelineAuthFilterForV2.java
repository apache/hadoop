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

package org.apache.hadoop.yarn.server.timelineservice.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timelineservice.collector.NodeTimelineCollectorManager;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests timeline authentication filter based security for timeline service v2.
 */
@RunWith(Parameterized.class)
public class TestTimelineAuthFilterForV2 {

  private static final String FOO_USER = "foo";
  private static final String HTTP_USER = "HTTP";

  private static final File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.dir", "target" + File.separator +
          "test-dir"), UUID.randomUUID().toString());
  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/"
          + TestTimelineAuthFilterForV2.class.getSimpleName();
  private static File httpSpnegoKeytabFile = new File(KerberosTestUtils.
      getKeytabFile());
  private static String httpSpnegoPrincipal = KerberosTestUtils.
      getServerPrincipal();

  @Parameterized.Parameters
  public static Collection<Object[]> withSsl() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private static MiniKdc testMiniKDC;
  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration conf;
  private boolean withSsl;
  private NodeTimelineCollectorManager collectorManager;
  private PerNodeTimelineCollectorsAuxService auxService;

  public TestTimelineAuthFilterForV2(boolean withSsl) {
    this.withSsl = withSsl;
  }

  @BeforeClass
  public static void setup() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
      testMiniKDC.start();
      testMiniKDC.createPrincipal(
          httpSpnegoKeytabFile, HTTP_USER + "/localhost");
    } catch (Exception e) {
      fail("Couldn't setup MiniKDC.");
    }

    // Setup timeline service v2.
    try {
      conf = new Configuration(false);
      conf.setStrings(TimelineAuthenticationFilterInitializer.PREFIX + "type",
          "kerberos");
      conf.set(TimelineAuthenticationFilterInitializer.PREFIX +
          KerberosAuthenticationHandler.PRINCIPAL, httpSpnegoPrincipal);
      conf.set(TimelineAuthenticationFilterInitializer.PREFIX +
          KerberosAuthenticationHandler.KEYTAB,
          httpSpnegoKeytabFile.getAbsolutePath());
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      conf.set(YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL,
          httpSpnegoPrincipal);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
          httpSpnegoKeytabFile.getAbsolutePath());
      // Enable timeline service v2
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
          FileSystemTimelineWriterImpl.class, TimelineWriter.class);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_BIND_HOST, "localhost");
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          TEST_ROOT_DIR.getAbsolutePath());
      conf.set("hadoop.proxyuser.HTTP.hosts", "*");
      conf.set("hadoop.proxyuser.HTTP.users", FOO_USER);
      UserGroupInformation.setConfiguration(conf);
      SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
          YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, "localhost");
    } catch (Exception e) {
      fail("Couldn't setup TimelineServer V2.");
    }
  }

  @Before
  public void initialize() throws Exception {
    if (withSsl) {
      conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY,
          HttpConfig.Policy.HTTPS_ONLY.name());
      File base = new File(BASEDIR);
      FileUtil.fullyDelete(base);
      base.mkdirs();
      keystoresDir = new File(BASEDIR).getAbsolutePath();
      sslConfDir =
          KeyStoreTestUtil.getClasspathDir(TestTimelineAuthFilterForV2.class);
      KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    } else  {
      conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY,
          HttpConfig.Policy.HTTP_ONLY.name());
    }
    collectorManager = new DummyNodeTimelineCollectorManager();
    auxService = PerNodeTimelineCollectorsAuxService.launchServer(new String[0],
        collectorManager, conf);
  }

  private TimelineV2Client createTimelineClientForUGI(ApplicationId appId) {
    TimelineV2Client client =
        TimelineV2Client.createTimelineClient(ApplicationId.newInstance(0, 1));
    // set the timeline service address.
    String restBindAddr = collectorManager.getRestServerBindAddress();
    String addr =
        "localhost" + restBindAddr.substring(restBindAddr.indexOf(":"));
    client.setTimelineServiceAddress(addr);
    client.init(conf);
    client.start();
    return client;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
    FileUtil.fullyDelete(TEST_ROOT_DIR);
  }

  @After
  public void destroy() throws Exception {
    if (auxService != null) {
      auxService.stop();
    }
    if (withSsl) {
      KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
      File base = new File(BASEDIR);
      FileUtil.fullyDelete(base);
    }
  }

  private static TimelineEntity createEntity(String id, String type) {
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setId(id);
    entityToStore.setType(type);
    entityToStore.setCreatedTime(0L);
    return entityToStore;
  }

  private static void verifyEntity(File entityTypeDir, String id, String type)
      throws IOException {
    File entityFile = new File(entityTypeDir, id +
        FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION);
    assertTrue(entityFile.exists());
    TimelineEntity entity = readEntityFile(entityFile);
    assertNotNull(entity);
    assertEquals(id, entity.getId());
    assertEquals(type, entity.getType());
  }

  private static TimelineEntity readEntityFile(File entityFile)
      throws IOException {
    BufferedReader reader = null;
    String strLine;
    try {
      reader = new BufferedReader(new FileReader(entityFile));
      while ((strLine = reader.readLine()) != null) {
        if (strLine.trim().length() > 0) {
          return FileSystemTimelineReaderImpl.
              getTimelineRecordFromJSON(strLine.trim(), TimelineEntity.class);
        }
      }
      return null;
    } finally {
      reader.close();
    }
  }

  @Test
  public void testPutTimelineEntities() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    auxService.addApplication(appId);
    final String entityType = "dummy_type";
    File entityTypeDir = new File(TEST_ROOT_DIR.getAbsolutePath() +
        File.separator + "entities" + File.separator +
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID + File.separator + "test_user" +
        File.separator + "test_flow_name" + File.separator +
        "test_flow_version" + File.separator + "1" + File.separator +
        appId.toString() + File.separator + entityType);
    try {
      KerberosTestUtils.doAs(HTTP_USER + "/localhost", new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          TimelineV2Client client = createTimelineClientForUGI(appId);
          try {
            // Sync call. Results available immediately.
            client.putEntities(createEntity("entity1", entityType));
            assertEquals(1, entityTypeDir.listFiles().length);
            verifyEntity(entityTypeDir, "entity1", entityType);
            // Async call.
            client.putEntitiesAsync(createEntity("entity2", entityType));
            return null;
          } finally {
            client.stop();
          }
        }
      });
      // Wait for async entity to be published.
      for (int i = 0; i < 50; i++) {
        if (entityTypeDir.listFiles().length == 2) {
          break;
        }
        Thread.sleep(50);
      }
      assertEquals(2, entityTypeDir.listFiles().length);
      verifyEntity(entityTypeDir, "entity2", entityType);
    } finally {
      FileUtils.deleteQuietly(entityTypeDir);
    }
  }

  private static class DummyNodeTimelineCollectorManager extends
      NodeTimelineCollectorManager {
    DummyNodeTimelineCollectorManager() {
      super();
    }

    @Override
    protected CollectorNodemanagerProtocol getNMCollectorService() {
      CollectorNodemanagerProtocol protocol =
          mock(CollectorNodemanagerProtocol.class);
      try {
        GetTimelineCollectorContextResponse response =
            GetTimelineCollectorContextResponse.newInstance("test_user",
                "test_flow_name", "test_flow_version", 1L);
        when(protocol.getTimelineCollectorContext(any(
            GetTimelineCollectorContextRequest.class))).thenReturn(response);
      } catch (YarnException | IOException e) {
        fail();
      }
      return protocol;
    }
  }
}