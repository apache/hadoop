/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timeline;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestLogInfo {

  private static final Path TEST_ROOT_DIR = new Path(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestLogInfo.class.getSimpleName());

  private static final String TEST_ATTEMPT_DIR_NAME = "test_app";
  private static final String TEST_ENTITY_FILE_NAME = "test_entity";
  private static final String TEST_DOMAIN_FILE_NAME = "test_domain";
  private static final String TEST_BROKEN_FILE_NAME = "test_broken";

  private Configuration config = new YarnConfiguration();
  private MiniDFSCluster hdfsCluster;
  private FileSystem fs;
  private FileContext fc;
  private FileContextTestHelper fileContextTestHelper = new FileContextTestHelper("/tmp/TestLogInfo");
  private ObjectMapper objMapper;

  private JsonFactory jsonFactory = new JsonFactory();
  private JsonGenerator jsonGenerator;
  private FSDataOutputStream outStream = null;
  private FSDataOutputStream outStreamDomain = null;

  private TimelineDomain testDomain;

  private static final short FILE_LOG_DIR_PERMISSIONS = 0770;

  @Before
  public void setup() throws Exception {
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR.toString());
    HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(1).build();
    fs = hdfsCluster.getFileSystem();
    fc = FileContext.getFileContext(hdfsCluster.getURI(0), config);
    Path testAppDirPath = getTestRootPath(TEST_ATTEMPT_DIR_NAME);
    fs.mkdirs(testAppDirPath, new FsPermission(FILE_LOG_DIR_PERMISSIONS));
    objMapper = PluginStoreTestUtils.createObjectMapper();

    TimelineEntities testEntities = PluginStoreTestUtils.generateTestEntities();
    writeEntitiesLeaveOpen(testEntities,
        new Path(testAppDirPath, TEST_ENTITY_FILE_NAME));

    testDomain = new TimelineDomain();
    testDomain.setId("domain_1");
    testDomain.setReaders(UserGroupInformation.getLoginUser().getUserName());
    testDomain.setOwner(UserGroupInformation.getLoginUser().getUserName());
    testDomain.setDescription("description");
    writeDomainLeaveOpen(testDomain,
        new Path(testAppDirPath, TEST_DOMAIN_FILE_NAME));

    writeBrokenFile(new Path(testAppDirPath, TEST_BROKEN_FILE_NAME));
  }

  @After
  public void tearDown() throws Exception {
    jsonGenerator.close();
    outStream.close();
    outStreamDomain.close();
    hdfsCluster.shutdown();
  }

  @Test
  public void testMatchesGroupId() throws Exception {
    String testGroupId = "app1_group1";
    // Match
    EntityLogInfo testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME,
        "app1_group1",
        UserGroupInformation.getLoginUser().getUserName());
    assertTrue(testLogInfo.matchesGroupId(testGroupId));
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME,
        "test_app1_group1",
        UserGroupInformation.getLoginUser().getUserName());
    assertTrue(testLogInfo.matchesGroupId(testGroupId));
    // Unmatch
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME, "app2_group1",
        UserGroupInformation.getLoginUser().getUserName());
    assertFalse(testLogInfo.matchesGroupId(testGroupId));
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME, "app1_group2",
        UserGroupInformation.getLoginUser().getUserName());
    assertFalse(testLogInfo.matchesGroupId(testGroupId));
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME, "app1_group12",
        UserGroupInformation.getLoginUser().getUserName());
    assertFalse(testLogInfo.matchesGroupId(testGroupId));
    // Check delimiters
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME, "app1_group1_2",
        UserGroupInformation.getLoginUser().getUserName());
    assertTrue(testLogInfo.matchesGroupId(testGroupId));
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME, "app1_group1.dat",
        UserGroupInformation.getLoginUser().getUserName());
    assertTrue(testLogInfo.matchesGroupId(testGroupId));
    // Check file names shorter than group id
    testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME, "app2",
        UserGroupInformation.getLoginUser().getUserName());
    assertFalse(testLogInfo.matchesGroupId(testGroupId));
  }

  @Test
  public void testParseEntity() throws Exception {
    // Load test data
    TimelineDataManager tdm = PluginStoreTestUtils.getTdmWithMemStore(config);
    EntityLogInfo testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME,
        TEST_ENTITY_FILE_NAME,
        UserGroupInformation.getLoginUser().getUserName());
    testLogInfo.parseForStore(tdm, getTestRootPath(), true, jsonFactory, objMapper,
        fs);
    // Verify for the first batch
    PluginStoreTestUtils.verifyTestEntities(tdm);
    // Load new data
    TimelineEntity entityNew = PluginStoreTestUtils
        .createEntity("id_3", "type_3", 789l, null, null,
            null, null, "domain_id_1");
    TimelineEntities entityList = new TimelineEntities();
    entityList.addEntity(entityNew);
    writeEntitiesLeaveOpen(entityList,
        new Path(getTestRootPath(TEST_ATTEMPT_DIR_NAME), TEST_ENTITY_FILE_NAME));
    testLogInfo.parseForStore(tdm, getTestRootPath(), true, jsonFactory, objMapper,
        fs);
    // Verify the newly added data
    TimelineEntity entity3 = tdm.getEntity(entityNew.getEntityType(),
        entityNew.getEntityId(), EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertNotNull(entity3);
    assertEquals("Failed to read out entity new",
        entityNew.getStartTime(), entity3.getStartTime());
    tdm.close();
  }

  @Test
  public void testParseBrokenEntity() throws Exception {
    // Load test data
    TimelineDataManager tdm = PluginStoreTestUtils.getTdmWithMemStore(config);
    EntityLogInfo testLogInfo = new EntityLogInfo(TEST_ATTEMPT_DIR_NAME,
        TEST_BROKEN_FILE_NAME,
        UserGroupInformation.getLoginUser().getUserName());
    DomainLogInfo domainLogInfo = new DomainLogInfo(TEST_ATTEMPT_DIR_NAME,
        TEST_BROKEN_FILE_NAME,
        UserGroupInformation.getLoginUser().getUserName());
    // Try parse, should not fail
    testLogInfo.parseForStore(tdm, getTestRootPath(), true, jsonFactory, objMapper,
        fs);
    domainLogInfo.parseForStore(tdm, getTestRootPath(), true, jsonFactory, objMapper,
        fs);
    tdm.close();
  }

  @Test
  public void testParseDomain() throws Exception {
    // Load test data
    TimelineDataManager tdm = PluginStoreTestUtils.getTdmWithMemStore(config);
    DomainLogInfo domainLogInfo = new DomainLogInfo(TEST_ATTEMPT_DIR_NAME,
        TEST_DOMAIN_FILE_NAME,
        UserGroupInformation.getLoginUser().getUserName());
    domainLogInfo.parseForStore(tdm, getTestRootPath(), true, jsonFactory, objMapper,
        fs);
    // Verify domain data
    TimelineDomain resultDomain = tdm.getDomain("domain_1",
        UserGroupInformation.getLoginUser());
    assertNotNull(resultDomain);
    assertEquals(testDomain.getReaders(), resultDomain.getReaders());
    assertEquals(testDomain.getOwner(), resultDomain.getOwner());
    assertEquals(testDomain.getDescription(), resultDomain.getDescription());
  }

  private void writeBrokenFile(Path logPath) throws IOException {
    FSDataOutputStream out = null;
    try {
      String broken = "{ broken { [[]} broken";
      out = PluginStoreTestUtils.createLogFile(logPath, fs);
      out.write(broken.getBytes(Charset.forName("UTF-8")));
      out.close();
      out = null;
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  // TestLogInfo needs to maintain opened hdfs files so we have to build our own
  // write methods
  private void writeEntitiesLeaveOpen(TimelineEntities entities, Path logPath)
      throws IOException {
    if (outStream == null) {
      outStream = PluginStoreTestUtils.createLogFile(logPath, fs);
      jsonGenerator = new JsonFactory().createGenerator(
          (OutputStream)outStream);
      jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
    }
    for (TimelineEntity entity : entities.getEntities()) {
      objMapper.writeValue(jsonGenerator, entity);
    }
    outStream.hflush();
  }

  private void writeDomainLeaveOpen(TimelineDomain domain, Path logPath)
      throws IOException {
    if (outStreamDomain == null) {
      outStreamDomain = PluginStoreTestUtils.createLogFile(logPath, fs);
    }
    // Write domain uses its own json generator to isolate from entity writers
    JsonGenerator jsonGeneratorLocal
        = new JsonFactory().createGenerator((OutputStream)outStreamDomain);
    jsonGeneratorLocal.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
    objMapper.writeValue(jsonGeneratorLocal, domain);
    outStreamDomain.hflush();
  }

  private Path getTestRootPath() {
    return fileContextTestHelper.getTestRootPath(fc);
  }

  private Path getTestRootPath(String pathString) {
    return fileContextTestHelper.getTestRootPath(fc, pathString);
  }

}
