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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTimelineClientForATS1_5 {

  private final static Logger LOG = LoggerFactory
      .getLogger(TestTimelineClientForATS1_5.class);

  private TimelineClientImpl client;
  private static FileContext localFS;
  private static File localActiveDir;
  private TimelineWriter spyTimelineWriter;
  private UserGroupInformation authUgi;

  @BeforeEach
  public void setup() throws Exception {
    localFS = FileContext.getLocalFSFileContext();
    localActiveDir =
        new File("target", this.getClass().getSimpleName() + "-activeDir")
            .getAbsoluteFile();
    localFS.delete(new Path(localActiveDir.getAbsolutePath()), true);
    localActiveDir.mkdir();
    LOG.info("Created activeDir in " + localActiveDir.getAbsolutePath());
    authUgi = UserGroupInformation.getCurrentUser();
  }

  private YarnConfiguration getConfigurations() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.5f);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR,
        localActiveDir.getAbsolutePath());
    conf.set(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES,
        "summary_type");
    return conf;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (client != null) {
      client.stop();
    }
    localFS.delete(new Path(localActiveDir.getAbsolutePath()), true);
  }

  @Test
  void testPostEntities() throws Exception {
    client = createTimelineClient(getConfigurations());
    verifyForPostEntities(false);
  }

  @Test
  void testPostEntitiesToKeepUnderUserDir() throws Exception {
    YarnConfiguration conf = getConfigurations();
    conf.setBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_WITH_USER_DIR,
        true);
    client = createTimelineClient(conf);
    verifyForPostEntities(true);
  }

  private void verifyForPostEntities(boolean storeInsideUserDir) {
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TimelineEntityGroupId groupId =
        TimelineEntityGroupId.newInstance(appId, "1");
    TimelineEntityGroupId groupId2 =
        TimelineEntityGroupId.newInstance(appId, "2");
    // Create two entities, includes an entity type and a summary type
    TimelineEntity[] entities = new TimelineEntity[2];
    entities[0] = generateEntity("entity_type");
    entities[1] = generateEntity("summary_type");
    try {
      // if attemptid is null, fall back to the original putEntities call, and
      // save the entity
      // into configured levelDB store
      client.putEntities(null, null, entities);
      verify(spyTimelineWriter, times(1)).putEntities(entities);
      reset(spyTimelineWriter);

      // if the attemptId is specified, but groupId is given as null, it would
      // fall back to the original putEntities call if we have the entity type.
      // the entity which is summary type would be written into FS
      ApplicationAttemptId attemptId1 =
          ApplicationAttemptId.newInstance(appId, 1);
      client.putEntities(attemptId1, null, entities);
      TimelineEntity[] entityTDB = new TimelineEntity[1];
      entityTDB[0] = entities[0];
      verify(spyTimelineWriter, times(1)).putEntities(entityTDB);
      assertTrue(localFS.util().exists(
          new Path(getAppAttemptDir(attemptId1, storeInsideUserDir),
              "summarylog-"
            + attemptId1.toString())));
      reset(spyTimelineWriter);

      // if we specified attemptId as well as groupId, it would save the entity
      // into
      // FileSystem instead of levelDB store
      ApplicationAttemptId attemptId2 =
          ApplicationAttemptId.newInstance(appId, 2);
      client.putEntities(attemptId2, groupId, entities);
      client.putEntities(attemptId2, groupId2, entities);
      verify(spyTimelineWriter, times(0)).putEntities(
        any(TimelineEntity[].class));
      assertTrue(localFS.util().exists(
          new Path(getAppAttemptDir(attemptId2, storeInsideUserDir),
              "summarylog-"
            + attemptId2.toString())));
      assertTrue(localFS.util().exists(
          new Path(getAppAttemptDir(attemptId2, storeInsideUserDir),
              "entitylog-"
            + groupId.toString())));
      assertTrue(localFS.util().exists(
          new Path(getAppAttemptDir(attemptId2, storeInsideUserDir),
              "entitylog-"
            + groupId2.toString())));
      reset(spyTimelineWriter);
    } catch (Exception e) {
      fail("Exception is not expected. " + e);
    }
  }

  @Test
  void testPutDomain() {
    client = createTimelineClient(getConfigurations());
    verifyForPutDomain(false);
  }

  @Test
  void testPutDomainToKeepUnderUserDir() {
    YarnConfiguration conf = getConfigurations();
    conf.setBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_WITH_USER_DIR,
        true);
    client = createTimelineClient(conf);
    verifyForPutDomain(true);
  }

  private void verifyForPutDomain(boolean storeInsideUserDir) {
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.newInstance(appId, 1);
    try {
      TimelineDomain domain = generateDomain();

      client.putDomain(null, domain);
      verify(spyTimelineWriter, times(1)).putDomain(domain);
      reset(spyTimelineWriter);

      client.putDomain(attemptId1, domain);
      verify(spyTimelineWriter, times(0)).putDomain(domain);
      assertTrue(localFS.util()
          .exists(new Path(getAppAttemptDir(attemptId1, storeInsideUserDir),
              "domainlog-" + attemptId1.toString())));
      reset(spyTimelineWriter);
    } catch (Exception e) {
      fail("Exception is not expected." + e);
    }
  }

  private Path getAppAttemptDir(ApplicationAttemptId appAttemptId,
      boolean storeInsideUserDir) {
    Path userDir = getUserDir(appAttemptId, storeInsideUserDir);
    Path appDir = new Path(userDir, appAttemptId.getApplicationId().toString());
    Path attemptDir = new Path(appDir, appAttemptId.toString());
    return attemptDir;
  }

  private Path getUserDir(ApplicationAttemptId appAttemptId,
      boolean storeInsideUserDir) {
    if (!storeInsideUserDir) {
      return new Path(localActiveDir.getAbsolutePath());
    }
    Path userDir =
        new Path(localActiveDir.getAbsolutePath(), authUgi.getShortUserName());
    return userDir;
  }

  private static TimelineEntity generateEntity(String type) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("entity id");
    entity.setEntityType(type);
    entity.setStartTime(System.currentTimeMillis());
    return entity;
  }

  private static TimelineDomain generateDomain() {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("namespace id");
    domain.setDescription("domain description");
    domain.setOwner("domain owner");
    domain.setReaders("domain_reader");
    domain.setWriters("domain_writer");
    domain.setCreatedTime(0L);
    domain.setModifiedTime(1L);
    return domain;
  }

  private TimelineClientImpl createTimelineClient(YarnConfiguration conf) {
    TimelineClientImpl client = new TimelineClientImpl() {
      @Override
      protected TimelineWriter createTimelineWriter(Configuration conf,
          UserGroupInformation authUgi, Client client, URI resURI)
          throws IOException {
        TimelineWriter timelineWriter =
            new FileSystemTimelineWriter(conf, authUgi, client, resURI) {
              public ClientResponse doPostingObject(Object object, String path) {
                ClientResponse response = mock(ClientResponse.class);
                when(response.getStatusInfo()).thenReturn(
                    ClientResponse.Status.OK);
                return response;
              }
            };
        spyTimelineWriter = spy(timelineWriter);
        return spyTimelineWriter;
      }
    };

    client.init(conf);
    client.start();
    return client;
  }
}
