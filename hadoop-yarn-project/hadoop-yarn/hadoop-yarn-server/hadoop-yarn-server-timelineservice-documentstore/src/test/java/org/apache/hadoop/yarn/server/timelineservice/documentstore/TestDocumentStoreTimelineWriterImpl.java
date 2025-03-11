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

package org.apache.hadoop.yarn.server.timelineservice.documentstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DummyDocumentStoreWriter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;

/**
 * Test case for {@link DocumentStoreTimelineWriterImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class TestDocumentStoreTimelineWriterImpl {

  private final DocumentStoreWriter<TimelineDocument> documentStoreWriter = new
      DummyDocumentStoreWriter<>();
  private final Configuration conf = new Configuration();
  private MockedStatic<DocumentStoreFactory> mockedFactory;

  @BeforeEach
  public void setUp() throws YarnException {
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME,
        "TestDB");
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_COSMOSDB_ENDPOINT,
        "https://localhost:443");
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_COSMOSDB_MASTER_KEY,
        "1234567");
    mockedFactory = Mockito.mockStatic(DocumentStoreFactory.class);
    mockedFactory.when(() -> DocumentStoreFactory.createDocumentStoreWriter(
                    ArgumentMatchers.any(Configuration.class)))
            .thenReturn(documentStoreWriter);
  }

  @AfterEach
  public void tearDown() {
    if (mockedFactory != null) {
      mockedFactory.close();
    }
  }

  @Test
  public void testFailOnNoCosmosDBConfigs() throws Exception {
    assertThrows(YarnException.class, ()->{
      DocumentStoreUtils.validateCosmosDBConf(new Configuration());
    });
  }

  @Test
  public void testWritingToCosmosDB() throws Exception {
    DocumentStoreTimelineWriterImpl timelineWriter = new
        DocumentStoreTimelineWriterImpl();

    timelineWriter.serviceInit(conf);

    TimelineEntities entities = new TimelineEntities();
    entities.addEntities(DocumentStoreTestUtils.bakeTimelineEntities());
    entities.addEntity(DocumentStoreTestUtils.bakeTimelineEntityDoc()
        .fetchTimelineEntity());

    mockedFactory.verify(()->
        DocumentStoreFactory.createDocumentStoreWriter(ArgumentMatchers.any(Configuration.class)),
        times(4));

    TimelineCollectorContext context = new TimelineCollectorContext();
    context.setFlowName("TestFlow");
    context.setAppId("DUMMY_APP_ID");
    context.setClusterId("yarn_cluster");
    context.setUserId("test_user");
    timelineWriter.write(context, entities,
        UserGroupInformation.createRemoteUser("test_user"));
  }
}