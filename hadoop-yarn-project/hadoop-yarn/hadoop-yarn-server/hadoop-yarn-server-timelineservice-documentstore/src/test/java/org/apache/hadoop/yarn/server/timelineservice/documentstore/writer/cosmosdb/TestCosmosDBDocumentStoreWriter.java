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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.cosmosdb;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreTestUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Test case for {@link CosmosDBDocumentStoreWriter}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DocumentStoreUtils.class)
public class TestCosmosDBDocumentStoreWriter {

  @Before
  public void setUp() {
    AsyncDocumentClient asyncDocumentClient =
        Mockito.mock(AsyncDocumentClient.class);
    PowerMockito.mockStatic(DocumentStoreUtils.class);
    PowerMockito.when(DocumentStoreUtils.getCosmosDBDatabaseName(
        ArgumentMatchers.any(Configuration.class)))
        .thenReturn("FooBar");
    PowerMockito.when(DocumentStoreUtils.createCosmosDBAsyncClient(
        ArgumentMatchers.any(Configuration.class)))
        .thenReturn(asyncDocumentClient);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void applyingUpdatesOnPrevDocTest() throws IOException {
    MockedCosmosDBDocumentStoreWriter documentStoreWriter =
        new MockedCosmosDBDocumentStoreWriter(null);

    TimelineEntityDocument actualEntityDoc =
        new TimelineEntityDocument();
    TimelineEntityDocument expectedEntityDoc =
        DocumentStoreTestUtils.bakeTimelineEntityDoc();

    Assert.assertEquals(1, actualEntityDoc.getInfo().size());
    Assert.assertEquals(0, actualEntityDoc.getMetrics().size());
    Assert.assertEquals(0, actualEntityDoc.getEvents().size());
    Assert.assertEquals(0, actualEntityDoc.getConfigs().size());
    Assert.assertEquals(0,
        actualEntityDoc.getIsRelatedToEntities().size());
    Assert.assertEquals(0, actualEntityDoc.
        getRelatesToEntities().size());

    actualEntityDoc = (TimelineEntityDocument) documentStoreWriter
        .applyUpdatesOnPrevDoc(CollectionType.ENTITY,
            actualEntityDoc, null);

    Assert.assertEquals(expectedEntityDoc.getInfo().size(),
        actualEntityDoc.getInfo().size());
    Assert.assertEquals(expectedEntityDoc.getMetrics().size(),
        actualEntityDoc.getMetrics().size());
    Assert.assertEquals(expectedEntityDoc.getEvents().size(),
        actualEntityDoc.getEvents().size());
    Assert.assertEquals(expectedEntityDoc.getConfigs().size(),
        actualEntityDoc.getConfigs().size());
    Assert.assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getIsRelatedToEntities().size());
    Assert.assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getRelatesToEntities().size());
  }
}