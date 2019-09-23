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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DummyDocumentStoreWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test case for ${@link DocumentStoreCollectionCreator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DocumentStoreFactory.class)
public class TestDocumentStoreCollectionCreator {

  private final DocumentStoreWriter<TimelineDocument> documentStoreWriter = new
      DummyDocumentStoreWriter<>();
  private final Configuration conf = new Configuration();

  @Before
  public void setUp() throws YarnException {
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_DOCUMENTSTORE_DATABASE_NAME,
        "TestDB");
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_COSMOSDB_ENDPOINT,
        "https://localhost:443");
    conf.set(DocumentStoreUtils.TIMELINE_SERVICE_COSMOSDB_MASTER_KEY,
        "1234567");
    PowerMockito.mockStatic(DocumentStoreFactory.class);
    PowerMockito.when(DocumentStoreFactory.createDocumentStoreWriter(
        ArgumentMatchers.any(Configuration.class)))
        .thenReturn(documentStoreWriter);
  }

  @Test
  public void collectionCreatorTest() {
    new DocumentStoreCollectionCreator().createTimelineSchema(new String[]{});
  }
}
