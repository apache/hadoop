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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.writer;

import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;

/**
 * Dummy Document Store Writer for mocking backend calls for unit test.
 */
public class DummyDocumentStoreWriter<Document extends TimelineDocument>
    implements DocumentStoreWriter<Document> {

  @Override
  public void createDatabase() {
  }

  @Override
  public void createCollection(String collectionName) {
  }

  @Override
  public void writeDocument(Document timelineDocument,
      CollectionType collectionType) {
  }

  @Override
  public void close() {
  }
}