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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;
import org.apache.hadoop.yarn.server.timelineservice.storage.SchemaCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This creates the Collection for a {@link DocumentStoreVendor} backend
 * configured for storing  application timeline information.
 */
public class DocumentStoreCollectionCreator implements SchemaCreator {

  private static final Logger LOG = LoggerFactory
      .getLogger(DocumentStoreCollectionCreator.class);


  @Override
  public void createTimelineSchema(String[] args) {
    try {

      Configuration conf = new YarnConfiguration();

      LOG.info("Creating database and collections for DocumentStore : {}",
          DocumentStoreUtils.getStoreVendor(conf));

      try(DocumentStoreWriter documentStoreWriter = DocumentStoreFactory
          .createDocumentStoreWriter(conf)) {
        documentStoreWriter.createDatabase();
        documentStoreWriter.createCollection(
            CollectionType.APPLICATION.getCollectionName());
        documentStoreWriter.createCollection(
            CollectionType.ENTITY.getCollectionName());
        documentStoreWriter.createCollection(
            CollectionType.FLOW_ACTIVITY.getCollectionName());
        documentStoreWriter.createCollection(
            CollectionType.FLOW_RUN.getCollectionName());
      }
    } catch (Exception e) {
      LOG.error("Error while creating Timeline Collections", e);
    }
  }
}