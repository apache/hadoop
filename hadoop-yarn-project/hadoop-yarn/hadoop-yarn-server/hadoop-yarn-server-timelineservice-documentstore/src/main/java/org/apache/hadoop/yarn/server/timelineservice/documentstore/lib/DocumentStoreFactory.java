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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.DocumentStoreReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.cosmosdb.CosmosDBDocumentStoreReader;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.cosmosdb.CosmosDBDocumentStoreWriter;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;

import static org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils.getStoreVendor;

/**
 * Factory methods for instantiating a timeline Document Store reader or
 * writer. Based on the {@link DocumentStoreVendor} that is configured,
 * appropriate reader or writer would be instantiated.
 */
public final class DocumentStoreFactory {

  // making factory class not instantiable
  private DocumentStoreFactory(){
  }

  /**
   * Creates a DocumentStoreWriter for a {@link DocumentStoreVendor}.
   * @param conf
   *              for creating client connection
   * @param <Document> type of Document for which the writer has to be created,
   *                  i.e TimelineEntityDocument, FlowActivityDocument etc
   * @return document store writer
   * @throws DocumentStoreNotSupportedException if there is no implementation
   *         for a configured {@link DocumentStoreVendor} or unknown
   *         {@link DocumentStoreVendor} is configured.
   * @throws YarnException if the required configs for DocumentStore is missing.
   */
  public static <Document extends TimelineDocument>
      DocumentStoreWriter <Document> createDocumentStoreWriter(
          Configuration conf) throws YarnException {
    final DocumentStoreVendor storeType = getStoreVendor(conf);
    switch (storeType) {
    case COSMOS_DB:
      DocumentStoreUtils.validateCosmosDBConf(conf);
      return new CosmosDBDocumentStoreWriter<>(conf);
    default:
      throw new DocumentStoreNotSupportedException(
          "Unable to create DocumentStoreWriter for type : "
              + storeType);
    }
  }

  /**
 * Creates a DocumentStoreReader for a {@link DocumentStoreVendor}.
 * @param conf
 *            for creating client connection
 * @param <Document> type of Document for which the writer has to be created,
 *                  i.e TimelineEntityDocument, FlowActivityDocument etc
 * @return document store reader
 * @throws DocumentStoreNotSupportedException if there is no implementation
 *         for a configured {@link DocumentStoreVendor} or unknown
 *         {@link DocumentStoreVendor} is configured.
 * @throws YarnException if the required configs for DocumentStore is missing.
 * */
  public static <Document extends TimelineDocument>
      DocumentStoreReader<Document> createDocumentStoreReader(
          Configuration conf) throws YarnException {
    final DocumentStoreVendor storeType = getStoreVendor(conf);
    switch (storeType) {
    case COSMOS_DB:
      DocumentStoreUtils.validateCosmosDBConf(conf);
      return new CosmosDBDocumentStoreReader<>(conf);
    default:
      throw new DocumentStoreNotSupportedException(
          "Unable to create DocumentStoreReader for type : "
              + storeType);
    }
  }
}