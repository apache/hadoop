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


import com.microsoft.azure.documentdb.AccessCondition;
import com.microsoft.azure.documentdb.AccessConditionType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RequestOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.timelineservice.metrics.PerNodeAggTimelineCollectorMetrics;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.DocumentStoreWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the Document Store Writer implementation for
 * {@link DocumentStoreVendor#COSMOS_DB}.
 */
public class CosmosDBDocumentStoreWriter<TimelineDoc extends TimelineDocument>
    implements DocumentStoreWriter<TimelineDoc> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CosmosDBDocumentStoreWriter.class);

  private static volatile DocumentClient client;
  private final String databaseName;
  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();
  private static final String DATABASE_LINK = "/dbs/%s";
  private static final String COLLECTION_LINK = DATABASE_LINK + "/colls/%s";
  private static final String DOCUMENT_LINK = COLLECTION_LINK + "/docs/%s";

  public CosmosDBDocumentStoreWriter(Configuration conf) {
    LOG.info("Initializing Cosmos DB DocumentStoreWriter...");
    databaseName = DocumentStoreUtils.getCosmosDBDatabaseName(conf);
    // making CosmosDB Client Singleton
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          LOG.info("Creating Cosmos DB Client...");
          client = DocumentStoreUtils.createCosmosDBClient(conf);
        }
      }
    }
  }

  @Override
  public void createDatabase() {
    try {
      client.readDatabase(String.format(
          DATABASE_LINK, databaseName), new RequestOptions());
      LOG.info("Database {} already exists.", databaseName);
    } catch (DocumentClientException docExceptionOnRead) {
      if (docExceptionOnRead.getStatusCode() ==  404) {
        LOG.info("Creating new Database : {}", databaseName);
        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseName);
        try {
          client.createDatabase(databaseDefinition, new RequestOptions());
        } catch (DocumentClientException docExceptionOnCreate) {
          LOG.error("Unable to create new Database : {}", databaseName,
              docExceptionOnCreate);
        }
      } else {
        LOG.error("Error while reading Database : {}", databaseName,
            docExceptionOnRead);
      }
    }
  }

  @Override
  public void createCollection(final String collectionName) {
    LOG.info("Creating Timeline Collection : {} for Database : {}",
        collectionName, databaseName);
    try {
      client.readCollection(String.format(COLLECTION_LINK, databaseName,
          collectionName), new RequestOptions());
      LOG.info("Collection {} already exists.", collectionName);
    } catch (DocumentClientException docExceptionOnRead) {
      if (docExceptionOnRead.getStatusCode() == 404) {
        DocumentCollection collection = new DocumentCollection();
        collection.setId(collectionName);
        LOG.info("Creating collection {} under Database {}",
            collectionName, databaseName);
        try {
          client.createCollection(
              String.format(DATABASE_LINK, databaseName),
              collection, new RequestOptions());
        } catch (DocumentClientException docExceptionOnCreate) {
          LOG.error("Unable to create Collection : {} under Database : {}",
              collectionName, databaseName, docExceptionOnCreate);
        }
      } else {
        LOG.error("Error while reading Collection : {} under Database : {}",
            collectionName, databaseName, docExceptionOnRead);
      }
    }
  }

  @Override
  public void writeDocument(final TimelineDoc timelineDoc,
      final CollectionType collectionType) {
    LOG.debug("Upserting document under collection : {} with  entity type : " +
        "{} under Database {}", databaseName, timelineDoc.getType(),
        collectionType.getCollectionName());
    boolean succeeded = false;
    long startTime = Time.monotonicNow();
    try {
      upsertDocument(collectionType, timelineDoc);
      succeeded = true;
    } catch (Exception e) {
      LOG.error("Unable to perform upsert for Document Id : {} under " +
          "Collection : {} under Database {}", timelineDoc.getId(),
          collectionType.getCollectionName(), databaseName, e);
    } finally {
      long latency = Time.monotonicNow() - startTime;
      METRICS.addPutEntitiesLatency(latency, succeeded);
    }
  }

  @SuppressWarnings("unchecked")
  private void upsertDocument(final  CollectionType collectionType,
      final TimelineDoc timelineDoc) {
    final String collectionLink = String.format(COLLECTION_LINK, databaseName,
        collectionType.getCollectionName());
    RequestOptions requestOptions  = new RequestOptions();
    AccessCondition accessCondition = new AccessCondition();
    StringBuilder eTagStrBuilder = new StringBuilder();

    TimelineDoc updatedTimelineDoc = applyUpdatesOnPrevDoc(collectionType,
        timelineDoc, eTagStrBuilder);

    accessCondition.setCondition(eTagStrBuilder.toString());
    accessCondition.setType(AccessConditionType.IfMatch);
    requestOptions.setAccessCondition(accessCondition);

    try {
      client.upsertDocument(collectionLink, updatedTimelineDoc,
          requestOptions, true);
      LOG.debug("Successfully wrote doc with id : {} and type : {} under " +
          "Database : {}", timelineDoc.getId(), timelineDoc.getType(),
          databaseName);
    } catch (DocumentClientException e) {
      if (e.getStatusCode() == 409) {
        LOG.warn("There was a conflict while upserting, hence retrying...", e);
        upsertDocument(collectionType, updatedTimelineDoc);
      }
      LOG.error("Error while upserting Collection : {} with Doc Id : {} under" +
          " Database : {}", collectionType.getCollectionName(),
          updatedTimelineDoc.getId(), databaseName, e);
    }
  }

  @SuppressWarnings("unchecked")
  private TimelineDoc applyUpdatesOnPrevDoc(CollectionType collectionType,
      TimelineDoc timelineDoc, StringBuilder eTagStrBuilder) {
    TimelineDoc prevDocument = fetchLatestDoc(collectionType,
        timelineDoc.getId(), eTagStrBuilder);
    if (prevDocument != null) {
      prevDocument.merge(timelineDoc);
      timelineDoc = prevDocument;
    }
    return timelineDoc;
  }

  @SuppressWarnings("unchecked")
  private TimelineDoc fetchLatestDoc(final CollectionType collectionType,
      final String documentId, StringBuilder eTagStrBuilder) {
    final String documentLink = String.format(DOCUMENT_LINK, databaseName,
        collectionType.getCollectionName(), documentId);
    try {
      Document latestDocument = client.readDocument(documentLink, new
          RequestOptions()).getResource();
      TimelineDoc timelineDoc;
      switch (collectionType) {
      case FLOW_RUN:
        timelineDoc = (TimelineDoc) latestDocument.toObject(
            FlowRunDocument.class);
        break;
      case FLOW_ACTIVITY:
        timelineDoc = (TimelineDoc) latestDocument.toObject(FlowActivityDocument
            .class);
        break;
      default:
        timelineDoc = (TimelineDoc) latestDocument.toObject(
            TimelineEntityDocument.class);
      }
      eTagStrBuilder.append(latestDocument.getETag());
      return timelineDoc;
    } catch (Exception e) {
      LOG.debug("No previous Document found with id : {} for Collection" +
          " : {} under Database : {}", documentId, collectionType
          .getCollectionName(), databaseName);
      return null;
    }
  }

  @Override
  public synchronized void close() {
    if (client != null) {
      LOG.info("Closing Cosmos DB Client...");
      client.close();
      client = null;
    }
  }
}