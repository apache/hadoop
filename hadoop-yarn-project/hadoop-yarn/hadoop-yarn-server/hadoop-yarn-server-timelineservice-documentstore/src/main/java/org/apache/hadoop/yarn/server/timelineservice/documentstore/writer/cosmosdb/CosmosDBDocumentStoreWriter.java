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


import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.cosmosdb.AccessCondition;
import com.microsoft.azure.cosmosdb.AccessConditionType;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
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
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This is the Document Store Writer implementation for
 * {@link DocumentStoreVendor#COSMOS_DB}.
 */
public class CosmosDBDocumentStoreWriter<TimelineDoc extends TimelineDocument>
    implements DocumentStoreWriter<TimelineDoc> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CosmosDBDocumentStoreWriter.class);

  private final String databaseName;
  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();

  private static AsyncDocumentClient client;
  // creating thread pool of size equal to number of collection types
  private ExecutorService executorService =
      Executors.newFixedThreadPool(CollectionType.values().length);
  private Scheduler schedulerForBlockingWork =
      Schedulers.from(executorService);

  private static final String DATABASE_LINK = "/dbs/%s";
  private static final String COLLECTION_LINK = DATABASE_LINK + "/colls/%s";
  private static final String DOCUMENT_LINK = COLLECTION_LINK + "/docs/%s";
  private static final String ID = "@id";
  private static final String QUERY_COLLECTION_IF_EXISTS = "SELECT * FROM r " +
      "where r.id = " + ID;

  public CosmosDBDocumentStoreWriter(Configuration conf) {
    LOG.info("Initializing Cosmos DB DocumentStoreWriter...");
    databaseName = DocumentStoreUtils.getCosmosDBDatabaseName(conf);
    initCosmosDBClient(conf);
  }

  private synchronized void initCosmosDBClient(Configuration conf) {
    // making CosmosDB Async Client Singleton
    if (client == null) {
      LOG.info("Creating Cosmos DB Writer Async Client...");
      client = DocumentStoreUtils.createCosmosDBAsyncClient(conf);
      addShutdownHook();
    }
  }

  @Override
  public void createDatabase() {
    Observable<ResourceResponse<Database>> databaseReadObs =
        client.readDatabase(String.format(DATABASE_LINK, databaseName), null);

    Observable<ResourceResponse<Database>> databaseExistenceObs =
        databaseReadObs
            .doOnNext(databaseResourceResponse ->
                LOG.info("Database {} already exists.", databaseName))
            .onErrorResumeNext(throwable -> {
              // if the database doesn't exists
              // readDatabase() will result in 404 error
              if (throwable instanceof DocumentClientException) {
                DocumentClientException de =
                    (DocumentClientException) throwable;
                if (de.getStatusCode() == 404) {
                  // if the database doesn't exist, create it.
                  LOG.info("Creating new Database : {}", databaseName);

                  Database dbDefinition = new Database();
                  dbDefinition.setId(databaseName);

                  return client.createDatabase(dbDefinition, null);
                }
              }
              // some unexpected failure in reading database happened.
              // pass the error up.
              LOG.error("Reading database : {} if it exists failed.",
                  databaseName, throwable);
              return Observable.error(throwable);
            });
    // wait for completion
    databaseExistenceObs.toCompletable().await();
  }

  @Override
  public void createCollection(final String collectionName) {
    LOG.info("Creating Timeline Collection : {} for Database : {}",
        collectionName, databaseName);
    client.queryCollections(String.format(DATABASE_LINK, databaseName),
        new SqlQuerySpec(QUERY_COLLECTION_IF_EXISTS,
            new SqlParameterCollection(
                new SqlParameter(ID, collectionName))), null)
        .single() // there should be single page of result
        .flatMap((Func1<FeedResponse<DocumentCollection>, Observable<?>>)
            page -> {
            if (page.getResults().isEmpty()) {
              // if there is no matching collection create one.
              DocumentCollection collection = new DocumentCollection();
              collection.setId(collectionName);
              LOG.info("Creating collection {}", collectionName);
              return client.createCollection(
                  String.format(DATABASE_LINK, databaseName),
                  collection, null);
            } else {
              // collection already exists, nothing else to be done.
              LOG.info("Collection {} already exists.", collectionName);
              return Observable.empty();
            }
          })
        .doOnError(throwable -> LOG.error("Unable to create collection : {}",
            collectionName, throwable))
        .toCompletable().await();
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

    final TimelineDoc updatedTimelineDoc = applyUpdatesOnPrevDoc(collectionType,
        timelineDoc, eTagStrBuilder);

    accessCondition.setCondition(eTagStrBuilder.toString());
    accessCondition.setType(AccessConditionType.IfMatch);
    requestOptions.setAccessCondition(accessCondition);

    ResourceResponse<Document> resourceResponse =
        client.upsertDocument(collectionLink, updatedTimelineDoc,
            requestOptions, true)
            .subscribeOn(schedulerForBlockingWork)
            .doOnError(throwable ->
                LOG.error("Error while upserting Collection : {} " +
                    "with Doc Id : {} under Database : {}",
                collectionType.getCollectionName(),
                updatedTimelineDoc.getId(), databaseName, throwable))
            .toBlocking()
            .single();

    if (resourceResponse.getStatusCode() == 409) {
      LOG.warn("There was a conflict while upserting, hence retrying...",
          resourceResponse);
      upsertDocument(collectionType, updatedTimelineDoc);
    } else if (resourceResponse.getStatusCode() >= 200 && resourceResponse
        .getStatusCode() < 300) {
      LOG.debug("Successfully wrote doc with id : {} and type : {} under " +
          "Database : {}", timelineDoc.getId(), timelineDoc.getType(),
          databaseName);
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  TimelineDoc applyUpdatesOnPrevDoc(CollectionType collectionType,
      TimelineDoc timelineDoc, StringBuilder eTagStrBuilder) {
    TimelineDoc prevDocument = fetchLatestDoc(collectionType,
        timelineDoc.getId(), eTagStrBuilder);
    if (prevDocument != null) {
      prevDocument.merge(timelineDoc);
      timelineDoc = prevDocument;
    }
    return timelineDoc;
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  TimelineDoc fetchLatestDoc(final CollectionType collectionType,
      final String documentId, StringBuilder eTagStrBuilder) {
    final String documentLink = String.format(DOCUMENT_LINK, databaseName,
        collectionType.getCollectionName(), documentId);
    try {
      Document latestDocument = client.readDocument(documentLink, new
          RequestOptions()).toBlocking().single().getResource();
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
      LOG.info("Closing Cosmos DB Writer Async Client...");
      client.close();
      client = null;
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (executorService != null) {
        executorService.shutdown();
      }
    }));
  }
}