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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.cosmosdb;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.NoDocumentFoundException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.reader.DocumentStoreReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * This is the Document Store Reader implementation for
 * {@link DocumentStoreVendor#COSMOS_DB}.
 */
public class CosmosDBDocumentStoreReader<TimelineDoc extends TimelineDocument>
    implements DocumentStoreReader<TimelineDoc> {

  private static final Logger LOG = LoggerFactory
      .getLogger(CosmosDBDocumentStoreReader.class);
  private static final int DEFAULT_DOCUMENTS_SIZE = 1;

  private static volatile DocumentClient client;
  private final String databaseName;
  private final static String COLLECTION_LINK = "/dbs/%s/colls/%s";
  private final static String SELECT_TOP_FROM_COLLECTION = "SELECT TOP %d * " +
      "FROM %s c";
  private final static String SELECT_ALL_FROM_COLLECTION =
      "SELECT  * FROM %s c";
  private final static String SELECT_DISTINCT_TYPES_FROM_COLLECTION =
      "SELECT  distinct c.type FROM %s c";
  private static final String ENTITY_TYPE_COLUMN = "type";
  private final static String WHERE_CLAUSE = " WHERE ";
  private final static String AND_OPERATOR = " AND ";
  private final static String CONTAINS_FUNC_FOR_ID = " CONTAINS(c.id, \"%s\") ";
  private final static String CONTAINS_FUNC_FOR_TYPE = " CONTAINS(c.type, " +
      "\"%s\") ";
  private final static String ORDER_BY_CLAUSE = " ORDER BY c.createdTime";

  public CosmosDBDocumentStoreReader(Configuration conf) {
    LOG.info("Initializing Cosmos DB DocumentStoreReader...");
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
  public List<TimelineDoc> readDocumentList(String collectionName,
      TimelineReaderContext context, final Class<TimelineDoc> timelineDocClass,
      long size) throws NoDocumentFoundException {
    final List<TimelineDoc> result = queryDocuments(collectionName,
        context, timelineDocClass, size);
    if (result.size() > 0) {
      return result;
    }
    throw new NoDocumentFoundException("No documents were found while " +
        "querying Collection : " + collectionName);
  }

  @Override
  public Set<String> fetchEntityTypes(String collectionName,
      TimelineReaderContext context) {
    StringBuilder queryStrBuilder = new StringBuilder();
    queryStrBuilder.append(
        String.format(SELECT_DISTINCT_TYPES_FROM_COLLECTION, collectionName));
    String sqlQuery = addPredicates(context, collectionName, queryStrBuilder);

    LOG.debug("Querying Collection : {} , with query {}", collectionName,
        sqlQuery);

    Set<String> entityTypes = new HashSet<>();
    Iterator<Document> documentIterator = client.queryDocuments(
        String.format(COLLECTION_LINK, databaseName, collectionName),
        sqlQuery, null).getQueryIterator();
    while (documentIterator.hasNext()) {
      Document document = documentIterator.next();
      entityTypes.add(document.getString(ENTITY_TYPE_COLUMN));
    }
    return entityTypes;
  }

  @Override
  public TimelineDoc readDocument(String collectionName, TimelineReaderContext
      context, final Class<TimelineDoc> timelineDocClass)
      throws  NoDocumentFoundException {
    final List<TimelineDoc> result = queryDocuments(collectionName,
        context, timelineDocClass, DEFAULT_DOCUMENTS_SIZE);
    if(result.size() > 0) {
      return result.get(0);
    }
    throw new NoDocumentFoundException("No documents were found while " +
        "querying Collection : " + collectionName);
  }

  private List<TimelineDoc> queryDocuments(String collectionName,
      TimelineReaderContext context, final Class<TimelineDoc> docClass,
      final long maxDocumentsSize) {
    final String sqlQuery = buildQueryWithPredicates(context, collectionName,
        maxDocumentsSize);
    List<TimelineDoc> timelineDocs = new ArrayList<>();
    LOG.debug("Querying Collection : {} , with query {}", collectionName,
        sqlQuery);

    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setPageSize((int) maxDocumentsSize);
    Iterator<Document> documentIterator = client.queryDocuments(
        String.format(COLLECTION_LINK, databaseName, collectionName),
        sqlQuery, feedOptions).getQueryIterator();
    while (documentIterator.hasNext()) {
      Document document = documentIterator.next();
      TimelineDoc resultDoc = document.toObject(docClass);
      if (resultDoc.getCreatedTime() == 0 &&
          document.getTimestamp() != null) {
        resultDoc.setCreatedTime(document.getTimestamp().getTime());
      }
      timelineDocs.add(resultDoc);
    }
    return timelineDocs;
  }

  private String buildQueryWithPredicates(TimelineReaderContext context,
      String collectionName, long size) {
    StringBuilder queryStrBuilder = new StringBuilder();
    if (size == -1) {
      queryStrBuilder.append(String.format(SELECT_ALL_FROM_COLLECTION,
          collectionName));
    } else {
      queryStrBuilder.append(String.format(SELECT_TOP_FROM_COLLECTION, size,
          collectionName));
    }

    return addPredicates(context, collectionName, queryStrBuilder);
  }

  private String addPredicates(TimelineReaderContext context,
      String collectionName, StringBuilder queryStrBuilder) {
    boolean hasPredicate = false;

    queryStrBuilder.append(WHERE_CLAUSE);

    if (context.getClusterId() != null) {
      hasPredicate = true;
      queryStrBuilder.append(String.format(CONTAINS_FUNC_FOR_ID,
          context.getClusterId()));
    }
    if (context.getUserId() != null) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getUserId()));
    }
    if (context.getFlowName() != null) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getFlowName()));
    }
    if (context.getAppId() != null) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getAppId()));
    }
    if (context.getEntityId() != null) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getEntityId()));
    }
    if (context.getFlowRunId() != null) {
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_ID, context.getFlowRunId()));
    }
    if (context.getEntityType() != null){
      hasPredicate = true;
      queryStrBuilder.append(AND_OPERATOR)
          .append(String.format(CONTAINS_FUNC_FOR_TYPE,
              context.getEntityType()));
    }

    if (hasPredicate) {
      queryStrBuilder.append(ORDER_BY_CLAUSE);
      LOG.debug("CosmosDB Sql Query with predicates : {}", queryStrBuilder);
      return queryStrBuilder.toString();
    }
    throw new IllegalArgumentException("The TimelineReaderContext does not " +
        "have enough information to query documents for Collection : " +
        collectionName);
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