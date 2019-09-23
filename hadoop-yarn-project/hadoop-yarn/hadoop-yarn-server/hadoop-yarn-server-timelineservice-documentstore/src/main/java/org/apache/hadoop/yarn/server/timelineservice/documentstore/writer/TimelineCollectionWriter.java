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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreFactory;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.lib.DocumentStoreVendor;
import org.apache.hadoop.yarn.server.timelineservice.metrics.PerNodeAggTimelineCollectorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a generic Collection Writer that can be used for writing documents
 * belonging to different {@link CollectionType} under a specific
 * {@link DocumentStoreVendor} backend.
 */
public class TimelineCollectionWriter<Document extends TimelineDocument> {

  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineCollectionWriter.class);

  private final static String DOCUMENT_BUFFER_SIZE_CONF =
      "yarn.timeline-service.document-buffer.size";
  private static final int DEFAULT_BUFFER_SIZE = 1024;
  private static final int AWAIT_TIMEOUT_SECS = 5;
  private static final PerNodeAggTimelineCollectorMetrics METRICS =
      PerNodeAggTimelineCollectorMetrics.getInstance();

  private final CollectionType collectionType;
  private final DocumentStoreWriter<Document> documentStoreWriter;
  private final Map<String, Document> documentsBuffer;
  private final int maxBufferSize;
  private final ScheduledExecutorService scheduledDocumentsFlusher;
  private final ExecutorService documentsBufferFullFlusher;

  public TimelineCollectionWriter(CollectionType collectionType,
      Configuration conf) throws YarnException {
    LOG.info("Initializing TimelineCollectionWriter for collection type : {}",
        collectionType);
    int flushIntervalSecs = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS,
        YarnConfiguration
            .DEFAULT_TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS);
    maxBufferSize = conf.getInt(DOCUMENT_BUFFER_SIZE_CONF, DEFAULT_BUFFER_SIZE);
    documentsBuffer = new HashMap<>(maxBufferSize);
    this.collectionType = collectionType;
    documentStoreWriter = DocumentStoreFactory.createDocumentStoreWriter(conf);
    scheduledDocumentsFlusher = Executors.newSingleThreadScheduledExecutor();
    scheduledDocumentsFlusher.scheduleAtFixedRate(this::flush,
        flushIntervalSecs, flushIntervalSecs, TimeUnit.SECONDS);
    documentsBufferFullFlusher = Executors.newSingleThreadExecutor();
  }

  @SuppressWarnings("unchecked")
  public void writeDocument(Document timelineDocument) {
    /*
     * The DocumentBuffer is used to buffer the most frequently used
     * documents for performing upserts on them, whenever either due to
     * buffer gets fulled or the scheduledDocumentsFlusher
     * invokes flush() periodically, all the buffered documents would be written
     * to DocumentStore in a background thread.
     */
    long startTime = Time.monotonicNow();

    synchronized(documentsBuffer) {
      //if buffer is full copy to flushBuffer in order to flush
      if (documentsBuffer.size() == maxBufferSize) {
        final Map<String, Document> flushedBuffer = copyToFlushBuffer();
        //flush all documents from flushBuffer in background
        documentsBufferFullFlusher.execute(() -> flush(flushedBuffer));
      }
      Document prevDocument = documentsBuffer.get(timelineDocument.getId());
      // check if Document exists inside documentsBuffer
      if (prevDocument != null) {
        prevDocument.merge(timelineDocument);
      } else { // else treat this as a new document
        prevDocument = timelineDocument;
      }
      documentsBuffer.put(prevDocument.getId(), prevDocument);
    }
    METRICS.addAsyncPutEntitiesLatency(Time.monotonicNow() - startTime,
        true);
  }

  private Map<String, Document> copyToFlushBuffer() {
    Map<String, Document> flushBuffer = new HashMap<>();
    synchronized(documentsBuffer) {
      if (documentsBuffer.size() > 0) {
        flushBuffer.putAll(documentsBuffer);
        documentsBuffer.clear();
      }
    }
    return flushBuffer;
  }

  private void flush(Map<String, Document> flushBuffer) {
    for (Document document : flushBuffer.values()) {
      documentStoreWriter.writeDocument(document, collectionType);
    }
  }

  public void flush() {
    flush(copyToFlushBuffer());
  }

  public void close() throws Exception {
    scheduledDocumentsFlusher.shutdown();
    documentsBufferFullFlusher.shutdown();

    flush();

    scheduledDocumentsFlusher.awaitTermination(
        AWAIT_TIMEOUT_SECS, TimeUnit.SECONDS);
    documentsBufferFullFlusher.awaitTermination(
        AWAIT_TIMEOUT_SECS, TimeUnit.SECONDS);
    documentStoreWriter.close();
  }
}