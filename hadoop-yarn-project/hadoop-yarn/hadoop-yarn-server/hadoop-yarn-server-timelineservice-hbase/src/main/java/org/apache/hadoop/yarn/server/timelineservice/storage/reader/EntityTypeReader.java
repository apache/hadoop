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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * Timeline entity reader for listing all available entity types given one
 * reader context. Right now only supports listing all entity types within one
 * YARN application.
 */
public final class EntityTypeReader extends AbstractTimelineStorageReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(EntityTypeReader.class);
  private static final EntityTable ENTITY_TABLE = new EntityTable();

  public EntityTypeReader(TimelineReaderContext context) {
    super(context);
  }

  /**
   * Reads a set of timeline entity types from the HBase storage for the given
   * context.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @return a set of <cite>TimelineEntity</cite> objects, with only type field
   *         set.
   * @throws IOException if any exception is encountered while reading entities.
   */
  public Set<String> readEntityTypes(Configuration hbaseConf,
      Connection conn) throws IOException {

    validateParams();
    augmentParams(hbaseConf, conn);

    Set<String> types = new TreeSet<>();
    TimelineReaderContext context = getContext();
    EntityRowKeyPrefix prefix = new EntityRowKeyPrefix(context.getClusterId(),
        context.getUserId(), context.getFlowName(), context.getFlowRunId(),
        context.getAppId());
    byte[] currRowKey = prefix.getRowKeyPrefix();
    byte[] nextRowKey = prefix.getRowKeyPrefix();
    nextRowKey[nextRowKey.length - 1]++;

    FilterList typeFilterList = new FilterList();
    typeFilterList.addFilter(new FirstKeyOnlyFilter());
    typeFilterList.addFilter(new KeyOnlyFilter());
    typeFilterList.addFilter(new PageFilter(1));
    LOG.debug("FilterList created for scan is - {}", typeFilterList);

    int counter = 0;
    while (true) {
      try (ResultScanner results =
          getResult(hbaseConf, conn, typeFilterList, currRowKey, nextRowKey)) {
        TimelineEntity entity = parseEntityForType(results.next());
        if (entity == null) {
          break;
        }
        ++counter;
        if (!types.add(entity.getType())) {
          LOG.warn("Failed to add type " + entity.getType()
              + " to the result set because there is a duplicated copy. ");
        }
        String currType = entity.getType();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Current row key: " + Arrays.toString(currRowKey));
          LOG.debug("New entity type discovered: " + currType);
        }
        currRowKey = getNextRowKey(prefix.getRowKeyPrefix(), currType);
      }
    }
    LOG.debug("Scanned {} records for {} types", counter, types.size());
    return types;
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(getContext(), "context shouldn't be null");
    Preconditions.checkNotNull(getContext().getClusterId(),
        "clusterId shouldn't be null");
    Preconditions.checkNotNull(getContext().getAppId(),
        "appId shouldn't be null");
  }

  /**
   * Gets the possibly next row key prefix given current prefix and type.
   *
   * @param currRowKeyPrefix The current prefix that contains user, cluster,
   *                         flow, run, and application id.
   * @param entityType Current entity type.
   * @return A new prefix for the possibly immediately next row key.
   */
  private static byte[] getNextRowKey(byte[] currRowKeyPrefix,
      String entityType) {
    if (currRowKeyPrefix == null || entityType == null) {
      return null;
    }

    byte[] entityTypeEncoded = Separator.QUALIFIERS.join(
        Separator.encode(entityType, Separator.SPACE, Separator.TAB,
            Separator.QUALIFIERS),
        Separator.EMPTY_BYTES);

    byte[] currRowKey
        = new byte[currRowKeyPrefix.length + entityTypeEncoded.length];
    System.arraycopy(currRowKeyPrefix, 0, currRowKey, 0,
        currRowKeyPrefix.length);
    System.arraycopy(entityTypeEncoded, 0, currRowKey, currRowKeyPrefix.length,
        entityTypeEncoded.length);

    return HBaseTimelineStorageUtils.calculateTheClosestNextRowKeyForPrefix(
        currRowKey);
  }

  private ResultScanner getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList, byte[] startPrefix, byte[] endPrefix)
      throws IOException {
    Scan scan = new Scan(startPrefix, endPrefix);
    scan.setFilter(filterList);
    scan.setSmall(true);
    return ENTITY_TABLE.getResultScanner(hbaseConf, conn, scan);
  }

  private TimelineEntity parseEntityForType(Result result)
      throws IOException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    TimelineEntity entity = new TimelineEntity();
    EntityRowKey newRowKey = EntityRowKey.parseRowKey(result.getRow());
    entity.setType(newRowKey.getEntityType());
    return entity;
  }

}
