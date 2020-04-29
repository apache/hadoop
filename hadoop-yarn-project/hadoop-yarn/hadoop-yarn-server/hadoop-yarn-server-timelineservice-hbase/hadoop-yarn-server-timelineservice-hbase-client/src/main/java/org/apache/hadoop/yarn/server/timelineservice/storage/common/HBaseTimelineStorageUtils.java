/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A bunch of utility functions used in HBase TimelineService backend.
 */
public final class HBaseTimelineStorageUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(HBaseTimelineStorageUtils.class);

  private HBaseTimelineStorageUtils() {
  }

  /**
   * @param conf YARN configuration. Used to see if there is an explicit config
   *          pointing to the HBase config file to read. It should not be null
   *          or a NullPointerException will be thrown.
   * @return a configuration with the HBase configuration from the classpath,
   *         optionally overwritten by the timeline service configuration URL if
   *         specified.
   * @throws IOException if a timeline service HBase configuration URL
   *           is specified but unable to read it.
   */
  public static Configuration getTimelineServiceHBaseConf(Configuration conf)
      throws IOException {
    if (conf == null) {
      throw new NullPointerException();
    }

    Configuration hbaseConf;
    String timelineServiceHBaseConfFilePath =
        conf.get(YarnConfiguration.TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE);

    if (timelineServiceHBaseConfFilePath != null
          && timelineServiceHBaseConfFilePath.length() > 0) {
      LOG.info("Using hbase configuration at " +
          timelineServiceHBaseConfFilePath);
      // create a clone so that we don't mess with out input one
      hbaseConf = new Configuration(conf);
      Configuration plainHBaseConf = new Configuration(false);
      Path hbaseConfigPath = new Path(timelineServiceHBaseConfFilePath);
      try (FileSystem fs =
          FileSystem.newInstance(hbaseConfigPath.toUri(), conf);
          FSDataInputStream in = fs.open(hbaseConfigPath)) {
        plainHBaseConf.addResource(in);
        HBaseConfiguration.merge(hbaseConf, plainHBaseConf);
      }
    } else {
      // default to what is on the classpath
      hbaseConf = HBaseConfiguration.create(conf);
    }
    return hbaseConf;
  }

  /**
   * Given a row key prefix stored in a byte array, return a byte array for its
   * immediate next row key.
   *
   * @param rowKeyPrefix The provided row key prefix, represented in an array.
   * @return the closest next row key of the provided row key.
   */
  public static byte[] calculateTheClosestNextRowKeyForPrefix(
      byte[] rowKeyPrefix) {
    // Essentially we are treating it like an 'unsigned very very long' and
    // doing +1 manually.
    // Search for the place where the trailing 0xFFs start
    int offset = rowKeyPrefix.length;
    while (offset > 0) {
      if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      // We got an 0xFFFF... (only FFs) stopRow value which is
      // the last possible prefix before the end of the table.
      // So set it to stop at the 'end of the table'
      return HConstants.EMPTY_END_ROW;
    }

    // Copy the right length of the original
    byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
    // And increment the last one
    newStopRow[newStopRow.length - 1]++;
    return newStopRow;
  }

  public static void setMetricsTimeRange(Query query, byte[] metricsCf,
      long tsBegin, long tsEnd) {
    if (tsBegin != 0 || tsEnd != Long.MAX_VALUE) {
      query.setColumnFamilyTimeRange(metricsCf,
          tsBegin, ((tsEnd == Long.MAX_VALUE) ? Long.MAX_VALUE : (tsEnd + 1)));
    }
  }
}
