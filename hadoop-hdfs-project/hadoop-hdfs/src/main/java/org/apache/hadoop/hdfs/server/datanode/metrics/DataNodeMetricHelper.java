/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.Interns;

import java.io.IOException;

public class DataNodeMetricHelper {

  /**
   * Get metrics helper provides Helper function for
   * metrics2 interface to act as a Metric source
   *
   * @param collector Metrics Collector that is passed in
   * @param beanClass The Class that currently impliments the metric functions
   * @param context A string that idenitifies the context
   *
   * @throws IOException
   */
  public static void getMetrics(MetricsCollector collector,
                                FSDatasetMBean beanClass, String context)
    throws IOException {

    if (beanClass == null) {
      throw new IOException("beanClass cannot be null");
    }

    String className = beanClass.getClass().getName();

    collector.addRecord(className)
      .setContext(context)
      .addGauge(Interns.info("Capacity", "Total storage capacity"),
        beanClass.getCapacity())
      .addGauge(Interns.info("DfsUsed", "Total bytes used by dfs datanode"),
        beanClass.getDfsUsed())
      .addGauge(Interns.info("Remaining", "Total bytes of free storage"),
        beanClass.getRemaining())
      .add(new MetricsTag(Interns.info("StorageInfo", "Storage ID"),
        beanClass.getStorageInfo()))
      .addGauge(Interns.info("NumFailedVolumes", "Number of failed Volumes" +
        " in the data Node"), beanClass.getNumFailedVolumes())
      .addGauge(Interns.info("LastVolumeFailureDate", "Last Volume failure in" +
        " milliseconds from epoch"), beanClass.getLastVolumeFailureDate())
      .addGauge(Interns.info("EstimatedCapacityLostTotal", "Total capacity lost"
        + " due to volume failure"), beanClass.getEstimatedCapacityLostTotal())
      .addGauge(Interns.info("CacheUsed", "Datanode cache used in bytes"),
        beanClass.getCacheUsed())
      .addGauge(Interns.info("CacheCapacity", "Datanode cache capacity"),
        beanClass.getCacheCapacity())
      .addGauge(Interns.info("NumBlocksCached", "Datanode number" +
        " of blocks cached"), beanClass.getNumBlocksCached())
      .addGauge(Interns.info("NumBlocksFailedToCache", "Datanode number of " +
        "blocks failed to cache"), beanClass.getNumBlocksFailedToCache())
        .addGauge(Interns.info("NumBlocksFailedToUnCache", "Datanode number of" +
          " blocks failed in cache eviction"),
        beanClass.getNumBlocksFailedToUncache())
        .addGauge(Interns.info("LastDirectoryScannerFinishTime",
        "Finish time of the last directory scan"), beanClass.getLastDirScannerFinishTime())
        .addGauge(Interns.info("PendingAsyncDeletions",
            "The count of pending and running asynchronous disk operations"),
            beanClass.getPendingAsyncDeletions());
  }

}
