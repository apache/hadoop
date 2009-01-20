/**
 * Copyright 2008 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The Region Historian task is to keep track of every modification a region
 * has to go through. Public methods are used to update the information in the
 * <code>.META.</code> table and to retrieve it.  This is a Singleton.  By
 * default, the Historian is offline; it will not log.  Its enabled in the
 * regionserver and master down in their guts after there's some certainty the
 * .META. has been deployed.
 */
public class RegionHistorian implements HConstants {
  private static final Log LOG = LogFactory.getLog(RegionHistorian.class);
  
  private HTable metaTable;



  /** Singleton reference */
  private static RegionHistorian historian;

  /** Date formater for the timestamp in RegionHistoryInformation */
  static SimpleDateFormat dateFormat = new SimpleDateFormat(
  "EEE, d MMM yyyy HH:mm:ss");

  
  private static enum HistorianColumnKey  {
    REGION_CREATION ( Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR+"creation")),
    REGION_OPEN ( Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR+"open")),
    REGION_SPLIT ( Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR+"split")),
    REGION_COMPACTION ( Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR+"compaction")),
    REGION_FLUSH ( Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR+"flush")),
    REGION_ASSIGNMENT ( Bytes.toBytes(COLUMN_FAMILY_HISTORIAN_STR+"assignment"));

  byte[] key;

    HistorianColumnKey(byte[] key) {
      this.key = key;
    }
  } 
  
  public static final String SPLIT_PREFIX = "Region split from: ";

  /**
   * Default constructor. Initializes reference to .META. table.  Inaccessible.
   * Use {@link #getInstance(HBaseConfiguration)} to obtain the Singleton
   * instance of this class.
   */
  private RegionHistorian() {
    super();
  }

  /**
   * Get the RegionHistorian Singleton instance.
   * @return The region historian
   */
  public synchronized static RegionHistorian getInstance() {
    if (historian == null) {
      historian = new RegionHistorian();
    }
    return historian;
  }

  /**
   * Returns, for a given region name, an ordered list by timestamp of all
   * values in the historian column of the .META. table.
   * @param regionName
   *          Region name as a string
   * @return List of RegionHistoryInformation or null if we're offline.
   */
  public List<RegionHistoryInformation> getRegionHistory(String regionName) {
    if (!isOnline()) {
      return null;
    }
    List<RegionHistoryInformation> informations =
      new ArrayList<RegionHistoryInformation>();
    try {
      /*
       * TODO REGION_HISTORIAN_KEYS is used because there is no other for the
       * moment to retrieve all version and to have the column key information.
       * To be changed when HTable.getRow handles versions.
       */
      for (HistorianColumnKey keyEnu : HistorianColumnKey.values()) {
        byte[] columnKey = keyEnu.key;
        Cell[] cells = this.metaTable.get(Bytes.toBytes(regionName),
            columnKey, ALL_VERSIONS);
        if (cells != null) {
          for (Cell cell : cells) {
            informations.add(historian.new RegionHistoryInformation(cell
                .getTimestamp(), Bytes.toString(columnKey).split(":")[1], Bytes
                .toString(cell.getValue())));
          }
        }
      }
    } catch (IOException ioe) {
      LOG.warn("Unable to retrieve region history", ioe);
    }
    Collections.sort(informations);
    return informations;
  }
  
  /**
   * Method to add a creation event to the row in the .META table
   * @param info
   * @param serverName
   */
  public void addRegionAssignment(HRegionInfo info, String serverName) {
    add(HistorianColumnKey.REGION_ASSIGNMENT.key, "Region assigned to server "
        + serverName, info);
  }

  /**
   * Method to add a creation event to the row in the .META table
   * @param info
   */
  public void addRegionCreation(HRegionInfo info) {
    add(HistorianColumnKey.REGION_CREATION.key, "Region creation", info);
  }

  /**
   * Method to add a opening event to the row in the .META table
   * @param info
   * @param address
   */
  public void addRegionOpen(HRegionInfo info, HServerAddress address) {
    add(HistorianColumnKey.REGION_OPEN.key, "Region opened on server : "
        + address.getHostname(), info);
  }

  /**
   * Method to add a split event to the rows in the .META table with
   * information from oldInfo.
   * @param oldInfo
   * @param newInfo1 
   * @param newInfo2
   */
  public void addRegionSplit(HRegionInfo oldInfo, HRegionInfo newInfo1,
     HRegionInfo newInfo2) {
    HRegionInfo[] infos = new HRegionInfo[] { newInfo1, newInfo2 };
    for (HRegionInfo info : infos) {
      add(HistorianColumnKey.REGION_SPLIT.key, SPLIT_PREFIX +
        oldInfo.getRegionNameAsString(), info);
    }
  }

  /**
   * Method to add a compaction event to the row in the .META table
   * @param info
   * @param timeTaken
   */
  public void addRegionCompaction(final HRegionInfo info,
      final String timeTaken) {
    // While historian can not log flushes because it could deadlock the
    // regionserver -- see the note in addRegionFlush -- there should be no
    // such danger compacting; compactions are not allowed when
    // Flusher#flushSomeRegions is run.
    if (LOG.isDebugEnabled()) {
      add(HistorianColumnKey.REGION_COMPACTION.key,
        "Region compaction completed in " + timeTaken, info);
    }
  }

  /**
   * Method to add a flush event to the row in the .META table
   * @param info
   * @param timeTaken
   */
  public void addRegionFlush(HRegionInfo info, String timeTaken) {
    // Disabled.  Noop.  If this regionserver is hosting the .META. AND is
    // holding the reclaimMemcacheMemory global lock --
    // see Flusher#flushSomeRegions --  we deadlock.  For now, just disable
    // logging of flushes.
  }

  /**
   * Method to add an event with LATEST_TIMESTAMP.
   * @param column
   * @param text
   * @param info
   */
  private void add(byte[] column,
      String text, HRegionInfo info) {
    add(column, text, info, LATEST_TIMESTAMP);
  }

  /**
   * Method to add an event with provided information.
   * @param column
   * @param text
   * @param info
   * @param timestamp
   */
  private void add(byte[] column,
      String text, HRegionInfo info, long timestamp) {
    if (!isOnline()) {
      // Its a noop
      return;
    }
    if (!info.isMetaRegion()) {
      BatchUpdate batch = new BatchUpdate(info.getRegionName());
      batch.setTimestamp(timestamp);
      batch.put(column, Bytes.toBytes(text));
      try {
        this.metaTable.commit(batch);
      } catch (IOException ioe) {
        LOG.warn("Unable to '" + text + "'", ioe);
      }
    }
  }

  /**
   * Inner class that only contains information about an event.
   * 
   */
  public class RegionHistoryInformation implements
  Comparable<RegionHistoryInformation> {
    
    private GregorianCalendar cal = new GregorianCalendar();

    private long timestamp;

    private String event;

    private String description;

    /**
     * @param timestamp
     * @param event
     * @param description
     */
    public RegionHistoryInformation(long timestamp, String event,
        String description) {
      this.timestamp = timestamp;
      this.event = event;
      this.description = description;
    }

    public int compareTo(RegionHistoryInformation otherInfo) {
      return -1 * Long.valueOf(timestamp).compareTo(otherInfo.getTimestamp());
    }

    /** @return the event */
    public String getEvent() {
      return event;
    }

    /** @return the description */
    public String getDescription() {
      return description;
    }

    /** @return the timestamp */
    public long getTimestamp() {
      return timestamp;
    }

    /**
     * @return The value of the timestamp processed with the date formater.
     */
    public String getTimestampAsString() {
      cal.setTimeInMillis(timestamp);
      return dateFormat.format(cal.getTime());
    }
  }

  /**
   * @return True if the historian is online. When offline, will not add
   * updates to the .META. table.
   */
  public boolean isOnline() {
    return this.metaTable != null;
  }

  /**
   * @param c Online the historian.  Invoke after cluster has spun up.
   */
  public void online(final HBaseConfiguration c) {
    try {
      this.metaTable = new HTable(c, META_TABLE_NAME);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Onlined");
      }
    } catch (IOException ioe) {
      LOG.error("Unable to create RegionHistorian", ioe);
    }
  }
  
  /**
   * Offlines the historian.
   * @see #online(HBaseConfiguration)
   */
  public void offline() {
    this.metaTable = null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Offlined");
    }
  }
}
