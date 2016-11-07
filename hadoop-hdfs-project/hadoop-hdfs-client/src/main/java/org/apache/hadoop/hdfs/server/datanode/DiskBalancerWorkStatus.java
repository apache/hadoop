/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.server.datanode;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import static com.fasterxml.jackson.databind.type.TypeFactory.defaultInstance;

/**
 * Helper class that reports how much work has has been done by the node.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskBalancerWorkStatus {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectMapper MAPPER_WITH_INDENT_OUTPUT =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
  private static final ObjectReader READER_WORKSTATUS =
      new ObjectMapper().reader(DiskBalancerWorkStatus.class);
  private static final ObjectReader READER_WORKENTRY = new ObjectMapper()
      .reader(defaultInstance().constructCollectionType(List.class,
          DiskBalancerWorkEntry.class));

  private final List<DiskBalancerWorkEntry> currentState;
  private Result result;
  private String planID;
  private String planFile;

  /**
   * Constructs a default workStatus Object.
   */
  public DiskBalancerWorkStatus() {
    this.currentState = new LinkedList<>();
  }

  /**
   * Constructs a workStatus Object.
   *
   * @param result - int
   * @param planID - Plan ID
   * @param planFile - Plan file name
   */
  public DiskBalancerWorkStatus(Result result, String planID, String planFile) {
    this();
    this.result = result;
    this.planID = planID;
    this.planFile = planFile;
  }

  /**
   * Constructs a workStatus Object.
   *
   * @param result       - int
   * @param planID       - Plan ID
   * @param currentState - Current State
   */
  public DiskBalancerWorkStatus(Result result, String planID,
                                List<DiskBalancerWorkEntry> currentState) {
    this.result = result;
    this.planID = planID;
    this.currentState = currentState;
  }


  /**
   * Constructs a workStatus Object.
   *
   * @param result       - int
   * @param planID       - Plan ID
   * @param currentState - List of WorkEntries.
   */
  public DiskBalancerWorkStatus(Result result, String planID, String planFile,
                                String currentState) throws IOException {
    this.result = result;
    this.planID = planID;
    this.planFile = planFile;
    this.currentState = READER_WORKENTRY.readValue(currentState);
  }


  /**
   * Returns result.
   *
   * @return long
   */
  public Result getResult() {
    return result;
  }

  /**
   * Returns planID.
   *
   * @return String
   */
  public String getPlanID() {
    return planID;
  }

  /**
   * Returns planFile.
   *
   * @return String
   */
  public String getPlanFile() {
    return planFile;
  }

  /**
   * Gets current Status.
   *
   * @return - Json String
   */
  public List<DiskBalancerWorkEntry> getCurrentState() {
    return currentState;
  }

  /**
   * Return current state as a string.
   *
   * @throws IOException
   **/
  public String currentStateString() throws IOException {
    return MAPPER_WITH_INDENT_OUTPUT.writeValueAsString(currentState);
  }

  public String toJsonString() throws IOException {
    return MAPPER.writeValueAsString(this);
  }

  /**
   * Returns a DiskBalancerWorkStatus object from the Json .
   * @param json - json String
   * @return DiskBalancerWorkStatus
   * @throws IOException
   */
  public static DiskBalancerWorkStatus parseJson(String json) throws
      IOException {
    return READER_WORKSTATUS.readValue(json);
  }


  /**
   * Adds a new work entry to the list.
   *
   * @param entry - DiskBalancerWorkEntry
   */

  public void addWorkEntry(DiskBalancerWorkEntry entry) {
    Preconditions.checkNotNull(entry);
    currentState.add(entry);
  }

  /** Various result values. **/
  public enum Result {
    NO_PLAN(0),
    PLAN_UNDER_PROGRESS(1),
    PLAN_DONE(2),
    PLAN_CANCELLED(3);
    private int result;

    private Result(int result) {
      this.result = result;
    }

    /**
     * Get int value of result.
     *
     * @return int
     */
    public int getIntResult() {
      return result;
    }
  }

  /**
   * A class that is used to report each work item that we are working on. This
   * class describes the Source, Destination and how much data has been already
   * moved, errors encountered etc. This is useful for the disk balancer stats
   * as well as the queryStatus RPC.
   */
  public static class DiskBalancerWorkEntry {
    private String sourcePath;
    private String destPath;
    private DiskBalancerWorkItem workItem;

    /**
     * Constructor needed for json serialization.
     */
    public DiskBalancerWorkEntry() {
    }

    public DiskBalancerWorkEntry(String workItem) throws IOException {
      this.workItem = DiskBalancerWorkItem.parseJson(workItem);
    }

    /**
     * Constructs a Work Entry class.
     *
     * @param sourcePath - Source Path where we are moving data from.
     * @param destPath   - Destination path to where we are moving data to.
     * @param workItem   - Current work status of this move.
     */
    public DiskBalancerWorkEntry(String sourcePath, String destPath,
                                 DiskBalancerWorkItem workItem) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      this.workItem = workItem;
    }

    /**
     * Returns the source path.
     *
     * @return - Source path
     */
    public String getSourcePath() {
      return sourcePath;
    }

    /**
     * Sets the Source Path.
     *
     * @param sourcePath - Volume Path.
     */
    public void setSourcePath(String sourcePath) {
      this.sourcePath = sourcePath;
    }

    /**
     * Gets the Destination path.
     *
     * @return - Path
     */
    public String getDestPath() {
      return destPath;
    }

    /**
     * Sets the destination path.
     *
     * @param destPath - Path
     */
    public void setDestPath(String destPath) {
      this.destPath = destPath;
    }

    /**
     * Gets the current status of work for these volumes.
     *
     * @return - Work Item
     */
    public DiskBalancerWorkItem getWorkItem() {
      return workItem;
    }

    /**
     * Sets the work item.
     *
     * @param workItem - sets the work item information
     */
    public void setWorkItem(DiskBalancerWorkItem workItem) {
      this.workItem = workItem;
    }
  }
}
