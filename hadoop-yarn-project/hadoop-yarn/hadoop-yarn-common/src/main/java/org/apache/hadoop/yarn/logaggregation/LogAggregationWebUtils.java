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
package org.apache.hadoop.yarn.logaggregation;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;

/**
 * Utils for rendering aggregated logs block.
 *
 */
@Private
public final class LogAggregationWebUtils {

  private LogAggregationWebUtils() {}

  /**
   * Parse start index from html.
   * @param html the html
   * @param startStr the start index string
   * @return the startIndex
   */
  public static long getLogStartIndex(Block html, String startStr)
      throws NumberFormatException {
    long start = -4096;

    if (startStr != null && !startStr.isEmpty()) {
      start = Long.parseLong(startStr);
    }
    return start;
  }

  /**
   * Parse end index from html.
   * @param html the html
   * @param endStr the end index string
   * @return the endIndex
   */
  public static long getLogEndIndex(Block html, String endStr)
      throws NumberFormatException {
    long end = Long.MAX_VALUE;

    if (endStr != null && !endStr.isEmpty()) {
      end = Long.parseLong(endStr);
    }
    return end;
  }

  /**
   * Verify and parse containerId.
   * @param html the html
   * @param containerIdStr the containerId string
   * @return the {@link ContainerId}
   */
  public static ContainerId verifyAndGetContainerId(Block html,
      String containerIdStr) {
    if (containerIdStr == null || containerIdStr.isEmpty()) {
      html.h1().__("Cannot get container logs without a ContainerId").__();
      return null;
    }
    ContainerId containerId = null;
    try {
      containerId = ContainerId.fromString(containerIdStr);
    } catch (IllegalArgumentException e) {
      html.h1()
          .__("Cannot get container logs for invalid containerId: "
              + containerIdStr).__();
      return null;
    }
    return containerId;
  }

  /**
   * Verify and parse NodeId.
   * @param html the html
   * @param nodeIdStr the nodeId string
   * @return the {@link NodeId}
   */
  public static NodeId verifyAndGetNodeId(Block html, String nodeIdStr) {
    if (nodeIdStr == null || nodeIdStr.isEmpty()) {
      html.h1().__("Cannot get container logs without a NodeId").__();
      return null;
    }
    NodeId nodeId = null;
    try {
      nodeId = NodeId.fromString(nodeIdStr);
    } catch (IllegalArgumentException e) {
      html.h1().__("Cannot get container logs. Invalid nodeId: " + nodeIdStr)
          .__();
      return null;
    }
    return nodeId;
  }

  /**
   * Verify and parse the application owner.
   * @param html the html
   * @param appOwner the Application owner
   * @return the appOwner
   */
  public static String verifyAndGetAppOwner(Block html, String appOwner) {
    if (appOwner == null || appOwner.isEmpty()) {
      html.h1().__("Cannot get container logs without an app owner").__();
    }
    return appOwner;
  }

  /**
   * Parse log start time from html.
   * @param startStr the start time string
   * @return the startIndex
   */
  public static long getLogStartTime(String startStr)
      throws NumberFormatException {
    long start = 0;

    if (startStr != null && !startStr.isEmpty()) {
      start = Long.parseLong(startStr);
    }
    return start;
  }

  /**
   * Parse log end time from html.
   * @param endStr the end time string
   * @return the endIndex
   */
  public static long getLogEndTime(String endStr)
      throws NumberFormatException {
    long end = Long.MAX_VALUE;

    if (endStr != null && !endStr.isEmpty()) {
      end = Long.parseLong(endStr);
    }
    return end;
  }
}
