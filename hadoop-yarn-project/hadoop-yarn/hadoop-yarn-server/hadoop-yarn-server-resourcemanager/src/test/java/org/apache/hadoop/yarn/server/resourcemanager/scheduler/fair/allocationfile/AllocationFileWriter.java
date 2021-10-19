/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is capable of serializing allocation file data to a file
 * in XML format.
 * See {@link #writeToFile(String)} method for the implementation.
 */
public final class AllocationFileWriter {
  private static final String DRF = "drf";
  private static final String FAIR = "fair";
  private static final String FIFO = "fifo";

  private Integer queueMaxAppsDefault;
  private String queueMaxResourcesDefault;
  private Integer userMaxAppsDefault;
  private Double queueMaxAMShareDefault;
  private Integer defaultMinSharePreemptionTimeout;
  private Integer defaultFairSharePreemptionTimeout;
  private Double defaultFairSharePreemptionThreshold;
  private String defaultQueueSchedulingPolicy;
  private List<AllocationFileQueue> queues = new ArrayList<>();
  private UserSettings userSettings;
  private boolean useLegacyTagNameForQueues = false;
  private String reservationAgent;
  private String reservationPolicy;
  private AllocationFileQueuePlacementPolicy queuePlacementPolicy;

  private AllocationFileWriter() {
  }

  public static AllocationFileWriter create() {
    return new AllocationFileWriter();
  }

  public AllocationFileWriter addQueue(AllocationFileQueue queue) {
    queues.add(queue);
    return this;
  }

  public AllocationFileWriter queueMaxAppsDefault(int value) {
    this.queueMaxAppsDefault = value;
    return this;
  }

  public AllocationFileWriter queueMaxResourcesDefault(String value) {
    this.queueMaxResourcesDefault = value;
    return this;
  }

  public AllocationFileWriter userMaxAppsDefault(int value) {
    this.userMaxAppsDefault = value;
    return this;
  }

  public AllocationFileWriter queueMaxAMShareDefault(double value) {
    this.queueMaxAMShareDefault = value;
    return this;
  }

  public AllocationFileWriter disableQueueMaxAMShareDefault() {
    this.queueMaxAMShareDefault = -1.0d;
    return this;
  }

  public AllocationFileWriter defaultMinSharePreemptionTimeout(int value) {
    this.defaultMinSharePreemptionTimeout = value;
    return this;
  }

  public AllocationFileWriter defaultFairSharePreemptionTimeout(int value) {
    this.defaultFairSharePreemptionTimeout = value;
    return this;
  }

  public AllocationFileWriter defaultFairSharePreemptionThreshold(
      double value) {
    this.defaultFairSharePreemptionThreshold = value;
    return this;
  }

  public AllocationFileWriter drfDefaultQueueSchedulingPolicy() {
    this.defaultQueueSchedulingPolicy = DRF;
    return this;
  }

  public AllocationFileWriter fairDefaultQueueSchedulingPolicy() {
    this.defaultQueueSchedulingPolicy = FAIR;
    return this;
  }

  public AllocationFileWriter fifoDefaultQueueSchedulingPolicy() {
    this.defaultQueueSchedulingPolicy = FIFO;
    return this;
  }

  public AllocationFileWriter useLegacyTagNameForQueues() {
    this.useLegacyTagNameForQueues = true;
    return this;
  }

  public AllocationFileWriter reservationAgent(String value) {
    this.reservationAgent = value;
    return this;
  }

  public AllocationFileWriter reservationPolicy(String value) {
    this.reservationPolicy = value;
    return this;
  }

  public AllocationFileWriter userSettings(UserSettings settings) {
    this.userSettings = settings;
    return this;
  }

  public AllocationFileWriter queuePlacementPolicy(
      AllocationFileQueuePlacementPolicy policy) {
    this.queuePlacementPolicy = policy;
    return this;
  }

  static void printQueues(PrintWriter pw, List<AllocationFileQueue> queues,
      boolean useLegacyTagName) {
    for (AllocationFileQueue queue : queues) {
      final String queueStr;
      if (useLegacyTagName) {
        queueStr = queue.renderWithLegacyTag();
      } else {
        queueStr = queue.render();
      }
      pw.println(queueStr);
    }
  }

  private void printUserSettings(PrintWriter pw) {
    pw.println(userSettings.render());
  }

  private void printQueuePlacementPolicy(PrintWriter pw) {
    pw.println(queuePlacementPolicy.render());
  }

  static void addIfPresent(PrintWriter pw, String tag, Object obj) {
    if (obj != null) {
      pw.println("<" + tag + ">" + obj.toString() + "</" + tag + ">");
    }
  }

  private void writeHeader(PrintWriter pw) {
    pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    pw.println("<allocations>");
  }

  private void writeFooter(PrintWriter pw) {
    pw.println("</allocations>");
  }

  public void writeToFile(String filename) {
    PrintWriter pw;
    try {
      pw = new PrintWriter(filename, "UTF-8");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    writeHeader(pw);
    if (!queues.isEmpty()) {
      printQueues(pw, queues, useLegacyTagNameForQueues);
    }
    if (userSettings != null) {
      printUserSettings(pw);
    }

    if (queuePlacementPolicy != null) {
      printQueuePlacementPolicy(pw);
    }

    addIfPresent(pw, "queueMaxAppsDefault", queueMaxAppsDefault);
    addIfPresent(pw, "queueMaxResourcesDefault", queueMaxResourcesDefault);
    addIfPresent(pw, "userMaxAppsDefault", userMaxAppsDefault);
    addIfPresent(pw, "queueMaxAMShareDefault", queueMaxAMShareDefault);
    addIfPresent(pw, "defaultMinSharePreemptionTimeout",
        defaultMinSharePreemptionTimeout);
    addIfPresent(pw, "defaultFairSharePreemptionTimeout",
        defaultFairSharePreemptionTimeout);
    addIfPresent(pw, "defaultFairSharePreemptionThreshold",
        defaultFairSharePreemptionThreshold);
    addIfPresent(pw, "defaultQueueSchedulingPolicy",
        defaultQueueSchedulingPolicy);
    addIfPresent(pw, "reservation-agent", reservationAgent);
    addIfPresent(pw, "reservation-policy", reservationPolicy);

    writeFooter(pw);
    pw.close();
  }

}
