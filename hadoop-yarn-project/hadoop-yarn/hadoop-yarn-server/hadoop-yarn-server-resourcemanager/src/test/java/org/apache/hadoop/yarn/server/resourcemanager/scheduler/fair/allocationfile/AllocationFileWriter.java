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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * This class is capable of serializing allocation file data to a file
 * in XML format.
 * See {@link #writeToFile(String)} method for the implementation.
 */
public final class AllocationFileWriter {
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

  private AllocationFileWriter() {
  }

  public static AllocationFileWriter create() {
    return new AllocationFileWriter();
  }

  public AllocationFileSimpleQueueBuilder queue(String queueName) {
    return new AllocationFileSimpleQueueBuilder(this, queueName);
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

  public AllocationFileWriter defaultQueueSchedulingPolicy(String value) {
    this.defaultQueueSchedulingPolicy = value;
    return this;
  }

  public UserSettings.Builder userSettings(String username) {
    return new UserSettings.Builder(this, username);
  }

  void addQueue(AllocationFileQueue queue) {
    this.queues.add(queue);
  }

  void setUserSettings(UserSettings userSettings) {
    this.userSettings = userSettings;
  }

  static void printQueues(PrintWriter pw, List<AllocationFileQueue> queues) {
    for (AllocationFileQueue queue : queues) {
      pw.println(queue.render());
    }
  }

  private void printUserSettings(PrintWriter pw) {
    pw.println(userSettings.render());
  }

  static void addIfPresent(PrintWriter pw, String tag,
      Supplier<String> supplier) {
    if (supplier.get() != null) {
      pw.println("<" + tag + ">" + supplier.get() + "</" + tag + ">");
    }
  }

  static String createNumberSupplier(Object number) {
    if (number != null) {
      return number.toString();
    }
    return null;
  }

  private void writeHeader(PrintWriter pw) {
    pw.println("<?xml version=\"1.0\"?>");
    pw.println("<allocations>");
  }

  private void writeFooter(PrintWriter pw) {
    pw.println("</allocations>");
  }

  public void writeToFile(String filename) {
    PrintWriter pw;
    try {
      pw = new PrintWriter(new FileWriter(filename));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    writeHeader(pw);
    if (!queues.isEmpty()) {
      printQueues(pw, queues);
    }
    if (userSettings != null) {
      printUserSettings(pw);
    }

    addIfPresent(pw, "queueMaxAppsDefault",
        () -> createNumberSupplier(queueMaxAppsDefault));
    addIfPresent(pw, "queueMaxResourcesDefault",
        () -> queueMaxResourcesDefault);
    addIfPresent(pw, "userMaxAppsDefault",
        () -> createNumberSupplier(userMaxAppsDefault));
    addIfPresent(pw, "queueMaxAMShareDefault",
        () -> createNumberSupplier(queueMaxAMShareDefault));
    addIfPresent(pw, "defaultMinSharePreemptionTimeout",
        () -> createNumberSupplier(defaultMinSharePreemptionTimeout));
    addIfPresent(pw, "defaultFairSharePreemptionTimeout",
        () -> createNumberSupplier(defaultFairSharePreemptionTimeout));
    addIfPresent(pw, "defaultFairSharePreemptionThreshold",
        () -> createNumberSupplier(defaultFairSharePreemptionThreshold));
    addIfPresent(pw, "defaultQueueSchedulingPolicy",
        () -> defaultQueueSchedulingPolicy);
    writeFooter(pw);
    pw.close();
  }

}
