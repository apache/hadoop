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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

class AllocationFileQueue {
  private final AllocationFileQueueProperties properties;
  private final List<AllocationFileQueue> subQueues;

  AllocationFileQueue(AllocationFileQueueProperties properties,
      List<AllocationFileQueue> subQueues) {
    this.properties = properties;
    this.subQueues = subQueues;
  }

  String render() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    printStartTag(pw);
    AllocationFileWriter.printQueues(pw, subQueues);
    AllocationFileWriter.addIfPresent(pw, "minResources",
            properties::getMinResources);
    AllocationFileWriter.addIfPresent(pw, "maxResources",
            properties::getMaxResources);
    AllocationFileWriter.addIfPresent(pw, "aclAdministerApps",
            properties::getAclAdministerApps);
    AllocationFileWriter.addIfPresent(pw, "aclSubmitApps",
            properties::getAclSubmitApps);
    AllocationFileWriter.addIfPresent(pw, "schedulingPolicy",
            properties::getSchedulingPolicy);
    AllocationFileWriter.addIfPresent(pw, "maxRunningApps",
        () -> AllocationFileWriter
            .createNumberSupplier(properties.getMaxRunningApps()));
    AllocationFileWriter.addIfPresent(pw, "maxAMShare",
        () -> AllocationFileWriter.createNumberSupplier(properties
                .getMaxAMShare()));
    AllocationFileWriter.addIfPresent(pw, "minSharePreemptionTimeout",
        () -> AllocationFileWriter
            .createNumberSupplier(properties.getMinSharePreemptionTimeout()));
    AllocationFileWriter.addIfPresent(pw, "maxChildResources",
            properties::getMaxChildResources);
    AllocationFileWriter.addIfPresent(pw, "fairSharePreemptionTimeout",
        () -> AllocationFileWriter
            .createNumberSupplier(properties.getFairSharePreemptionTimeout()));
    AllocationFileWriter.addIfPresent(pw, "fairSharePreemptionThreshold",
        () -> AllocationFileWriter
            .createNumberSupplier(
                    properties.getFairSharePreemptionThreshold()));
    printEndTag(pw);
    pw.close();
    return sw.toString();
  }

  private void printStartTag(PrintWriter pw) {
    pw.print("<queue name=\"" + properties.getQueueName() + "\" ");
    if (properties.getParent()) {
      pw.print("type=\"parent\"");
    }
    pw.println(">");
  }

  private void printEndTag(PrintWriter pw) {
    pw.println("</queue>");
  }
}
