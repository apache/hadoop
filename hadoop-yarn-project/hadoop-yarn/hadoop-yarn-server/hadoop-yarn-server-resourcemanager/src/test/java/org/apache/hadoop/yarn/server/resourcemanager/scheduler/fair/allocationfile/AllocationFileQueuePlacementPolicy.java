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


import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * Helper class to manage {@link AllocationFileQueuePlacementRule}
 * instances for {@link AllocationFileWriter}.
 */
public class AllocationFileQueuePlacementPolicy {
  private List<AllocationFileQueuePlacementRule> rules = Lists.newArrayList();

  public AllocationFileQueuePlacementPolicy addRule(
      AllocationFileQueuePlacementRule rule) {
    this.rules.add(rule);
    return this;
  }

  public String render() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    addStartTag(pw);
    addRules(pw);
    addEndTag(pw);
    pw.close();

    return sw.toString();
  }

  private void addStartTag(PrintWriter pw) {
    pw.println("<queuePlacementPolicy>");
  }

  private void addRules(PrintWriter pw) {
    for (AllocationFileQueuePlacementRule rule : rules) {
      pw.println(rule.render());
    }
  }

  private void addEndTag(PrintWriter pw) {
    pw.println("</queuePlacementPolicy>");
  }
}
