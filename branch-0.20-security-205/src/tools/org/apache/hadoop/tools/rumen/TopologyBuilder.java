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
package org.apache.hadoop.tools.rumen;

import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Building the cluster topology.
 */
public class TopologyBuilder {
  private Set<ParsedHost> allHosts = new HashSet<ParsedHost>();

  /**
   * Process one {@link HistoryEvent}
   * 
   * @param event
   *          The {@link HistoryEvent} to be processed.
   */
  public void process(HistoryEvent event) {
    if (event instanceof TaskAttemptFinishedEvent) {
      processTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) event);
    } else if (event instanceof TaskAttemptUnsuccessfulCompletionEvent) {
      processTaskAttemptUnsuccessfulCompletionEvent((TaskAttemptUnsuccessfulCompletionEvent) event);
    } else if (event instanceof TaskStartedEvent) {
      processTaskStartedEvent((TaskStartedEvent) event);
    }

    // I do NOT expect these if statements to be exhaustive.
  }

  /**
   * Process a collection of JobConf {@link Properties}. We do not restrict it
   * to be called once.
   * 
   * @param conf
   *          The job conf properties to be added.
   */
  public void process(Properties conf) {
    // no code
  }

  /**
   * Request the builder to build the final object. Once called, the
   * {@link TopologyBuilder} would accept no more events or job-conf properties.
   * 
   * @return Parsed {@link LoggedNetworkTopology} object.
   */
  public LoggedNetworkTopology build() {
    return new LoggedNetworkTopology(allHosts);
  }

  private void processTaskStartedEvent(TaskStartedEvent event) {
    preferredLocationForSplits(event.getSplitLocations());
  }

  private void processTaskAttemptUnsuccessfulCompletionEvent(
      TaskAttemptUnsuccessfulCompletionEvent event) {
    recordParsedHost(event.getHostname());
  }

  private void processTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    recordParsedHost(event.getHostname());
  }

  private void recordParsedHost(String hostName) {
    ParsedHost result = ParsedHost.parse(hostName);

    if (result != null && !allHosts.contains(result)) {
      allHosts.add(result);
    }
  }

  private void preferredLocationForSplits(String splits) {
    if (splits != null) {
      StringTokenizer tok = new StringTokenizer(splits, ",", false);

      while (tok.hasMoreTokens()) {
        String nextSplit = tok.nextToken();

        recordParsedHost(nextSplit);
      }
    }
  }
}
