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

package org.apache.hadoop.yarn.logaggregation.testutils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;

import static org.apache.hadoop.yarn.logaggregation.testutils.MockRMClientUtils.createMockRMClient;

public class AggregatedLogDeletionServiceForTest extends AggregatedLogDeletionService {
  private final List<ApplicationId> finishedApplications;
  private final List<ApplicationId> runningApplications;
  private final Configuration conf;
  private ApplicationClientProtocol mockRMClient;

  public AggregatedLogDeletionServiceForTest(List<ApplicationId> runningApplications,
                                             List<ApplicationId> finishedApplications) {
    this(runningApplications, finishedApplications, null);
  }

  public AggregatedLogDeletionServiceForTest(List<ApplicationId> runningApplications,
                                             List<ApplicationId> finishedApplications,
                                             Configuration conf) {
    this.runningApplications = runningApplications;
    this.finishedApplications = finishedApplications;
    this.conf = conf;
  }

  @Override
  protected ApplicationClientProtocol createRMClient() throws IOException {
    if (mockRMClient != null) {
      return mockRMClient;
    }
    try {
      mockRMClient =
          createMockRMClient(finishedApplications, runningApplications);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return mockRMClient;
  }

  @Override
  protected Configuration createConf() {
    return conf;
  }

  public ApplicationClientProtocol getMockRMClient() {
    return mockRMClient;
  }
}
