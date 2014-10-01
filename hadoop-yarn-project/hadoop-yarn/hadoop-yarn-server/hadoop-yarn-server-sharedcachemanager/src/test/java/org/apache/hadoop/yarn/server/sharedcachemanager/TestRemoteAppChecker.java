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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.junit.After;
import org.junit.Test;

public class TestRemoteAppChecker {

  private RemoteAppChecker checker;

  @After
  public void cleanup() {
    if (checker != null) {
      checker.stop();
    }
  }

  /**
   * Creates/initializes/starts a RemoteAppChecker with a spied
   * DummyYarnClientImpl.
   * 
   * @return the spied DummyYarnClientImpl in the created AppChecker
   */
  private YarnClient createCheckerWithMockedClient() {
    YarnClient client = spy(new DummyYarnClientImpl());
    checker = new RemoteAppChecker(client);
    checker.init(new Configuration());
    checker.start();
    return client;
  }

  @Test
  public void testNonExistentApp() throws Exception {
    YarnClient client = createCheckerWithMockedClient();
    ApplicationId id = ApplicationId.newInstance(1, 1);

    // test for null
    doReturn(null).when(client).getApplicationReport(id);
    assertFalse(checker.isApplicationActive(id));

    // test for ApplicationNotFoundException
    doThrow(new ApplicationNotFoundException("Throw!")).when(client)
        .getApplicationReport(id);
    assertFalse(checker.isApplicationActive(id));
  }

  @Test
  public void testRunningApp() throws Exception {
    YarnClient client = createCheckerWithMockedClient();
    ApplicationId id = ApplicationId.newInstance(1, 1);

    // create a report and set the state to an active one
    ApplicationReport report = new ApplicationReportPBImpl();
    report.setYarnApplicationState(YarnApplicationState.ACCEPTED);
    doReturn(report).when(client).getApplicationReport(id);

    assertTrue(checker.isApplicationActive(id));
  }

  class DummyYarnClientImpl extends YarnClientImpl {
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      // do nothing
    }

    @Override
    protected void serviceStart() {
      // do nothing
    }

    @Override
    protected void serviceStop() {
      // do nothing
    }
  }
}
