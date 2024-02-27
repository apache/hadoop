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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestGPGWebApp {

  private static final Logger LOG = LoggerFactory.getLogger(TestGPGWebApp.class);

  @Test
  public void testGPGPoliciesPageWebView()
      throws InterruptedException, YarnException, IOException {
    LOG.info("testGPGPoliciesPageWebView.");
    WebAppTests.testPage(GPGPoliciesPage.class, GlobalPolicyGenerator.class,
        new GlobalPolicyGenerator());
  }

  @Test
  public void testGPGOverview()
      throws InterruptedException, YarnException, IOException {
    LOG.info("testGPGOverview.");
    GlobalPolicyGenerator globalPolicyGenerator = new GlobalPolicyGenerator();
    globalPolicyGenerator.setConfig(new Configuration());
    WebAppTests.testPage(GPGOverviewPage.class, GlobalPolicyGenerator.class,
        globalPolicyGenerator);
  }
}
