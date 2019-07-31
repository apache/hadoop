/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.KillJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestKillJobCliParsing {
  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext = new MockClientContext();
    KillJobCli killJobCli = new KillJobCli(mockClientContext);
    killJobCli.printUsages();
  }

  @Test
  public void testKillJob()
      throws InterruptedException, SubmarineException, YarnException,
      ParseException, IOException {
    MockClientContext mockClientContext = new MockClientContext();
    KillJobCli killJobCli = new KillJobCli(mockClientContext) {
      @Override
      protected boolean killJob() {
        // do nothing
        return false;
      }
    };
    killJobCli.run(new String[] { "--name", "my-job" });
    KillJobParameters parameters = killJobCli.getParameters();
    Assert.assertEquals(parameters.getName(), "my-job");
  }
}
