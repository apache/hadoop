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

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.ShowJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.MemorySubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestShowJobCliParsing {
  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext = new MockClientContext();
    ShowJobCli showJobCli = new ShowJobCli(mockClientContext);
    showJobCli.printUsages();
  }

  @Test
  public void testShowJob()
      throws InterruptedException, SubmarineException, YarnException,
      ParseException, IOException {
    MockClientContext mockClientContext = new MockClientContext();
    ShowJobCli showJobCli = new ShowJobCli(mockClientContext) {
      @Override
      protected void getAndPrintJobInfo() {
        // do nothing
      }
    };
    showJobCli.run(new String[] { "--name", "my-job" });
    ShowJobParameters parameters = showJobCli.getParameters();
    Assert.assertEquals(parameters.getName(), "my-job");
  }

  private Map<String, String> getMockJobInfo(String jobName) {
    Map<String, String> map = new HashMap<>();
    map.put(StorageKeyConstants.APPLICATION_ID,
        ApplicationId.newInstance(1234L, 1).toString());
    map.put(StorageKeyConstants.JOB_RUN_ARGS, "job run 123456");
    map.put(StorageKeyConstants.INPUT_PATH, "hdfs://" + jobName);
    return map;
  }

  @Test
  public void testSimpleShowJob()
      throws InterruptedException, SubmarineException, YarnException,
      ParseException, IOException {
    SubmarineStorage storage = new MemorySubmarineStorage();
    MockClientContext mockClientContext = new MockClientContext();
    RuntimeFactory runtimeFactory = mock(RuntimeFactory.class);
    when(runtimeFactory.getSubmarineStorage()).thenReturn(storage);
    mockClientContext.setRuntimeFactory(runtimeFactory);

    ShowJobCli showJobCli = new ShowJobCli(mockClientContext);

    try {
      showJobCli.run(new String[] { "--name", "my-job" });
    } catch (IOException e) {
      // expected
    }


    storage.addNewJob("my-job", getMockJobInfo("my-job"));
    showJobCli.run(new String[] { "--name", "my-job" });
  }
}
