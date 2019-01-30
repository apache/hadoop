/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service;

import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

import static org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test miscellaneous service operations through mocked failures.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestServiceOperations {

  @Mock
  private Service service;

  @Mock
  private RuntimeException e;

  @Test
  public void testStopQuietlyWhenServiceStopThrowsException() throws Exception {
    Logger logger = LoggerFactory.getLogger(TestServiceOperations.class);
    LogCapturer logCapturer = captureLogs(logger);
    doThrow(e).when(service).stop();

    ServiceOperations.stopQuietly(logger, service);

    assertThat(logCapturer.getOutput(),
        containsString("When stopping the service " + service.getName()));
    verify(e, times(1)).printStackTrace(Mockito.any(PrintWriter.class));
  }

}