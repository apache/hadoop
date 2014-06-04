/*
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

package org.apache.hadoop.service.launcher;

import org.junit.Test;

public class TestServiceLauncher extends AbstractServiceLauncherTestBase {


  @Test
  public void testNoArgs() throws Throwable {
    try {
      ServiceLauncher.serviceMain();
    } catch (ServiceLaunchException e) {
      assertExceptionCodeEquals(EXIT_USAGE, e);
    }
  }
  
  @Test
  public void testUnknownClass() throws Throwable {
    try {
      ServiceLauncher.serviceMain("no.such.classname");
    } catch (ServiceLaunchException e) {
      assertExceptionCodeEquals(EXIT_SERVICE_CREATION_FAILURE, e);
    }
  }
   
  @Test
  public void testNotAService() throws Throwable {
    try {
      ServiceLauncher.serviceMain("org.apache.hadoop.service.launcher.TestServiceLauncher");
    } catch (ServiceLaunchException e) {
      assertExceptionCodeEquals(EXIT_SERVICE_CREATION_FAILURE, e);
    }
  }
  
}
