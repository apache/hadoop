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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

public class TestFicaSchedulerAPP {
    
  @Test
  public void testGetActivedAppDiagnosticMessage() {
      FiCaSchedulerApp schedulerApp = new FiCaSchedulerApp(null,
              "yarn",null,null, new RMContextImpl());
      StringBuilder diagnosticMessage = new StringBuilder(
              "Application is Activated, waiting for resources to be assigned for AM");
      schedulerApp.getActivedAppDiagnosticMessage(diagnosticMessage);
      assertThat("AM Resource Request information was not successfully displayed.", 
              diagnosticMessage.toString(), containsString("AM Resource Request ="));
  }
  
}
