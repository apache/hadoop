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

package org.apache.hadoop.yarn;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Test;

public class TestContainerLogAppender {

  @Test
  public void testAppendInClose() throws Exception {
    final ContainerLogAppender claAppender = new ContainerLogAppender();
    claAppender.setName("testCLA");
    claAppender.setLayout(new PatternLayout("%-5p [%t]: %m%n"));
    claAppender.setContainerLogDir("target/testAppendInClose/logDir");
    claAppender.setContainerLogFile("syslog");
    claAppender.setTotalLogFileSize(1000);
    claAppender.activateOptions();
    final Logger claLog = Logger.getLogger("testAppendInClose-catergory");
    claLog.setAdditivity(false);
    claLog.addAppender(claAppender);
    claLog.info(new Object() {
      public String toString() {
        claLog.info("message1");
        return "return message1";
      }
    });
    claAppender.close();
  }
}
