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

package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.net.URL;

/**
 * This test just debugs which log resources are being picked up
 */
public class TestLogResources implements SwiftTestConstants {
  protected static final Log LOG =
    LogFactory.getLog(TestLogResources.class);

  private void printf(String format, Object... args) {
    String msg = String.format(format, args);
    System.out.printf(msg + "\n");
    LOG.info(msg);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testWhichLog4JPropsFile() throws Throwable {
    locateResource("log4j.properties");
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testWhichLog4JXMLFile() throws Throwable {
    locateResource("log4j.XML");
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testCommonsLoggingProps() throws Throwable {
    locateResource("commons-logging.properties");
  }

  private void locateResource(String resource) {
    URL url = this.getClass().getClassLoader().getResource(resource);
    if (url != null) {
      printf("resource %s is at %s", resource, url);
    } else {
      printf("resource %s is not on the classpath", resource);
    }
  }
}
