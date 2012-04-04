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
package org.apache.hadoop.ha;

import java.io.File;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.JMXEnv;

/**
 * A subclass of ZK's ClientBase testing utility, with some fixes
 * necessary for running in the Hadoop context.
 */
public class ClientBaseWithFixes extends ClientBase {

  /**
   * When running on the Jenkins setup, we need to ensure that this
   * build directory exists before running the tests.
   */
  @Override
  public void setUp() throws Exception {
    // build.test.dir is used by zookeeper
    new File(System.getProperty("build.test.dir", "build")).mkdirs();
    super.setUp();
  }
  
  /**
   * ZK seems to have a bug when we muck with its sessions
   * behind its back, causing disconnects, etc. This bug
   * ends up leaving JMX beans around at the end of the test,
   * and ClientBase's teardown method will throw an exception
   * if it finds JMX beans leaked. So, clear them out there
   * to workaround the ZK bug. See ZOOKEEPER-1438.
   */
  @Override
  public void tearDown() throws Exception {
    Set<ObjectName> names = JMXEnv.ensureAll();
    for (ObjectName n : names) {
      try {
        JMXEnv.conn().unregisterMBean(n);
      } catch (Throwable t) {
        // ignore
      }
    }
  }
}
