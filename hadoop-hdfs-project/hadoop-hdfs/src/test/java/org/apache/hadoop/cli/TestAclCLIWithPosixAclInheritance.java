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
package org.apache.hadoop.cli;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY;

import org.junit.Test;

/**
 * Test ACL CLI with POSIX ACL inheritance enabled.
 */
public class TestAclCLIWithPosixAclInheritance extends TestAclCLI {

  @Override
  protected void initConf() {
    super.initConf();
    conf.setBoolean(DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY, true);
  }

  @Override
  protected String getTestFile() {
    return "testAclCLIWithPosixAclInheritance.xml";
  }

  @Test
  @Override
  public void testAll() {
    super.testAll();
  }
}
