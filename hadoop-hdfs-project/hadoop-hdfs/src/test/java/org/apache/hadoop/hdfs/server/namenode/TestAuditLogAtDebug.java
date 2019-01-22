/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.DefaultAuditLogger;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.Inet4Address;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test that the HDFS Audit logger respects DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST. 
 */
public class TestAuditLogAtDebug {
  static final Log LOG = LogFactory.getLog(TestAuditLogAtDebug.class);

  @Rule
  public Timeout timeout = new Timeout(300000);
  
  private static final String DUMMY_COMMAND_1 = "dummycommand1";
  private static final String DUMMY_COMMAND_2 = "dummycommand2";
  
  private DefaultAuditLogger makeSpyLogger(
      Level level, Optional<List<String>> debugCommands) {
    DefaultAuditLogger logger = new DefaultAuditLogger();
    Configuration conf = new HdfsConfiguration();
    if (debugCommands.isPresent()) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_DEBUG_CMDLIST,
               Joiner.on(",").join(debugCommands.get()));
    }
    logger.initialize(conf);
    GenericTestUtils.setLogLevel(FSNamesystem.auditLog, level);
    return spy(logger);
  }
  
  private void logDummyCommandToAuditLog(HdfsAuditLogger logger, String command) {
    logger.logAuditEvent(true, "",
                         Inet4Address.getLoopbackAddress(),
                         command, "", "",
                         null, null, null, null);
  }

  @Test
  public void testDebugCommandNotLoggedAtInfo() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.INFO, Optional.of(Arrays.asList(DUMMY_COMMAND_1)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    verify(logger, never()).logAuditMessage(anyString());
  }

  @Test
  public void testDebugCommandLoggedAtDebug() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.DEBUG, Optional.of(Arrays.asList(DUMMY_COMMAND_1)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    verify(logger, times(1)).logAuditMessage(anyString());
  }
  
  @Test
  public void testInfoCommandLoggedAtInfo() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.INFO, Optional.of(Arrays.asList(DUMMY_COMMAND_1)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, times(1)).logAuditMessage(anyString());
  }

  @Test
  public void testMultipleDebugCommandsNotLoggedAtInfo() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.INFO,
            Optional.of(Arrays.asList(DUMMY_COMMAND_1, DUMMY_COMMAND_2)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, never()).logAuditMessage(anyString());
  }

  @Test
  public void testMultipleDebugCommandsLoggedAtDebug() {
    DefaultAuditLogger logger =
        makeSpyLogger(
            Level.DEBUG,
            Optional.of(Arrays.asList(DUMMY_COMMAND_1, DUMMY_COMMAND_2)));
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, times(2)).logAuditMessage(anyString());
  }
  
  @Test
  public void testEmptyDebugCommands() {
    DefaultAuditLogger logger = makeSpyLogger(Level.INFO, Optional.empty());
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_1);
    logDummyCommandToAuditLog(logger, DUMMY_COMMAND_2);
    verify(logger, times(2)).logAuditMessage(anyString());
  }
}
