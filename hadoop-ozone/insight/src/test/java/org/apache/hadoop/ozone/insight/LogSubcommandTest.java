/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import org.junit.Assert;
import org.junit.Test;

/**
 * Testing utility methods of the log subcommand test.
 */
public class LogSubcommandTest {

  @Test
  public void filterLog() {
    LogSubcommand logSubcommand = new LogSubcommand();
    String result = logSubcommand.processLogLine(
        "2019-08-04 12:27:08,648 [TRACE|org.apache.hadoop.hdds.scm.node"
            + ".SCMNodeManager|SCMNodeManager] HB is received from "
            + "[datanode=localhost]: <json>storageReport {\\n  storageUuid: "
            + "\"DS-29204db6-a615-4106-9dd4-ce294c2f4cf6\"\\n  "
            + "storageLocation: \"/tmp/hadoop-elek/dfs/data\"\\n  capacity: "
            + "8348086272\\n  scmUsed: 4096\\n  remaining: 8246956032n  "
            + "storageType: DISK\\n  failed: falsen}\\n</json>\n");
    Assert.assertEquals(3, result.split("\n").length);
  }
}