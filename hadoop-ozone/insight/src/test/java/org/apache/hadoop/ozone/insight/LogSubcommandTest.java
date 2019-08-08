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