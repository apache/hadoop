package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.junit.Test;

public class TestNameNodeOptionParsing {

  @Test(timeout = 10000)
  public void testUpgrade() {
    StartupOption opt = null;
    // UPGRADE is set, but nothing else
    opt = NameNode.parseArguments(new String[] {"-upgrade"});
    assertEquals(opt, StartupOption.UPGRADE);
    assertNull(opt.getClusterId());
    assertTrue(FSImageFormat.renameReservedMap.isEmpty());
    // cluster ID is set
    opt = NameNode.parseArguments(new String[] { "-upgrade", "-clusterid",
        "mycid" });
    assertEquals(StartupOption.UPGRADE, opt);
    assertEquals("mycid", opt.getClusterId());
    assertTrue(FSImageFormat.renameReservedMap.isEmpty());
    // Everything is set
    opt = NameNode.parseArguments(new String[] { "-upgrade", "-clusterid",
        "mycid", "-renameReserved",
        ".snapshot=.my-snapshot,.reserved=.my-reserved" });
    assertEquals(StartupOption.UPGRADE, opt);
    assertEquals("mycid", opt.getClusterId());
    assertEquals(".my-snapshot",
        FSImageFormat.renameReservedMap.get(".snapshot"));
    assertEquals(".my-reserved",
        FSImageFormat.renameReservedMap.get(".reserved"));
    // Reset the map
    FSImageFormat.renameReservedMap.clear();
    // Everything is set, but in a different order
    opt = NameNode.parseArguments(new String[] { "-upgrade", "-renameReserved",
        ".reserved=.my-reserved,.snapshot=.my-snapshot", "-clusterid",
        "mycid"});
    assertEquals(StartupOption.UPGRADE, opt);
    assertEquals("mycid", opt.getClusterId());
    assertEquals(".my-snapshot",
        FSImageFormat.renameReservedMap.get(".snapshot"));
    assertEquals(".my-reserved",
        FSImageFormat.renameReservedMap.get(".reserved"));
    // Try the default renameReserved
    opt = NameNode.parseArguments(new String[] { "-upgrade", "-renameReserved"});
    assertEquals(StartupOption.UPGRADE, opt);
    assertEquals(
        ".snapshot." + LayoutVersion.getCurrentLayoutVersion()
            + ".UPGRADE_RENAMED",
        FSImageFormat.renameReservedMap.get(".snapshot"));
    assertEquals(
        ".reserved." + LayoutVersion.getCurrentLayoutVersion()
            + ".UPGRADE_RENAMED",
        FSImageFormat.renameReservedMap.get(".reserved"));

    // Try some error conditions
    try {
      opt =
          NameNode.parseArguments(new String[] { "-upgrade", "-renameReserved",
              ".reserved=.my-reserved,.not-reserved=.my-not-reserved" });
    } catch (IllegalArgumentException e) {
      assertExceptionContains("Unknown reserved path", e);
    }
    try {
      opt =
          NameNode.parseArguments(new String[] { "-upgrade", "-renameReserved",
              ".reserved=.my-reserved,.snapshot=.snapshot" });
    } catch (IllegalArgumentException e) {
      assertExceptionContains("Invalid rename path", e);
    }
    try {
      opt =
          NameNode.parseArguments(new String[] { "-upgrade", "-renameReserved",
              ".snapshot=.reserved" });
    } catch (IllegalArgumentException e) {
      assertExceptionContains("Invalid rename path", e);
    }
    opt = NameNode.parseArguments(new String[] { "-upgrade", "-cid"});
    assertNull(opt);
  }

}
