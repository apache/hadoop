package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileStatusWithECPolicy {
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;

  @Before
  public void before() throws IOException {
    cluster =
        new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    client = fs.getClient();
  }

  @After
  public void after() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileStatusWithECPolicy() throws Exception {
    // test directory not in EC zone
    final Path dir = new Path("/foo");
    assertTrue(fs.mkdir(dir, FsPermission.getDirDefault()));
    assertNull(client.getFileInfo(dir.toString()).getErasureCodingPolicy());
    // test file not in EC zone
    final Path file = new Path(dir, "foo");
    fs.create(file).close();
    assertNull(client.getFileInfo(file.toString()).getErasureCodingPolicy());
    fs.delete(file, true);

    final ErasureCodingPolicy ecPolicy1 = ErasureCodingPolicyManager.getSystemDefaultPolicy();
    // create EC zone on dir
    fs.createErasureCodingZone(dir, ecPolicy1);
    final ErasureCodingPolicy ecPolicy2 = client.getFileInfo(dir.toUri().getPath()).getErasureCodingPolicy();
    assertNotNull(ecPolicy2);
    assertTrue(ecPolicy1.equals(ecPolicy2));

    // test file in EC zone
    fs.create(file).close();
    final ErasureCodingPolicy ecPolicy3 =
        fs.getClient().getFileInfo(file.toUri().getPath()).getErasureCodingPolicy();
    assertNotNull(ecPolicy3);
    assertTrue(ecPolicy1.equals(ecPolicy3));
  }
}
