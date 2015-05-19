package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingSchemaManager;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileStatusWithECschema {
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
  public void testFileStatusWithECschema() throws Exception {
    // test directory not in EC zone
    final Path dir = new Path("/foo");
    assertTrue(fs.mkdir(dir, FsPermission.getDirDefault()));
    assertNull(client.getFileInfo(dir.toString()).getECSchema());
    // test file not in EC zone
    final Path file = new Path(dir, "foo");
    fs.create(file).close();
    assertNull(client.getFileInfo(file.toString()).getECSchema());
    fs.delete(file, true);

    final ECSchema schema1 = ErasureCodingSchemaManager.getSystemDefaultSchema();
    // create EC zone on dir
    fs.createErasureCodingZone(dir, schema1, 0);
    final ECSchema schame2 = client.getFileInfo(dir.toUri().getPath()).getECSchema();
    assertNotNull(schame2);
    assertTrue(schema1.equals(schame2));

    // test file in EC zone
    fs.create(file).close();
    final ECSchema schame3 =
        fs.getClient().getFileInfo(file.toUri().getPath()).getECSchema();
    assertNotNull(schame3);
    assertTrue(schema1.equals(schame3));
  }
}
