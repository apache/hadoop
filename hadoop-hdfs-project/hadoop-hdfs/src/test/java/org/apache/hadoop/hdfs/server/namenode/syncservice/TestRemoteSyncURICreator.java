package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.junit.Test;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePath;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.RemoteSyncURICreator.createRemotePathFromAbsolutePath;
import static org.junit.Assert.assertEquals;

public class TestRemoteSyncURICreator {

  @Test
  public void testFileInRoot() {
    SyncMount syncMount = new SyncMount("test-bm",
        new Path("/irrelavant/local/path/to/backup/dir"),
        UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/").build());
    URI remotePath = createRemotePath(syncMount, "file.bin");
    assertEquals(UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/file.bin").build(),
        remotePath);
  }


  @Test
  public void testDirectoriesAndFile() {
    SyncMount syncMount = new SyncMount("test-bm",
        new Path("/irrelavant/local/path/to/backup/dir"),
        UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/").build());
    URI remotePath = createRemotePath(syncMount, "a/b/c/file.bin");
    assertEquals(UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/a/b/c/file.bin").build(),
        remotePath);
  }

  @Test
  public void testAbsoluteCaseFileInRoot() {
    SyncMount syncMount = new SyncMount("test-bm",
        new Path("/local/backup-dir-path/"),
        UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/").build());
    URI remotePath = createRemotePathFromAbsolutePath(syncMount, "/local/backup-dir-path/file.bin");
    assertEquals(UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/file.bin").build(),
        remotePath);
  }

  @Test
  public void testAbsoluteCaseFileDirectoriesAndFile() {
    SyncMount syncMount = new SyncMount("test-bm",
        new Path("/local/backup-dir-path/"),
        UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/").build());
    URI remotePath = createRemotePathFromAbsolutePath(syncMount, "/local/backup-dir-path/a/b/c/file.bin");
    assertEquals(UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/a/b/c/file.bin").build(),
        remotePath);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testAbsoluteCaseNoSharedPrefixThrowsException() {
    SyncMount syncMount = new SyncMount("test-bm",
        new Path("/local/backup-dir-path/"),
        UriBuilder.fromUri("hdfs://127.0.0.1:0/remote-path/").build());
    URI remotePath = createRemotePathFromAbsolutePath(syncMount, "/local/alternative-backup-dir-path/a/b/c/file.bin");

  }


}