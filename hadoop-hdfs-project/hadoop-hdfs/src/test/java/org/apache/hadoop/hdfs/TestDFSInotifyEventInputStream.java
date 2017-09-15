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

package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestDFSInotifyEventInputStream {

  private static final int BLOCK_SIZE = 1024;
  private static final Log LOG = LogFactory.getLog(
      TestDFSInotifyEventInputStream.class);

  public static EventBatch waitForNextEvents(DFSInotifyEventInputStream eis)
    throws IOException, MissingEventsException {
    EventBatch batch = null;
    while ((batch = eis.poll()) == null);
    return batch;
  }

  private static long checkTxid(EventBatch batch, long prevTxid){
    Assert.assertTrue("Previous txid " + prevTxid + " was not less than " +
        "new txid " + batch.getTxid(), prevTxid < batch.getTxid());
    return batch.getTxid();
  }

  /**
   * If this test fails, check whether the newly added op should map to an
   * inotify event, and if so, establish the mapping in
   * {@link org.apache.hadoop.hdfs.server.namenode.InotifyFSEditLogOpTranslator}
   * and update testBasic() to include the new op.
   */
  @Test
  public void testOpcodeCount() {
    Assert.assertEquals(54, FSEditLogOpCodes.values().length);
  }


  /**
   * Tests all FsEditLogOps that are converted to inotify events.
   */
  @Test(timeout = 120000)
  @SuppressWarnings("deprecation")
  public void testBasic() throws IOException, URISyntaxException,
      InterruptedException, MissingEventsException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    // so that we can get an atime change
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);

    MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
    builder.getDfsBuilder().numDataNodes(2);
    MiniQJMHACluster cluster = builder.build();

    try {
      cluster.getDfsCluster().waitActive();
      cluster.getDfsCluster().transitionToActive(0);
      DFSClient client = new DFSClient(cluster.getDfsCluster().getNameNode(0)
          .getNameNodeAddress(), conf);
      FileSystem fs = cluster.getDfsCluster().getFileSystem(0);
      DFSTestUtil.createFile(fs, new Path("/file"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/file3"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/file5"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/truncate_file"),
          BLOCK_SIZE * 2, (short) 1, 0L);
      DFSInotifyEventInputStream eis = client.getInotifyEventStream();
      client.rename("/file", "/file4", null); // RenameOp -> RenameEvent
      client.rename("/file4", "/file2"); // RenameOldOp -> RenameEvent
      // DeleteOp, AddOp -> UnlinkEvent, CreateEvent
      OutputStream os = client.create("/file2", true, (short) 2, BLOCK_SIZE);
      os.write(new byte[BLOCK_SIZE]);
      os.close(); // CloseOp -> CloseEvent
      // AddOp -> AppendEvent
      os = client.append("/file2", BLOCK_SIZE, EnumSet.of(CreateFlag.APPEND),
          null, null);
      os.write(new byte[BLOCK_SIZE]);
      os.close(); // CloseOp -> CloseEvent
      Thread.sleep(10); // so that the atime will get updated on the next line
      client.open("/file2").read(new byte[1]); // TimesOp -> MetadataUpdateEvent
      // SetReplicationOp -> MetadataUpdateEvent
      client.setReplication("/file2", (short) 1);
      // ConcatDeleteOp -> AppendEvent, UnlinkEvent, CloseEvent
      client.concat("/file2", new String[]{"/file3"});
      client.delete("/file2", false); // DeleteOp -> UnlinkEvent
      client.mkdirs("/dir", null, false); // MkdirOp -> CreateEvent
      // SetPermissionsOp -> MetadataUpdateEvent
      client.setPermission("/dir", FsPermission.valueOf("-rw-rw-rw-"));
      // SetOwnerOp -> MetadataUpdateEvent
      client.setOwner("/dir", "username", "groupname");
      client.createSymlink("/dir", "/dir2", false); // SymlinkOp -> CreateEvent
      client.setXAttr("/file5", "user.field", "value".getBytes(), EnumSet.of(
          XAttrSetFlag.CREATE)); // SetXAttrOp -> MetadataUpdateEvent
      // RemoveXAttrOp -> MetadataUpdateEvent
      client.removeXAttr("/file5", "user.field");
      // SetAclOp -> MetadataUpdateEvent
      client.setAcl("/file5", AclEntry.parseAclSpec(
          "user::rwx,user:foo:rw-,group::r--,other::---", true));
      client.removeAcl("/file5"); // SetAclOp -> MetadataUpdateEvent
      client.rename("/file5", "/dir"); // RenameOldOp -> RenameEvent
      //TruncateOp -> TruncateEvent
      client.truncate("/truncate_file", BLOCK_SIZE);
      EventBatch batch = null;

      // RenameOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      long txid = batch.getTxid();
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.RENAME);
      Event.RenameEvent re = (Event.RenameEvent) batch.getEvents()[0];
      Assert.assertEquals("/file4", re.getDstPath());
      Assert.assertEquals("/file", re.getSrcPath());
      Assert.assertTrue(re.getTimestamp() > 0);
      LOG.info(re.toString());
      Assert.assertTrue(re.toString().startsWith("RenameEvent [srcPath="));

      long eventsBehind = eis.getTxidsBehindEstimate();

      // RenameOldOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.RENAME);
      Event.RenameEvent re2 = (Event.RenameEvent) batch.getEvents()[0];
      Assert.assertTrue(re2.getDstPath().equals("/file2"));
      Assert.assertTrue(re2.getSrcPath().equals("/file4"));
      Assert.assertTrue(re2.getTimestamp() > 0);
      LOG.info(re2.toString());

      // AddOp with overwrite
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
      Event.CreateEvent ce = (Event.CreateEvent) batch.getEvents()[0];
      Assert.assertTrue(ce.getiNodeType() == Event.CreateEvent.INodeType.FILE);
      Assert.assertTrue(ce.getPath().equals("/file2"));
      Assert.assertTrue(ce.getCtime() > 0);
      Assert.assertTrue(ce.getReplication() > 0);
      Assert.assertTrue(ce.getSymlinkTarget() == null);
      Assert.assertTrue(ce.getOverwrite());
      Assert.assertEquals(BLOCK_SIZE, ce.getDefaultBlockSize());
      LOG.info(ce.toString());
      Assert.assertTrue(ce.toString().startsWith("CreateEvent [INodeType="));

      // CloseOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CLOSE);
      Event.CloseEvent ce2 = (Event.CloseEvent) batch.getEvents()[0];
      Assert.assertTrue(ce2.getPath().equals("/file2"));
      Assert.assertTrue(ce2.getFileSize() > 0);
      Assert.assertTrue(ce2.getTimestamp() > 0);
      LOG.info(ce2.toString());
      Assert.assertTrue(ce2.toString().startsWith("CloseEvent [path="));

      // AppendOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.APPEND);
      Event.AppendEvent append2 = (Event.AppendEvent)batch.getEvents()[0];
      Assert.assertEquals("/file2", append2.getPath());
      Assert.assertFalse(append2.toNewBlock());
      LOG.info(append2.toString());
      Assert.assertTrue(append2.toString().startsWith("AppendEvent [path="));

      // CloseOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CLOSE);
      Assert.assertTrue(((Event.CloseEvent) batch.getEvents()[0]).getPath().equals("/file2"));

      // TimesOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue.getPath().equals("/file2"));
      Assert.assertTrue(mue.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.TIMES);
      LOG.info(mue.toString());
      Assert.assertTrue(mue.toString().startsWith("MetadataUpdateEvent [path="));

      // SetReplicationOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue2 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue2.getPath().equals("/file2"));
      Assert.assertTrue(mue2.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.REPLICATION);
      Assert.assertTrue(mue2.getReplication() == 1);
      LOG.info(mue2.toString());

      // ConcatDeleteOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(3, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.APPEND);
      Assert.assertTrue(((Event.AppendEvent) batch.getEvents()[0]).getPath().equals("/file2"));
      Assert.assertTrue(batch.getEvents()[1].getEventType() == Event.EventType.UNLINK);
      Event.UnlinkEvent ue2 = (Event.UnlinkEvent) batch.getEvents()[1];
      Assert.assertTrue(ue2.getPath().equals("/file3"));
      Assert.assertTrue(ue2.getTimestamp() > 0);
      LOG.info(ue2.toString());
      Assert.assertTrue(ue2.toString().startsWith("UnlinkEvent [path="));
      Assert.assertTrue(batch.getEvents()[2].getEventType() == Event.EventType.CLOSE);
      Event.CloseEvent ce3 = (Event.CloseEvent) batch.getEvents()[2];
      Assert.assertTrue(ce3.getPath().equals("/file2"));
      Assert.assertTrue(ce3.getTimestamp() > 0);

      // DeleteOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.UNLINK);
      Event.UnlinkEvent ue = (Event.UnlinkEvent) batch.getEvents()[0];
      Assert.assertTrue(ue.getPath().equals("/file2"));
      Assert.assertTrue(ue.getTimestamp() > 0);
      LOG.info(ue.toString());

      // MkdirOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
      Event.CreateEvent ce4 = (Event.CreateEvent) batch.getEvents()[0];
      Assert.assertTrue(ce4.getiNodeType() ==
          Event.CreateEvent.INodeType.DIRECTORY);
      Assert.assertTrue(ce4.getPath().equals("/dir"));
      Assert.assertTrue(ce4.getCtime() > 0);
      Assert.assertTrue(ce4.getReplication() == 0);
      Assert.assertTrue(ce4.getSymlinkTarget() == null);
      LOG.info(ce4.toString());

      // SetPermissionsOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue3 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue3.getPath().equals("/dir"));
      Assert.assertTrue(mue3.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.PERMS);
      Assert.assertTrue(mue3.getPerms().toString().contains("rw-rw-rw-"));
      LOG.info(mue3.toString());

      // SetOwnerOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue4 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue4.getPath().equals("/dir"));
      Assert.assertTrue(mue4.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.OWNER);
      Assert.assertTrue(mue4.getOwnerName().equals("username"));
      Assert.assertTrue(mue4.getGroupName().equals("groupname"));
      LOG.info(mue4.toString());

      // SymlinkOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
      Event.CreateEvent ce5 = (Event.CreateEvent) batch.getEvents()[0];
      Assert.assertTrue(ce5.getiNodeType() ==
          Event.CreateEvent.INodeType.SYMLINK);
      Assert.assertTrue(ce5.getPath().equals("/dir2"));
      Assert.assertTrue(ce5.getCtime() > 0);
      Assert.assertTrue(ce5.getReplication() == 0);
      Assert.assertTrue(ce5.getSymlinkTarget().equals("/dir"));
      LOG.info(ce5.toString());

      // SetXAttrOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue5 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue5.getPath().equals("/file5"));
      Assert.assertTrue(mue5.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.XATTRS);
      Assert.assertTrue(mue5.getxAttrs().size() == 1);
      Assert.assertTrue(mue5.getxAttrs().get(0).getName().contains("field"));
      Assert.assertTrue(!mue5.isxAttrsRemoved());
      LOG.info(mue5.toString());

      // RemoveXAttrOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue6 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue6.getPath().equals("/file5"));
      Assert.assertTrue(mue6.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.XATTRS);
      Assert.assertTrue(mue6.getxAttrs().size() == 1);
      Assert.assertTrue(mue6.getxAttrs().get(0).getName().contains("field"));
      Assert.assertTrue(mue6.isxAttrsRemoved());
      LOG.info(mue6.toString());

      // SetAclOp (1)
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue7 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue7.getPath().equals("/file5"));
      Assert.assertTrue(mue7.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.ACLS);
      Assert.assertTrue(mue7.getAcls().contains(
          AclEntry.parseAclEntry("user::rwx", true)));
      LOG.info(mue7.toString());

      // SetAclOp (2)
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.METADATA);
      Event.MetadataUpdateEvent mue8 = (Event.MetadataUpdateEvent) batch.getEvents()[0];
      Assert.assertTrue(mue8.getPath().equals("/file5"));
      Assert.assertTrue(mue8.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.ACLS);
      Assert.assertTrue(mue8.getAcls() == null);
      LOG.info(mue8.toString());

      // RenameOp (2)
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.RENAME);
      Event.RenameEvent re3 = (Event.RenameEvent) batch.getEvents()[0];
      Assert.assertTrue(re3.getDstPath().equals("/dir/file5"));
      Assert.assertTrue(re3.getSrcPath().equals("/file5"));
      Assert.assertTrue(re3.getTimestamp() > 0);
      LOG.info(re3.toString());

      // TruncateOp
      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert
          .assertTrue(batch.getEvents()[0].getEventType() ==
          Event.EventType.TRUNCATE);
      Event.TruncateEvent et = ((Event.TruncateEvent) batch.getEvents()[0]);
      Assert.assertTrue(et.getPath().equals("/truncate_file"));
      Assert.assertTrue(et.getFileSize() == BLOCK_SIZE);
      Assert.assertTrue(et.getTimestamp() > 0);
      LOG.info(et.toString());
      Assert.assertTrue(et.toString().startsWith("TruncateEvent [path="));

      // Returns null when there are no further events
      Assert.assertTrue(eis.poll() == null);

      // make sure the estimate hasn't changed since the above assertion
      // tells us that we are fully caught up to the current namesystem state
      // and we should not have been behind at all when eventsBehind was set
      // either, since there were few enough events that they should have all
      // been read to the client during the first poll() call
      Assert.assertTrue(eis.getTxidsBehindEstimate() == eventsBehind);

    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testNNFailover() throws IOException, URISyntaxException,
      MissingEventsException {
    Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).build();

    try {
      cluster.getDfsCluster().waitActive();
      cluster.getDfsCluster().transitionToActive(0);
      DFSClient client = ((DistributedFileSystem) HATestUtil.configureFailoverFs
          (cluster.getDfsCluster(), conf)).dfs;
      DFSInotifyEventInputStream eis = client.getInotifyEventStream();
      for (int i = 0; i < 10; i++) {
        client.mkdirs("/dir" + i, null, false);
      }
      cluster.getDfsCluster().shutdownNameNode(0);
      cluster.getDfsCluster().transitionToActive(1);
      EventBatch batch = null;
      // we can read all of the edits logged by the old active from the new
      // active
      for (int i = 0; i < 10; i++) {
        batch = waitForNextEvents(eis);
        Assert.assertEquals(1, batch.getEvents().length);
        Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
        Assert.assertTrue(((Event.CreateEvent) batch.getEvents()[0]).getPath().equals("/dir" +
            i));
      }
      Assert.assertTrue(eis.poll() == null);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testTwoActiveNNs() throws IOException, MissingEventsException {
    Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).build();

    try {
      cluster.getDfsCluster().waitActive();
      cluster.getDfsCluster().transitionToActive(0);
      DFSClient client0 = new DFSClient(cluster.getDfsCluster().getNameNode(0)
          .getNameNodeAddress(), conf);
      DFSClient client1 = new DFSClient(cluster.getDfsCluster().getNameNode(1)
          .getNameNodeAddress(), conf);
      DFSInotifyEventInputStream eis = client0.getInotifyEventStream();
      for (int i = 0; i < 10; i++) {
        client0.mkdirs("/dir" + i, null, false);
      }

      cluster.getDfsCluster().transitionToActive(1);
      for (int i = 10; i < 20; i++) {
        client1.mkdirs("/dir" + i, null, false);
      }

      // make sure that the old active can't read any further than the edits
      // it logged itself (it has no idea whether the in-progress edits from
      // the other writer have actually been committed)
      EventBatch batch = null;
      for (int i = 0; i < 10; i++) {
        batch = waitForNextEvents(eis);
        Assert.assertEquals(1, batch.getEvents().length);
        Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
        Assert.assertTrue(((Event.CreateEvent) batch.getEvents()[0]).getPath().equals("/dir" +
            i));
      }
      Assert.assertTrue(eis.poll() == null);
    } finally {
      try {
        cluster.shutdown();
      } catch (ExitUtil.ExitException e) {
        // expected because the old active will be unable to flush the
        // end-of-segment op since it is fenced
      }
    }
  }

  @Test(timeout = 120000)
  public void testReadEventsWithTimeout() throws IOException,
      InterruptedException, MissingEventsException {
    Configuration conf = new HdfsConfiguration();
    MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).build();

    try {
      cluster.getDfsCluster().waitActive();
      cluster.getDfsCluster().transitionToActive(0);
      final DFSClient client = new DFSClient(cluster.getDfsCluster()
          .getNameNode(0).getNameNodeAddress(), conf);
      DFSInotifyEventInputStream eis = client.getInotifyEventStream();
      ScheduledExecutorService ex = Executors
          .newSingleThreadScheduledExecutor();
      ex.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            client.mkdirs("/dir", null, false);
          } catch (IOException e) {
            // test will fail
            LOG.error("Unable to create /dir", e);
          }
        }
      }, 1, TimeUnit.SECONDS);
      // a very generous wait period -- the edit will definitely have been
      // processed by the time this is up
      EventBatch batch = eis.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(batch);
      Assert.assertEquals(1, batch.getEvents().length);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
      Assert.assertEquals("/dir", ((Event.CreateEvent) batch.getEvents()[0]).getPath());
    } finally {
      cluster.shutdown();
    }
  }
}
