package org.apache.hadoop.hdfs.server.namenode;

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NumberReplicas;

import junit.framework.TestCase;

/**
 * Test if live nodes count per node is correct 
 * so NN makes right decision for under/over-replicated blocks
 */
public class TestNodeCount extends TestCase {
  public void testNodeCount() throws Exception {
    // start a mini dfs cluster of 2 nodes
    final Configuration conf = new Configuration();
    final short REPLICATION_FACTOR = (short)2;
    final MiniDFSCluster cluster = 
      new MiniDFSCluster(conf, REPLICATION_FACTOR, true, null);
    try {
      final FSNamesystem namesystem = cluster.getNameNode().namesystem;
      final FileSystem fs = cluster.getFileSystem();
      
      // populate the cluster with a one block file
      final Path FILE_PATH = new Path("/testfile");
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      Block block = DFSTestUtil.getFirstBlock(fs, FILE_PATH);

      // keep a copy of all datanode descriptor
      DatanodeDescriptor[] datanodes = (DatanodeDescriptor[])
         namesystem.heartbeats.toArray(new DatanodeDescriptor[REPLICATION_FACTOR]);
      
      // start two new nodes
      cluster.startDataNodes(conf, 2, true, null, null);
      cluster.waitActive();
      
      // bring down first datanode
      DatanodeDescriptor datanode = datanodes[0];
      DataNodeProperties dnprop = cluster.stopDataNode(datanode.getName());
      // make sure that NN detects that the datanode is down
      synchronized (namesystem.heartbeats) {
        datanode.setLastUpdate(0); // mark it dead
        namesystem.heartbeatCheck();
      }
      // the block will be replicated
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);

      // restart the first datanode
      cluster.restartDataNode(dnprop);
      cluster.waitActive();
      
      // check if excessive replica is detected
      NumberReplicas num = null;
      do {
       synchronized (namesystem) {
         num = namesystem.countNodes(block);
       }
      } while (num.excessReplicas() == 0);
      
      // find out a non-excess node
      Iterator<DatanodeDescriptor> iter = namesystem.blocksMap.nodeIterator(block);
      DatanodeDescriptor nonExcessDN = null;
      while (iter.hasNext()) {
        DatanodeDescriptor dn = iter.next();
        Collection<Block> blocks = namesystem.excessReplicateMap.get(dn.getStorageID());
        if (blocks == null || !blocks.contains(block) ) {
          nonExcessDN = dn;
          break;
        }
      }
      assertTrue(nonExcessDN!=null);
      
      // bring down non excessive datanode
      dnprop = cluster.stopDataNode(nonExcessDN.getName());
      // make sure that NN detects that the datanode is down
      synchronized (namesystem.heartbeats) {
        nonExcessDN.setLastUpdate(0); // mark it dead
        namesystem.heartbeatCheck();
      }
      
      // The block should be replicated
      do {
        num = namesystem.countNodes(block);
      } while (num.liveReplicas() != REPLICATION_FACTOR);
      
      // restart the first datanode
      cluster.restartDataNode(dnprop);
      cluster.waitActive();
      
      // check if excessive replica is detected
      do {
       num = namesystem.countNodes(block);
      } while (num.excessReplicas() != 2);
    } finally {
      cluster.shutdown();
    }
  }
}
