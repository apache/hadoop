package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import junit.framework.TestCase;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestComputeInvalidateWork extends TestCase {
  /**
   * Test if {@link FSNamesystem#computeInvalidateWork(int)}
   * can schedule invalidate work correctly 
   */
  public void testCompInvalidate() throws Exception {
    final Configuration conf = new Configuration();
    final int NUM_OF_DATANODES = 3;
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, NUM_OF_DATANODES, true, null);
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
      DatanodeDescriptor[] nodes =
        namesystem.heartbeats.toArray(new DatanodeDescriptor[NUM_OF_DATANODES]);
      assertEquals(nodes.length, NUM_OF_DATANODES);
      
      synchronized (namesystem) {
      for (int i=0; i<nodes.length; i++) {
        for(int j=0; j<3*namesystem.blockInvalidateLimit+1; j++) {
          Block block = new Block(i*(namesystem.blockInvalidateLimit+1)+j, 0, 
              GenerationStamp.FIRST_VALID_STAMP);
          namesystem.addToInvalidatesNoLog(block, nodes[i]);
        }
      }
      
      assertEquals(namesystem.blockInvalidateLimit*NUM_OF_DATANODES, 
          namesystem.computeInvalidateWork(NUM_OF_DATANODES+1));
      assertEquals(namesystem.blockInvalidateLimit*NUM_OF_DATANODES, 
          namesystem.computeInvalidateWork(NUM_OF_DATANODES));
      assertEquals(namesystem.blockInvalidateLimit*(NUM_OF_DATANODES-1), 
          namesystem.computeInvalidateWork(NUM_OF_DATANODES-1));
      int workCount = namesystem.computeInvalidateWork(1);
      if (workCount == 1) {
        assertEquals(namesystem.blockInvalidateLimit+1, 
            namesystem.computeInvalidateWork(2));        
      } else {
        assertEquals(workCount, namesystem.blockInvalidateLimit);
        assertEquals(2, namesystem.computeInvalidateWork(2));
      }
      }
    } finally {
      cluster.shutdown();
    }
  }
}
