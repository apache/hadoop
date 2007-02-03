package org.apache.hadoop.net;

import java.util.HashSet;
import org.apache.hadoop.dfs.DatanodeDescriptor;
import org.apache.hadoop.dfs.DatanodeID;
import junit.framework.TestCase;

public class TestNetworkTopology extends TestCase {
  private NetworkTopology cluster = new NetworkTopology();
  private final static DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
      new DatanodeDescriptor(new DatanodeID("h1:5020", "0", -1), "/d1/r1"),
      new DatanodeDescriptor(new DatanodeID("h2:5020", "0", -1), "/d1/r1"),
      new DatanodeDescriptor(new DatanodeID("h3:5020", "0", -1), "/d1/r2"),
      new DatanodeDescriptor(new DatanodeID("h4:5020", "0", -1), "/d1/r2"),
      new DatanodeDescriptor(new DatanodeID("h5:5020", "0", -1), "/d1/r2"),
      new DatanodeDescriptor(new DatanodeID("h6:5020", "0", -1), "/d2/r3"),
      new DatanodeDescriptor(new DatanodeID("h7:5020", "0", -1), "/d2/r3")
  };
  private final static DatanodeDescriptor NODE = 
    new DatanodeDescriptor(new DatanodeID("h8:5020", "0", -1), "/d2/r4");
  
  public TestNetworkTopology() {
    for(int i=0; i<dataNodes.length; i++) {
      cluster.add( dataNodes[i] );
    }    
  }
  
  public void testContains() {
    for(int i=0; i<dataNodes.length; i++) {
      assertTrue( cluster.contains(dataNodes[i]));
    }
    assertFalse( cluster.contains( NODE ));
  }
  
  public void testNumOfChildren() throws Exception {
    assertEquals(cluster.getNumOfLeaves(), dataNodes.length);
  }

  public void testNumOfRacks() throws Exception {
    assertEquals(cluster.getNumOfRacks(), 3);
  }
  
  public void testGetLeaves() throws Exception {
    DatanodeDescriptor [] leaves = cluster.getLeaves(NodeBase.ROOT);
    assertEquals(leaves.length, dataNodes.length);
    HashSet<DatanodeDescriptor> set1 = 
      new HashSet<DatanodeDescriptor>(leaves.length);
    HashSet<DatanodeDescriptor> set2 = 
      new HashSet<DatanodeDescriptor>(dataNodes.length);
    for(int i=0; i<leaves.length; i++) {
      set1.add(leaves[i]);
      set2.add(dataNodes[i]);
    }
    assertTrue(set1.equals(set2));
  }
  
  public void testGetDistance() throws Exception {
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[0]), 0);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[1]), 2);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[3]), 4);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[6]), 6);
  }

  public void testRemove() throws Exception {
    for(int i=0; i<dataNodes.length; i++) {
      cluster.remove( dataNodes[i] );
    }
    for(int i=0; i<dataNodes.length; i++) {
      assertFalse( cluster.contains( dataNodes[i] ) );
    }
    assertEquals(0, cluster.getNumOfLeaves());
    for(int i=0; i<dataNodes.length; i++) {
      cluster.add( dataNodes[i] );
    }
  }


}
