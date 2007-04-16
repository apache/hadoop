package org.apache.hadoop.net;


import org.apache.hadoop.dfs.DatanodeDescriptor;
import org.apache.hadoop.dfs.DatanodeID;
import junit.framework.TestCase;

public class TestNetworkTopology extends TestCase {
  private final static NetworkTopology cluster = new NetworkTopology();
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
  
  static {
    for(int i=0; i<dataNodes.length; i++) {
      cluster.add( dataNodes[i] );
    }
  }
  
  public void testContains() throws Exception {
    for(int i=0; i<dataNodes.length; i++) {
      assertTrue( cluster.contains(dataNodes[i]));
    }
    assertFalse( cluster.contains( NODE ));
  }
  
  public void testNumOfChildren() throws Exception {
    assertEquals(cluster.getNumOfLeaves(), dataNodes.length);
  }

  public void testRacks() throws Exception {
    assertEquals(cluster.getNumOfRacks(), 3);
    assertTrue(cluster.isOnSameRack(dataNodes[0], dataNodes[1]));
    assertFalse(cluster.isOnSameRack(dataNodes[1], dataNodes[2]));
    assertTrue(cluster.isOnSameRack(dataNodes[2], dataNodes[3]));
    assertTrue(cluster.isOnSameRack(dataNodes[3], dataNodes[4]));
    assertFalse(cluster.isOnSameRack(dataNodes[4], dataNodes[5]));
    assertTrue(cluster.isOnSameRack(dataNodes[5], dataNodes[6]));
  }
  
  public void testGetDistance() throws Exception {
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[0]), 0);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[1]), 2);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[3]), 4);
    assertEquals(cluster.getDistance(dataNodes[0], dataNodes[6]), 6);
  }

  public void testPseudoSortByDistance() throws Exception {
    DatanodeDescriptor[] testNodes = new DatanodeDescriptor[3];
    
    // array contains both local node & local rack node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[2];
    testNodes[2] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[2]);

    // array contains local node
    testNodes[0] = dataNodes[1];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[0];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[0]);
    assertTrue(testNodes[1] == dataNodes[1]);
    assertTrue(testNodes[2] == dataNodes[3]);

    // array contains local rack node
    testNodes[0] = dataNodes[5];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[1];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[1]);
    assertTrue(testNodes[1] == dataNodes[3]);
    assertTrue(testNodes[2] == dataNodes[5]);

    // array contains neither local node & local rack node
    testNodes[0] = dataNodes[5];
    testNodes[1] = dataNodes[3];
    testNodes[2] = dataNodes[2];
    cluster.pseudoSortByDistance(dataNodes[0], testNodes );
    assertTrue(testNodes[0] == dataNodes[5]);
    assertTrue(testNodes[1] == dataNodes[3]);
    assertTrue(testNodes[2] == dataNodes[2]);
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
