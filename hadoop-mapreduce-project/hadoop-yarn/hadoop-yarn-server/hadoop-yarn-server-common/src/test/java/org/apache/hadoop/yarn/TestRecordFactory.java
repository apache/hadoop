package org.apache.hadoop.yarn;

import junit.framework.Assert;

import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.impl.pb.HeartbeatResponsePBImpl;
import org.junit.Test;

public class TestRecordFactory {
  
  @Test
  public void testPbRecordFactory() {
    RecordFactory pbRecordFactory = RecordFactoryPBImpl.get();
    
    try {
      HeartbeatResponse response = pbRecordFactory.newRecordInstance(HeartbeatResponse.class);
      Assert.assertEquals(HeartbeatResponsePBImpl.class, response.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
    
    try {
      NodeHeartbeatRequest request = pbRecordFactory.newRecordInstance(NodeHeartbeatRequest.class);
      Assert.assertEquals(NodeHeartbeatRequestPBImpl.class, request.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
    
  }

}
