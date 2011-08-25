package org.apache.hadoop.yarn;

import junit.framework.Assert;

import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.impl.pb.AMResponsePBImpl;
import org.junit.Test;

public class TestRecordFactory {
  
  @Test
  public void testPbRecordFactory() {
    RecordFactory pbRecordFactory = RecordFactoryPBImpl.get();
    
    try {
      AMResponse response = pbRecordFactory.newRecordInstance(AMResponse.class);
      Assert.assertEquals(AMResponsePBImpl.class, response.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
    
    try {
      AllocateRequest response = pbRecordFactory.newRecordInstance(AllocateRequest.class);
      Assert.assertEquals(AllocateRequestPBImpl.class, response.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
  }

}
