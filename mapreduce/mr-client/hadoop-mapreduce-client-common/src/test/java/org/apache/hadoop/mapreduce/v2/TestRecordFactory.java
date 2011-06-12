package org.apache.hadoop.mapreduce.v2;

import junit.framework.Assert;

import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetCountersRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.CounterGroupPBImpl;
import org.junit.Test;

public class TestRecordFactory {
  
  @Test
  public void testPbRecordFactory() {
    RecordFactory pbRecordFactory = RecordFactoryPBImpl.get();
    
    try {
      CounterGroup response = pbRecordFactory.newRecordInstance(CounterGroup.class);
      Assert.assertEquals(CounterGroupPBImpl.class, response.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
    
    try {
      GetCountersRequest response = pbRecordFactory.newRecordInstance(GetCountersRequest.class);
      Assert.assertEquals(GetCountersRequestPBImpl.class, response.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
  }

}
