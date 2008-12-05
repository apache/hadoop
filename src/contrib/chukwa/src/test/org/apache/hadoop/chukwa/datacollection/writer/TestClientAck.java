package org.apache.hadoop.chukwa.datacollection.writer;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.writer.ClientAck;

public class TestClientAck extends TestCase
{

  public void testWait4AckTimeOut()
  {
    ClientAck clientAck = new ClientAck();
    long startDate = System.currentTimeMillis();
    clientAck.wait4Ack();
    long now = System.currentTimeMillis();
    long duration = now - startDate ;
    duration = duration - clientAck.getTimeOut();
    
    Assert.assertTrue("should not wait nore than " 
        + clientAck.getTimeOut() + " + 7sec" , duration < 7000);
    Assert.assertEquals(ClientAck.KO_LOCK, clientAck.getStatus());
  }

}
