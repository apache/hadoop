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
package org.apache.hadoop.hdfs.server.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * This is a unit test, which tests {@link Util#stringAsURI(String)}
 * for IDs being used in HDFS, e.g. ClusterID and BlockPoolID.
 */
public class TestStorageInfo extends TestCase {

  /**
   * Test write() / readFieds() of StroageInfo.  Write StorageInfo into a buffer
   * then read it back and the result should be the same with the original one.
   * @throws IOException 
   */
  public void testStorageInfo() throws IOException {
    
    int nsID = 123;
    String cid = "cid-test";
    int layoutV = 234;
    long cT = 0L;
    
    StorageInfo sinfo = new StorageInfo(layoutV, nsID, cid,  cT);
    
    Assert.assertNotNull(sinfo);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(bos);

    try {
        // we need to first create an DataOutputStream for sinfo to write into
        sinfo.write(output);
        //remember to close the DataOutputStream 
        //to make sure the data has been written
        bos.close();
        
        // convert ByteArrayInputStream to ByteArrayOutputStream
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bis);

        StorageInfo secondsinfo = new StorageInfo();
        secondsinfo.readFields(dataInputStream);
        
        // compare
        Assert.assertEquals(sinfo.getClusterID(), secondsinfo.getClusterID());
        Assert.assertEquals(sinfo.getNamespaceID(), secondsinfo.getNamespaceID());
        Assert.assertEquals(sinfo.getLayoutVersion(), secondsinfo.getLayoutVersion());
        Assert.assertEquals(sinfo.getCTime(), secondsinfo.getCTime());
    }catch (IOException e) {
      e.getMessage();
    }
  }
}

