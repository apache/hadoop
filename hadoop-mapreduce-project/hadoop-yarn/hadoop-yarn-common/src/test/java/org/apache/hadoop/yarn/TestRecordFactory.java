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
