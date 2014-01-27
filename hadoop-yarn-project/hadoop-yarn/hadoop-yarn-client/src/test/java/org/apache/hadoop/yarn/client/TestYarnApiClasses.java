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
package org.apache.hadoop.yarn.client;



import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestYarnApiClasses {
  private final org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);

  /**
   * Simple test Resource request.
   * Test hashCode, equals and compare.
   */
  @Test
  public void testResourceRequest() {

    Resource resource = recordFactory.newRecordInstance(Resource.class);
    Priority priority = recordFactory.newRecordInstance(Priority.class);

    ResourceRequest original = ResourceRequest.newInstance(priority, "localhost", resource, 2) ;

    ResourceRequest copy = ResourceRequest.newInstance(priority, "localhost", resource, 2);

    assertTrue(original.equals(copy));
    assertEquals(0, original.compareTo(copy));
    assertTrue(original.hashCode() == copy.hashCode());

    copy.setNumContainers(1);

    assertFalse(original.equals(copy));
    assertNotSame(0, original.compareTo(copy));
    assertFalse(original.hashCode() == copy.hashCode());

  }

  /**
  * Test CancelDelegationTokenRequestPBImpl.
  * Test a transformation to prototype and back
  */
  @Test
  public void testCancelDelegationTokenRequestPBImpl() {

    Token token = getDelegationToken();

    CancelDelegationTokenRequestPBImpl original = new CancelDelegationTokenRequestPBImpl();
    original.setDelegationToken(token);
    CancelDelegationTokenRequestProto protoType = original.getProto();

    CancelDelegationTokenRequestPBImpl copy = new CancelDelegationTokenRequestPBImpl(protoType);
    assertNotNull(copy.getDelegationToken());
    //compare source and converted
    assertEquals(token, copy.getDelegationToken());

  }

  /**
  * Test RenewDelegationTokenRequestPBImpl.
  * Test a transformation to prototype and back
  */

  @Test
  public void testRenewDelegationTokenRequestPBImpl() {

    Token token = getDelegationToken();

    RenewDelegationTokenRequestPBImpl original = new RenewDelegationTokenRequestPBImpl();
    original.setDelegationToken(token);
    RenewDelegationTokenRequestProto protoType = original.getProto();

    RenewDelegationTokenRequestPBImpl copy = new RenewDelegationTokenRequestPBImpl(protoType);
    assertNotNull(copy.getDelegationToken());
    //compare source and converted
    assertEquals(token, copy.getDelegationToken());

  }

 
  private Token getDelegationToken() {
    return Token.newInstance(new byte[0], "", new byte[0], "");
  }


}
