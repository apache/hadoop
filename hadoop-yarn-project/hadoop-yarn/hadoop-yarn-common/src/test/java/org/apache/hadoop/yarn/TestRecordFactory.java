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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TestRecordFactory {

  @Test
  void testPbRecordFactory() {
    RecordFactory pbRecordFactory = RecordFactoryPBImpl.get();

    try {
      AllocateResponse response =
          pbRecordFactory.newRecordInstance(AllocateResponse.class);
      assertEquals(AllocateResponsePBImpl.class, response.getClass());
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      fail("Failed to crete record");
    }

    try {
      AllocateRequest response =
          pbRecordFactory.newRecordInstance(AllocateRequest.class);
      assertEquals(AllocateRequestPBImpl.class, response.getClass());
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      fail("Failed to crete record");
    }
  }

}
