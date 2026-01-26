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

package org.apache.hadoop.mapreduce.v2;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetCountersRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.CounterGroupPBImpl;
import org.junit.jupiter.api.Test;

public class TestRecordFactory {

  @Test
  public void testPbRecordFactory() {
    RecordFactory pbRecordFactory = RecordFactoryPBImpl.get();

    try {
      CounterGroup response = pbRecordFactory.newRecordInstance(CounterGroup.class);
      assertEquals(CounterGroupPBImpl.class, response.getClass());
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      fail("Failed to crete record");
    }

    try {
      GetCountersRequest response = pbRecordFactory.newRecordInstance(GetCountersRequest.class);
      assertEquals(GetCountersRequestPBImpl.class, response.getClass());
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      fail("Failed to crete record");
    }
  }

}
