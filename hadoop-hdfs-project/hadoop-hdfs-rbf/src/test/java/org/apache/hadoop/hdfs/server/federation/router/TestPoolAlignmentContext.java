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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class TestPoolAlignmentContext {
  @Test
  public void testNamenodeRequestsOnlyUsePoolLocalStateID() {
    RouterStateIdContext routerStateIdContext = new RouterStateIdContext(new Configuration());
    String namespaceId = "namespace1";
    routerStateIdContext.getNamespaceStateId(namespaceId).accumulate(20L);
    PoolAlignmentContext poolContext1 = new PoolAlignmentContext(routerStateIdContext, namespaceId);
    PoolAlignmentContext poolContext2 = new PoolAlignmentContext(routerStateIdContext, namespaceId);

    assertRequestHeaderStateId(poolContext1, Long.MIN_VALUE);
    assertRequestHeaderStateId(poolContext2, Long.MIN_VALUE);
    Assertions.assertEquals(20L, poolContext1.getLastSeenStateId());
    Assertions.assertEquals(20L, poolContext2.getLastSeenStateId());

    poolContext1.advanceClientStateId(30L);
    assertRequestHeaderStateId(poolContext1, 30L);
    assertRequestHeaderStateId(poolContext2, Long.MIN_VALUE);
    Assertions.assertEquals(20L, poolContext1.getLastSeenStateId());
    Assertions.assertEquals(20L, poolContext2.getLastSeenStateId());
  }

  private void assertRequestHeaderStateId(PoolAlignmentContext poolAlignmentContext,
      Long expectedValue) {
    RpcRequestHeaderProto.Builder builder = RpcRequestHeaderProto.newBuilder();
    poolAlignmentContext.updateRequestState(builder);
    Assertions.assertEquals(expectedValue, builder.getStateId());
  }
}
