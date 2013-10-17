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
package org.apache.hadoop.hdfs.protocolPB;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;

import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.EmptyPathError;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddPathBasedCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.PathBasedCacheDirectiveProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class TestClientNamenodeProtocolServerSideTranslatorPB {

  @Test
  public void testAddPathBasedCacheDirectiveEmptyPathError() throws Exception {
    ClientProtocol server = mock(ClientProtocol.class);
    RpcController controller = mock(RpcController.class);
    AddPathBasedCacheDirectiveRequestProto request = 
        AddPathBasedCacheDirectiveRequestProto.newBuilder().
            setDirective(PathBasedCacheDirectiveProto.newBuilder().
                setPath("").
                setPool("pool").
                setReplication(1).
                build()).
            build();
    ClientNamenodeProtocolServerSideTranslatorPB translator =
        new ClientNamenodeProtocolServerSideTranslatorPB(server);
    try {
      translator.addPathBasedCacheDirective(controller, request);
      fail("Expected ServiceException");
    } catch (ServiceException e) {
      assertNotNull(e.getCause());
      assertTrue(e.getCause() instanceof EmptyPathError);
    }
  }
}
