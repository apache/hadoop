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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.junit.Assert;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.junit.Test;

public class TestSerializedExceptionPBImpl {
  @Test
  public void testSerializedException() throws Exception {
    SerializedExceptionPBImpl orig = new SerializedExceptionPBImpl();
    orig.init(new Exception("test exception"));
    SerializedExceptionProto proto = orig.getProto();
    SerializedExceptionPBImpl deser = new SerializedExceptionPBImpl(proto);
    Assert.assertEquals(orig, deser);
    Assert.assertEquals(orig.getMessage(), deser.getMessage());
    Assert.assertEquals(orig.getRemoteTrace(), deser.getRemoteTrace());
    Assert.assertEquals(orig.getCause(), deser.getCause());
  }

  @Test
  public void testDeserialize() throws Exception {
    Exception ex = new Exception("test exception");
    SerializedExceptionPBImpl pb = new SerializedExceptionPBImpl();

    try {
      pb.deSerialize();
      Assert.fail("deSerialze should throw YarnRuntimeException");
    } catch (YarnRuntimeException e) {
      Assert.assertEquals(ClassNotFoundException.class,
          e.getCause().getClass());
    }

    pb.init(ex);
    Assert.assertEquals(ex.toString(), pb.deSerialize().toString());
  }

  @Test
  public void testBeforeInit() throws Exception {
    SerializedExceptionProto defaultProto =
        SerializedExceptionProto.newBuilder().build();

    SerializedExceptionPBImpl pb1 = new SerializedExceptionPBImpl();
    Assert.assertNull(pb1.getCause());

    SerializedExceptionPBImpl pb2 = new SerializedExceptionPBImpl();
    Assert.assertEquals(defaultProto, pb2.getProto());

    SerializedExceptionPBImpl pb3 = new SerializedExceptionPBImpl();
    Assert.assertEquals(defaultProto.getTrace(), pb3.getRemoteTrace());
  }
}
