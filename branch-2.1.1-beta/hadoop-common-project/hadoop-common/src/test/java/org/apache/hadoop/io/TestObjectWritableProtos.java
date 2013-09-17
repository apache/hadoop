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
package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Message;

/**
 * Test case for the use of Protocol Buffers within ObjectWritable.
 */
public class TestObjectWritableProtos {

  @Test
  public void testProtoBufs() throws IOException {
    doTest(1);
  }

  @Test
  public void testProtoBufs2() throws IOException {
    doTest(2);
  }
  
  @Test
  public void testProtoBufs3() throws IOException {
    doTest(3);
  }
  
  /**
   * Write a protobuf to a buffer 'numProtos' times, and then
   * read them back, making sure all data comes through correctly.
   */
  private void doTest(int numProtos) throws IOException {
    Configuration conf = new Configuration();
    DataOutputBuffer out = new DataOutputBuffer();

    // Write numProtos protobufs to the buffer
    Message[] sent = new Message[numProtos];
    for (int i = 0; i < numProtos; i++) {
      // Construct a test protocol buffer using one of the
      // protos that ships with the protobuf library
      Message testProto = DescriptorProtos.EnumValueDescriptorProto.newBuilder()
        .setName("test" + i).setNumber(i).build();
      ObjectWritable.writeObject(out, testProto,
          DescriptorProtos.EnumValueDescriptorProto.class, conf);
      sent[i] = testProto;
    }

    // Read back the data
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    
    for (int i = 0; i < numProtos; i++) {
      Message received = (Message)ObjectWritable.readObject(in, conf);
      
      assertEquals(sent[i], received);
    }
  }

}
