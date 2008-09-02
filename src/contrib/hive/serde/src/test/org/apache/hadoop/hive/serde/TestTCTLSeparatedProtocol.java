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

package org.apache.hadoop.hive.serde.thrift;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde.thrift.*;
import java.util.*;
import com.facebook.thrift.TException;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.*;
import com.facebook.thrift.protocol.*;

public class TestTCTLSeparatedProtocol extends TestCase {

  public TestTCTLSeparatedProtocol() throws Exception {
  }

  public void testLookupSerDe() throws Exception {
    try {
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    String foo = "Hello";
    String bar = "World!";

    byte separator [] = { TCTLSeparatedProtocol.defaultPrimarySeparatorChar_ };


    trans.write(foo.getBytes(), 0, foo.getBytes().length);
    trans.write(separator, 0, 1);
    trans.write(separator, 0, 1);
    trans.write(bar.getBytes(), 0, bar.getBytes().length);
    trans.flush();

    // use 3 as the row buffer size to force lots of re-buffering.
    TCTLSeparatedProtocol prot = new TCTLSeparatedProtocol(trans, 3);

    prot.readStructBegin();

    prot.readFieldBegin();
    String hello = prot.readString();
    prot.readFieldEnd();

    assertTrue(hello.equals(foo));

    prot.readFieldBegin();
    hello = prot.readString();
    prot.readFieldEnd();

    assertTrue(hello.equals(""));

    prot.readFieldBegin();
     hello = prot.readString();
    prot.readFieldEnd();

    assertTrue(hello.equals(bar));

    prot.readFieldBegin();
    hello = prot.readString();
    prot.readFieldEnd();

    assertTrue(hello.length() == 0);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
