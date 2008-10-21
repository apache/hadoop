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

package org.apache.hadoop.hive.serde2;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.thrift.*;
import java.util.*;
import com.facebook.thrift.TException;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.*;
import com.facebook.thrift.protocol.*;

public class TestTCTLSeparatedProtocol extends TestCase {

  public TestTCTLSeparatedProtocol() throws Exception {
  }

  public void testReads() throws Exception {
    try {
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    String foo = "Hello";
    String bar = "World!";

    String key = "22";
    String  value = "TheValue";
    String key2 = "24";
    String  value2 = "TheValueAgain";

    byte columnSeparator [] = { 1 };
    byte elementSeparator [] = { 2 };
    byte kvSeparator [] = { 3 };


    trans.write(foo.getBytes(), 0, foo.getBytes().length);
    trans.write(columnSeparator, 0, 1);

    trans.write(columnSeparator, 0, 1);

    trans.write(bar.getBytes(), 0, bar.getBytes().length);
    trans.write(columnSeparator, 0, 1);

    trans.write(key.getBytes(), 0, key.getBytes().length); 
    trans.write(kvSeparator, 0, 1);
    trans.write(value.getBytes(), 0, value.getBytes().length);
    trans.write(elementSeparator, 0, 1);

    trans.write(key2.getBytes(), 0, key2.getBytes().length); 
    trans.write(kvSeparator, 0, 1);
    trans.write(value2.getBytes(), 0, value2.getBytes().length);
    

    trans.flush();


    // use 3 as the row buffer size to force lots of re-buffering.
    TCTLSeparatedProtocol prot = new TCTLSeparatedProtocol(trans, 3);

    prot.readStructBegin();

    prot.readFieldBegin();
    String hello = prot.readString();
    prot.readFieldEnd();

    assertTrue(hello.equals(foo));

    prot.readFieldBegin();
    assertTrue(prot.readString().equals(""));
    prot.readFieldEnd();

    prot.readFieldBegin();
    assertTrue(prot.readString().equals(bar));
    prot.readFieldEnd();

    prot.readFieldBegin();
    TMap mapHeader = prot.readMapBegin();
    assertTrue(mapHeader.size == 2);

    assertTrue(prot.readI32() == 22);
    assertTrue(prot.readString().equals(value));
    assertTrue(prot.readI32() == 24);
    assertTrue(prot.readString().equals(value2));
    prot.readMapEnd();
    prot.readFieldEnd();

    prot.readFieldBegin();
    hello = prot.readString();
    prot.readFieldEnd();
    assertTrue(hello.length() == 0);

    prot.readStructEnd();

    } catch(Exception e) {
      e.printStackTrace();
    }
  }


  public void testWrites() throws Exception {
    try {
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    TCTLSeparatedProtocol prot = new TCTLSeparatedProtocol(trans, 3);

    prot.writeStructBegin(new TStruct());
    prot.writeFieldBegin(new TField());
    prot.writeI32(100);
    prot.writeFieldEnd();

    prot.writeFieldBegin(new TField());
    prot.writeListBegin(new TList());
    prot.writeDouble(348.55);
    prot.writeDouble(234.22);
    prot.writeListEnd();
    prot.writeFieldEnd();

    prot.writeFieldBegin(new TField());
    prot.writeString("hello world!");
    prot.writeFieldEnd();

    prot.writeFieldBegin(new TField());
    prot.writeMapBegin(new TMap());
    prot.writeString("key1");
    prot.writeString("val1");
    prot.writeString("key2");
    prot.writeString("val2");
    prot.writeString("key3");
    prot.writeString("val3");
    prot.writeMapEnd();
    prot.writeFieldEnd();

    prot.writeFieldBegin(new TField());
    prot.writeListBegin(new TList());
    prot.writeString("elem1");
    prot.writeString("elem2");
    prot.writeListEnd();
    prot.writeFieldEnd();

    
    prot.writeFieldBegin(new TField());
    prot.writeString("bye!");
    prot.writeFieldEnd();

    prot.writeStructEnd();
    trans.flush();
    byte b[] = new byte[3*1024];
    int len = trans.read(b,0,b.length);
    String test = new String(b, 0, len);

    String testRef = "100348.55234.22hello world!key1val1key2val2key3val3elem1elem2bye!";
    //    System.err.println("test=" + test + ">");
    //    System.err.println(" ref=" + testRef + ">");
    assertTrue(test.equals(testRef));

    trans = new TMemoryBuffer(1023);
    trans.write(b, 0, len);

    //
    // read back!
    //

    prot = new TCTLSeparatedProtocol(trans, 10);

    // 100 is the start
    prot.readStructBegin();
    prot.readFieldBegin();
    assertTrue(prot.readI32() == 100);
    prot.readFieldEnd();

    // let's see if doubles work ok
    prot.readFieldBegin();
    TList l = prot.readListBegin();
    assertTrue(l.size == 2);
    assertTrue(prot.readDouble() == 348.55);
    assertTrue(prot.readDouble() == 234.22);
    prot.readListEnd();
    prot.readFieldEnd();

    // nice message
    prot.readFieldBegin();
    assertTrue(prot.readString().equals("hello world!"));
    prot.readFieldEnd();

    // 3 element map
    prot.readFieldBegin();
    TMap m = prot.readMapBegin();
    assertTrue(m.size == 3);
    assertTrue(prot.readString().equals("key1"));
    assertTrue(prot.readString().equals("val1"));
    assertTrue(prot.readString().equals("key2"));
    assertTrue(prot.readString().equals("val2"));
    assertTrue(prot.readString().equals("key3"));
    assertTrue(prot.readString().equals("val3"));
    prot.readMapEnd();
    prot.readFieldEnd();

    // the 2 element list
    prot.readFieldBegin();
    l = prot.readListBegin();
    assertTrue(l.size == 2);
    assertTrue(prot.readString().equals("elem1"));
    assertTrue(prot.readString().equals("elem2"));
    prot.readListEnd();
    prot.readFieldEnd();

    // final string
    prot.readFieldBegin();
    assertTrue(prot.readString().equals("bye!"));
    prot.readFieldEnd();

    // shouldl return nulls at end
    prot.readFieldBegin();
    assertTrue(prot.readString().equals(""));
    prot.readFieldEnd();

    // shouldl return nulls at end
    prot.readFieldBegin();
    assertTrue(prot.readString().equals(""));
    prot.readFieldEnd();

    prot.readStructEnd();


    } catch(Exception e) {
      e.printStackTrace();
    }
  }

}
