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

package org.apache.hadoop.hive.serde;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.hive.serde.thrift.*;
import org.apache.hadoop.hive.serde.test.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

public class TestThriftSerDe extends TestCase {

  ThriftSerDe serde;
  Configuration conf;
  Properties schema;
  ThriftTestObj testObj;



  public TestThriftSerDe() throws Exception {
    try {
      serde = new ThriftSerDe();
      conf = new Configuration();
      schema = new Properties();
      schema.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS, org.apache.hadoop.hive.serde.test.ThriftTestObj.class.getName());
      schema.setProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "com.facebook.thrift.protocol.TJSONProtocol");
      serde.initialize(conf, schema);
      testObj = new ThriftTestObj();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  BytesWritable buffer;
  final String output = "{\"-1\":{\"i32\":10},\"-2\":{\"str\":\"hello world!\"}}";

  public void testSerialize() throws Exception {
    try {
      testObj.field1 = 10;
      testObj.field2 = "hello world!";
      buffer = (BytesWritable)serde.serialize(testObj);
      byte temp[] = buffer.get();
      String tempStr = new String(temp, 0, buffer.getSize());
      assertTrue(tempStr.equals(output));
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void testDeSerialize() throws Exception {
    try {
      buffer = new BytesWritable(output.getBytes());
      ThriftTestObj obj = (ThriftTestObj)serde.deserialize(buffer);
      assertTrue(obj.field1 == 10);
      assertTrue(obj.field2.equals("hello world!"));
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void testGetFieldFromExpression() throws Exception {
    try {
      SerDeField f = serde.getFieldFromExpression(null, "field1");
      assertTrue(f.getName().equals("field1"));
      assertTrue(f.getType().getName().equals("int"));
      assertTrue(f.isPrimitive() == true);
      assertTrue(f.isList() == false);
      assertTrue(f.isMap() == false);

      f = serde.getFieldFromExpression(null, "field3");
      assertTrue(f.isList() == true);
      assertTrue(f.getType().getName().equals("java.util.List"));

      f = serde.getFieldFromExpression(f, "field0");
      assertTrue(f.getName().equals("field0"));
      assertTrue(f.getType().getName().equals("int"));

      try {
        f = serde.getFieldFromExpression(null, "fieldXXX");
        assertTrue(false);
      } catch(SerDeException e) {
        assertTrue(true);
      }

    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void testGetFields() throws Exception {
    try {
      List<SerDeField> fields = serde.getFields(null);
      assertTrue(fields.size() == 3);
      assertTrue(fields.get(0).getName().equals("field1"));
      assertTrue(fields.get(1).getName().equals("field2"));

      List<SerDeField> fields2 = serde.getFields(fields.get(2));
      assertTrue(fields2.size() == 1);
      assertTrue(fields2.get(0).getName().equals("field0"));
    } catch(Exception e) {
      e.printStackTrace();
    }
  }



}
