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
package org.apache.hadoop.hive.serde2.dynamic_type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol;
import org.apache.hadoop.hive.serde.Constants;

import junit.framework.TestCase;
import org.apache.hadoop.io.BytesWritable;

public class TestDynamicSerDe extends TestCase {

  public void testDynamicSerDe() throws Throwable {
    try {

      // Try to construct an object
      ArrayList<String> bye = new ArrayList<String>();
      bye.add("firstString");
      bye.add("secondString");
      HashMap<String, Integer> another = new HashMap<String, Integer>();  
      another.put("firstKey", 1);
      another.put("secondKey", 2);
      ArrayList<Object> struct = new ArrayList<Object>();
      struct.add(Integer.valueOf(234));
      struct.add(bye);
      struct.add(another);
      
      // All protocols
      ArrayList<String> protocols = new ArrayList<String>();
      ArrayList<Boolean> isBinaries = new ArrayList<Boolean>();
      
      protocols.add(com.facebook.thrift.protocol.TBinaryProtocol.class.getName());
      isBinaries.add(true);
      
      protocols.add(com.facebook.thrift.protocol.TJSONProtocol.class.getName());
      isBinaries.add(false);

      // TSimpleJSONProtocol does not support deserialization.
      // protocols.add(com.facebook.thrift.protocol.TSimpleJSONProtocol.class.getName());
      // isBinaries.add(false);
      
      // TCTLSeparatedProtocol is not done yet.
      protocols.add(org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      isBinaries.add(false);
      
      System.out.println("input struct = " + struct);
        
      for(int pp = 0; pp<protocols.size(); pp++) {
        
        String protocol = protocols.get(pp);
        boolean isBinary = isBinaries.get(pp);
        
        System.out.println("Testing protocol: " + protocol);
        Properties schema = new Properties();
        schema.setProperty(Constants.SERIALIZATION_FORMAT, protocol);
        schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
        schema.setProperty(Constants.SERIALIZATION_DDL,
            "struct test { i32 hello, list<string> bye, map<string,i32> another}");
        schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());
  
        DynamicSerDe serde = new DynamicSerDe();
        serde.initialize(new Configuration(), schema);
        
        // Try getObjectInspector
        ObjectInspector oi = serde.getObjectInspector();
        System.out.println("TypeName = " + oi.getTypeName());

        
        // Try to serialize
        BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<bytes.getSize(); i++) {
          byte b = bytes.get()[i];
          int v = (b<0 ? 256 + b : b);
          sb.append(String.format("x%02x", v));
        }
        System.out.println("bytes =" + sb);
        
        if (!isBinary) {
          System.out.println("bytes in text =" + new String(bytes.get(), 0, bytes.getSize()));
        }
        
        // Try to deserialize
        Object o = serde.deserialize(bytes);
        System.out.println("o class = " + o.getClass());
        List<?> olist = (List<?>)o;
        System.out.println("o size = " + olist.size());
        System.out.println("o[0] class = " + olist.get(0).getClass());
        System.out.println("o[1] class = " + olist.get(1).getClass());
        System.out.println("o[2] class = " + olist.get(2).getClass());
        System.out.println("o = " + o);
        
        assertEquals(o, struct);
      }
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  




  public void testConfigurableTCTLSeparated() throws Throwable {
    try {


      // Try to construct an object
      ArrayList<String> bye = new ArrayList<String>();
      bye.add("firstString");
      bye.add("secondString");
      HashMap<String, Integer> another = new HashMap<String, Integer>();  
      another.put("firstKey", 1);
      another.put("secondKey", 2);
      ArrayList<Object> struct = new ArrayList<Object>();
      struct.add(Integer.valueOf(234));
      struct.add(bye);
      struct.add(another);

      Properties schema = new Properties();
      schema.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
      schema.setProperty(Constants.SERIALIZATION_DDL,
                         "struct test { i32 hello, list<string> bye, map<string,i32> another}");
      schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());

      schema.setProperty(Constants.FIELD_DELIM, "9");
      schema.setProperty(Constants.COLLECTION_DELIM, "1");
      schema.setProperty(Constants.LINE_DELIM, "2");
      schema.setProperty(Constants.MAPKEY_DELIM, "4");

      DynamicSerDe serde = new DynamicSerDe();
      serde.initialize(new Configuration(), schema);

      TCTLSeparatedProtocol prot = (TCTLSeparatedProtocol)serde.oprot_;
      assertTrue(prot.getPrimarySeparator() == 9);
      
      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<bytes.getSize(); i++) {
        byte b = bytes.get()[i];
        int v = (b<0 ? 256 + b : b);
        sb.append(String.format("x%02x", v));
      }
      System.out.println("bytes =" + sb);

      String compare = "234" + "\u0009" + "firstString" + "\u0001" + "secondString" + "\u0009" + "firstKey" + "\u0004" + "1" + "\u0001" + "secondKey" + "\u0004" + "2";

      System.out.println("bytes in text =" + new String(bytes.get(), 0, bytes.getSize()) + ">");
      System.out.println("compare to    =" + compare + ">");
        
      assertTrue(compare.equals( new String(bytes.get(), 0, bytes.getSize())));
      
      // Try to deserialize
      Object o = serde.deserialize(bytes);
      System.out.println("o class = " + o.getClass());
      List<?> olist = (List<?>)o;
      System.out.println("o size = " + olist.size());
      System.out.println("o[0] class = " + olist.get(0).getClass());
      System.out.println("o[1] class = " + olist.get(1).getClass());
      System.out.println("o[2] class = " + olist.get(2).getClass());
      System.out.println("o = " + o);
        
      assertEquals(o, struct);
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  
}
