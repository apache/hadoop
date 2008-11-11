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
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol;
import org.apache.hadoop.hive.serde.Constants;

import junit.framework.TestCase;
import org.apache.hadoop.io.BytesWritable;

public class TestDynamicSerDe extends TestCase {

  public static HashMap<String,String> makeHashMap(String... params) {
    HashMap<String,String> r = new HashMap<String,String>(); 
    for(int i=0; i<params.length; i+=2) {
      r.put(params[i], params[i+1]);
    }
    return r;
  }
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
      struct.add(Integer.valueOf(-234));
      struct.add(Double.valueOf(1.0));
      struct.add(Double.valueOf(-2.5));
      
      
      // All protocols
      ArrayList<String> protocols = new ArrayList<String>();
      ArrayList<Boolean> isBinaries = new ArrayList<Boolean>();
      ArrayList<HashMap<String,String>> additionalParams = new ArrayList<HashMap<String,String>>();

      protocols.add(org.apache.hadoop.hive.serde2.thrift.TBinarySortableProtocol.class.getName());
      isBinaries.add(true);
      additionalParams.add(makeHashMap("serialization.sort.order", "++++++"));
      protocols.add(org.apache.hadoop.hive.serde2.thrift.TBinarySortableProtocol.class.getName());
      isBinaries.add(true);
      additionalParams.add(makeHashMap("serialization.sort.order", "------"));


      protocols.add(com.facebook.thrift.protocol.TBinaryProtocol.class.getName());
      isBinaries.add(true);
      additionalParams.add(null);
      
      protocols.add(com.facebook.thrift.protocol.TJSONProtocol.class.getName());
      isBinaries.add(false);
      additionalParams.add(null);

      // TSimpleJSONProtocol does not support deserialization.
      // protocols.add(com.facebook.thrift.protocol.TSimpleJSONProtocol.class.getName());
      // isBinaries.add(false);
      // additionalParams.add(null);
      
      // TCTLSeparatedProtocol is not done yet.
      protocols.add(org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      isBinaries.add(false);
      additionalParams.add(null);
      
      System.out.println("input struct = " + struct);
        
      for(int pp = 0; pp<protocols.size(); pp++) {
        
        String protocol = protocols.get(pp);
        boolean isBinary = isBinaries.get(pp);
        
        System.out.println("Testing protocol: " + protocol);
        Properties schema = new Properties();
        schema.setProperty(Constants.SERIALIZATION_FORMAT, protocol);
        schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
        schema.setProperty(Constants.SERIALIZATION_DDL,
        "struct test { i32 hello, list<string> bye, map<string,i32> another, i32 nhello, double d, double nd}");
        schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());
        HashMap<String, String> p = additionalParams.get(pp);
        if (p != null) {
          for(Entry<String, String> e: p.entrySet()) {
            schema.setProperty(e.getKey(), e.getValue());
          }
        }
  
        DynamicSerDe serde = new DynamicSerDe();
        serde.initialize(new Configuration(), schema);
        
        // Try getObjectInspector
        ObjectInspector oi = serde.getObjectInspector();
        System.out.println("TypeName = " + oi.getTypeName());

        
        // Try to serialize
        BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        System.out.println("bytes =" + hexString(bytes)); 
       
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
        
        assertEquals(struct, o);
      }
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }
  public String hexString(BytesWritable bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<bytes.getSize(); i++) {
      byte b = bytes.get()[i];
      int v = (b<0 ? 256 + b : b);
      sb.append(String.format("x%02x", v));
    }
    return sb.toString();
  }  


  private void testTBinarySortableProtocol(Object[] structs, String ddl, boolean ascending) throws Throwable{
    int fields = ((List)structs[structs.length-1]).size();
    String order = "";
    for(int i=0; i<fields; i++) {
      order = order + (ascending ? "+" : "-"); 
    }
    
    Properties schema = new Properties();
    schema.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TBinarySortableProtocol.class.getName());
    schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
    schema.setProperty(Constants.SERIALIZATION_DDL, ddl);
    schema.setProperty(Constants.SERIALIZATION_LIB, DynamicSerDe.class.getName());
    schema.setProperty(Constants.SERIALIZATION_SORT_ORDER, order);

    DynamicSerDe serde = new DynamicSerDe();
    serde.initialize(new Configuration(), schema);

    ObjectInspector oi = serde.getObjectInspector();

    // Try to serialize
    BytesWritable bytes[] = new BytesWritable[structs.length];
    for (int i=0; i<structs.length; i++) {
      bytes[i] = new BytesWritable();
      BytesWritable s = (BytesWritable)serde.serialize(structs[i], oi);
      bytes[i].set(s);
      if (i>0) {
        int compareResult = bytes[i-1].compareTo(bytes[i]);
        if ( (compareResult<0 && !ascending) || (compareResult>0 && ascending) ) {
          System.out.println("Test failed in " + (ascending ? "ascending" : "descending") + " order.");
          System.out.println("serialized data of " + structs[i-1] + " = " + hexString(bytes[i-1]));
          System.out.println("serialized data of " + structs[i] + " = " + hexString(bytes[i]));
          fail("Sort order of serialized " + structs[i-1] + " and " + structs[i] + " are reversed!");            
        }
      }
    }

    // Try to deserialize
    Object[] deserialized = new Object[structs.length];
    for (int i=0; i<structs.length; i++) {
      deserialized[i] = serde.deserialize(bytes[i]);
      if (!structs[i].equals(deserialized[i])) {
        System.out.println("structs[i] = " + structs[i]);
        System.out.println("deserialized[i] = " + deserialized[i]);
        System.out.println("serialized[i] = " + hexString(bytes[i]));
        assertEquals(structs[i], deserialized[i]);
      }
    }
  }
  
  static int compare(Object a, Object b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    if (a instanceof List) {
      List la = (List)a;
      List lb = (List)b;
      assert(la.size() == lb.size());
      for (int i=0; i<la.size(); i++) {
        int r = compare(la.get(i), lb.get(i));
        if (r != 0) return r;
      }
      return 0;
    } else if (a instanceof Number) {
      Number na = (Number) a;
      Number nb = (Number) b;
      if (na.doubleValue() < nb.doubleValue()) return -1;
      if (na.doubleValue() > nb.doubleValue()) return 1;
      return 0;      
    } else if (a instanceof String) {
      String sa = (String) a;
      String sb = (String) b;
      return sa.compareTo(sb);
    } 
    return 0;
  }
  
  private void sort(Object[] structs) {
    for (int i=0; i<structs.length; i++) for (int j=i+1; j<structs.length; j++)
      if (compare(structs[i], structs[j])>0) {
        Object t = structs[i];
        structs[i] = structs[j];
        structs[j] = t;
      }
  }  

  public void testTBinarySortableProtocol() throws Throwable {
    try {

      System.out.println("Beginning Test testTBinarySortableProtocol:");
      
      int num = 100;
      Random r = new Random(1234);
      Object structs[] = new Object[num];
      String ddl;
      
      // Test double
      for (int i=0; i<num; i++) {
        ArrayList<Object> struct = new ArrayList<Object>();
        if (i==0) {
          struct.add(null);
        } else {
          struct.add(Double.valueOf((r.nextDouble()-0.5)*10));
        }
        structs[i] = struct;
      }
      sort(structs);
      ddl = "struct test { double hello}";
      System.out.println("Testing " + ddl);
      testTBinarySortableProtocol(structs, ddl, true);
      testTBinarySortableProtocol(structs, ddl, false);

      // Test integer
      for (int i=0; i<num; i++) {
        ArrayList<Object> struct = new ArrayList<Object>();
        if (i==0) {
          struct.add(null);
        } else {
          struct.add((int)((r.nextDouble()-0.5)*1.5*Integer.MAX_VALUE));
        }
        structs[i] = struct;
      }
      sort(structs);
      // Null should be smaller than any other value, so put a null at the front end
      // to test whether that is held.
      ((List)structs[0]).set(0, null);
      ddl = "struct test { i32 hello}";
      System.out.println("Testing " + ddl);
      testTBinarySortableProtocol(structs, ddl, true);
      testTBinarySortableProtocol(structs, ddl, false);

      // Test long
      for (int i=0; i<num; i++) {
        ArrayList<Object> struct = new ArrayList<Object>();
        if (i==0) {
          struct.add(null);
        } else {
          struct.add((long)((r.nextDouble()-0.5)*1.5*Long.MAX_VALUE));
        }
        structs[i] = struct;
      }
      sort(structs);
      // Null should be smaller than any other value, so put a null at the front end
      // to test whether that is held.
      ((List)structs[0]).set(0, null);
      ddl = "struct test { i64 hello}";
      System.out.println("Testing " + ddl);
      testTBinarySortableProtocol(structs, ddl, true);
      testTBinarySortableProtocol(structs, ddl, false);

      // Test string
      for (int i=0; i<num; i++) {
        ArrayList<Object> struct = new ArrayList<Object>();
        if (i==0) {
          struct.add(null);
        } else {
          struct.add(String.valueOf((r.nextDouble()-0.5)*1000));
        }
        structs[i] = struct;
      }
      sort(structs);
      // Null should be smaller than any other value, so put a null at the front end
      // to test whether that is held.
      ((List)structs[0]).set(0, null);
      ddl = "struct test { string hello}";
      System.out.println("Testing " + ddl);
      testTBinarySortableProtocol(structs, ddl, true);
      testTBinarySortableProtocol(structs, ddl, false);

      // Test string + double
      for (int i=0; i<num; i++) {
        ArrayList<Object> struct = new ArrayList<Object>();
        if (i%9==0) {
          struct.add(null);
        } else {
          struct.add("str" + (i/5));
        }
        if (i%7==0) {
          struct.add(null);
        } else {
          struct.add(Double.valueOf((r.nextDouble()-0.5)*10));
        }
        structs[i] = struct;
      }
      sort(structs);
      // Null should be smaller than any other value, so put a null at the front end
      // to test whether that is held.
      ((List)structs[0]).set(0, null);
      ddl = "struct test { string hello, double another}";
      System.out.println("Testing " + ddl);
      testTBinarySortableProtocol(structs, ddl, true);
      testTBinarySortableProtocol(structs, ddl, false);
      
      System.out.println("Test testTBinarySortableProtocol passed!");
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
      assertTrue(prot.getPrimarySeparator().equals("\u0009"));
      
      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      hexString(bytes);

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


  /**
   * Tests a single null list within a struct with return nulls on
   */

  public void testNulls1() throws Throwable {
    try {


      // Try to construct an object
      ArrayList<String> bye = null;
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
      schema.setProperty(TCTLSeparatedProtocol.ReturnNullsKey, "true");

      DynamicSerDe serde = new DynamicSerDe();
      serde.initialize(new Configuration(), schema);

      ObjectInspector oi = serde.getObjectInspector();
      
      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      hexString(bytes);

      // Try to deserialize
      Object o = serde.deserialize(bytes);
      assertEquals(struct, o);
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  

  /**
   * Tests all elements of a struct being null with return nulls on
   */

  public void testNulls2() throws Throwable {
    try {


      // Try to construct an object
      ArrayList<String> bye = null;
      HashMap<String, Integer> another = null;
      ArrayList<Object> struct = new ArrayList<Object>();
      struct.add(null);
      struct.add(bye);
      struct.add(another);

      Properties schema = new Properties();
      schema.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
      schema.setProperty(Constants.SERIALIZATION_DDL,
                         "struct test { i32 hello, list<string> bye, map<string,i32> another}");
      schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());
      schema.setProperty(TCTLSeparatedProtocol.ReturnNullsKey, "true");

      DynamicSerDe serde = new DynamicSerDe();
      serde.initialize(new Configuration(), schema);

      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      hexString(bytes);

      // Try to deserialize
      Object o = serde.deserialize(bytes);
      List<?> olist = (List<?>)o;

      assertTrue(olist.size() == 3);
      assertEquals(null, olist.get(0));
      assertEquals(null, olist.get(1));
      assertEquals(null, olist.get(2));
      
      //      assertEquals(o, struct); Cannot do this because types of null lists are wrong.
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  

  /**
   * Tests map and list being empty with return nulls on
   */

  public void testNulls3() throws Throwable {
    try {


      // Try to construct an object
      ArrayList<String> bye = new ArrayList<String> ();
      HashMap<String, Integer> another = null;
      ArrayList<Object> struct = new ArrayList<Object>();
      struct.add(null);
      struct.add(bye);
      struct.add(another);

      Properties schema = new Properties();
      schema.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
      schema.setProperty(Constants.SERIALIZATION_DDL,
                         "struct test { i32 hello, list<string> bye, map<string,i32> another}");
      schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());

      schema.setProperty(TCTLSeparatedProtocol.ReturnNullsKey, "true");
      DynamicSerDe serde = new DynamicSerDe();
      serde.initialize(new Configuration(), schema);

      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      hexString(bytes);

      // Try to deserialize
      Object o = serde.deserialize(bytes);
      List<?> olist = (List<?>)o;

      assertTrue(olist.size() == 3);
      assertEquals(null, olist.get(0));
      assertEquals(0, ((List<?>)olist.get(1)).size());
      assertEquals(null, olist.get(2));
      
      //      assertEquals(o, struct); Cannot do this because types of null lists are wrong.
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  


  /**
   * Tests map and list null/empty with return nulls *off*
   */

  public void testNulls4() throws Throwable {
    try {


      // Try to construct an object
      ArrayList<String> bye = new ArrayList<String> ();
      HashMap<String, Integer> another = null;
      ArrayList<Object> struct = new ArrayList<Object>();
      struct.add(null);
      struct.add(bye);
      struct.add(another);

      Properties schema = new Properties();
      schema.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
      schema.setProperty(Constants.SERIALIZATION_DDL,
                         "struct test { i32 hello, list<string> bye, map<string,i32> another}");
      schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());

      schema.setProperty(TCTLSeparatedProtocol.ReturnNullsKey, "false");
      DynamicSerDe serde = new DynamicSerDe();
      serde.initialize(new Configuration(), schema);

      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      hexString(bytes);

      // Try to deserialize
      Object o = serde.deserialize(bytes);
      List<?> olist = (List<?>)o;

      assertTrue(olist.size() == 3);
      assertEquals(new Integer(0), (Integer)olist.get(0));
      List<?> num1 = (List<?>)olist.get(1);
      assertTrue(num1.size() == 0);
      Map<?,?> num2 = (Map<?,?>)olist.get(2);
      assertTrue(num2.size() == 0);
      
      //      assertEquals(o, struct); Cannot do this because types of null lists are wrong.
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  


  /**
   * Tests map and list null/empty with return nulls *off*
   */

  public void testStructsinStructs() throws Throwable {
    try {


      Properties schema = new Properties();
      //      schema.setProperty(Constants.SERIALIZATION_FORMAT, com.facebook.thrift.protocol.TJSONProtocol.class.getName());
      schema.setProperty(Constants.SERIALIZATION_FORMAT, com.facebook.thrift.protocol.TBinaryProtocol.class.getName());
      schema.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "test");
      schema.setProperty(Constants.SERIALIZATION_DDL,
                         "struct inner { i32 field1, string field2 },struct  test {inner foo,  i32 hello, list<string> bye, map<string,i32> another}");
      schema.setProperty(Constants.SERIALIZATION_LIB, new DynamicSerDe().getClass().toString());

      
      //
      // construct object of above type
      //

      // construct the inner struct
      ArrayList<Object> innerStruct = new ArrayList<Object>();
      innerStruct.add(new Integer(22));
      innerStruct.add(new String("hello world"));

      // construct outer struct
      ArrayList<String> bye = new ArrayList<String> ();
      bye.add("firstString");
      bye.add("secondString");
      HashMap<String, Integer> another = new HashMap<String, Integer>();  
      another.put("firstKey", 1);
      another.put("secondKey", 2);

      ArrayList<Object> struct = new ArrayList<Object>();

      struct.add(innerStruct);
      struct.add(Integer.valueOf(234));
      struct.add(bye);
      struct.add(another);

      DynamicSerDe serde = new DynamicSerDe();
      serde.initialize(new Configuration(), schema);

      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);

      // Try to deserialize
      Object o = serde.deserialize(bytes);
      List<?> olist = (List<?>)o;


      assertEquals(4, olist.size());
      assertEquals(innerStruct, olist.get(0));
      assertEquals(new Integer(234), olist.get(1));
      assertEquals(bye, olist.get(2));
      assertEquals(another, olist.get(3));

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  




  public void testSkip() throws Throwable {
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
      assertTrue(prot.getPrimarySeparator().equals("\u0009"));
      
      ObjectInspector oi = serde.getObjectInspector();

      // Try to serialize
      BytesWritable bytes = (BytesWritable) serde.serialize(struct, oi);
        
      hexString(bytes);

      String compare = "234" + "\u0009" + "firstString" + "\u0001" + "secondString" + "\u0009" + "firstKey" + "\u0004" + "1" + "\u0001" + "secondKey" + "\u0004" + "2";

      System.out.println("bytes in text =" + new String(bytes.get(), 0, bytes.getSize()) + ">");
      System.out.println("compare to    =" + compare + ">");
        
      assertTrue(compare.equals( new String(bytes.get(), 0, bytes.getSize())));
      
      schema.setProperty(Constants.SERIALIZATION_DDL,
                         "struct test { i32 hello, skip list<string> bye, map<string,i32> another}");

      serde.initialize(new Configuration(), schema);

      // Try to deserialize
      Object o = serde.deserialize(bytes);
      System.out.println("o class = " + o.getClass());
      List<?> olist = (List<?>)o;
      System.out.println("o size = " + olist.size());
      System.out.println("o = " + o);
        
      assertEquals(null, olist.get(1));

      // set the skipped field to null
      struct.set(1,null);

      assertEquals(o, struct);
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
    
  }  

}
