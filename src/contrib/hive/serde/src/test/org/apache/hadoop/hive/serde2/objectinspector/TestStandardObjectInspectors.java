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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import junit.framework.TestCase;

public class TestStandardObjectInspectors extends TestCase {

  
  void doTestStandardPrimitiveObjectInspector(Class<?> c) throws Throwable {
    try {
      StandardPrimitiveObjectInspector oi1 = ObjectInspectorFactory.getStandardPrimitiveObjectInspector(c);
      StandardPrimitiveObjectInspector oi2 = ObjectInspectorFactory.getStandardPrimitiveObjectInspector(c);
      assertEquals(oi1, oi2);
      assertEquals(Category.PRIMITIVE, oi1.getCategory());
      assertEquals(c, oi1.getPrimitiveClass());
      assertEquals(ObjectInspectorUtils.getClassShortName(c.getName()),
          oi1.getTypeName()); 
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
  
  public void testStandardPrimitiveObjectInspector() throws Throwable {
    try {
      doTestStandardPrimitiveObjectInspector(Boolean.class);
      doTestStandardPrimitiveObjectInspector(Byte.class);
      doTestStandardPrimitiveObjectInspector(Integer.class);
      doTestStandardPrimitiveObjectInspector(Long.class);
      doTestStandardPrimitiveObjectInspector(Float.class);
      doTestStandardPrimitiveObjectInspector(Double.class);
      doTestStandardPrimitiveObjectInspector(String.class);
      doTestStandardPrimitiveObjectInspector(java.sql.Date.class);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testStandardListObjectInspector() throws Throwable {
    try {
      StandardListObjectInspector loi1 = ObjectInspectorFactory.getStandardListObjectInspector(
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class));
      StandardListObjectInspector loi2 = ObjectInspectorFactory.getStandardListObjectInspector(
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class));
      assertEquals(loi1, loi2);
      
      // metadata
      assertEquals(Category.LIST, loi1.getCategory());
      assertEquals(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class),
          loi1.getListElementObjectInspector());
      
      // null
      assertNull("loi1.getList(null) should be null.", loi1.getList(null));
      assertEquals("loi1.getListLength(null) should be -1.", loi1.getListLength(null), -1);
      assertNull("loi1.getListElement(null, 0) should be null", loi1.getListElement(null, 0));
      assertNull("loi1.getListElement(null, 100) should be null", loi1.getListElement(null, 100));
      
      // ArrayList
      ArrayList<Integer> list = new ArrayList<Integer>();
      list.add(0);
      list.add(1);
      list.add(2);
      list.add(3);
      assertEquals(4, loi1.getList(list).size());
      assertEquals(4, loi1.getListLength(list));
      assertEquals(0, loi1.getListElement(list, 0));
      assertEquals(3, loi1.getListElement(list, 3));
      assertNull(loi1.getListElement(list, -1));
      assertNull(loi1.getListElement(list, 4));
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
      
  }

  public void testStandardMapObjectInspector() throws Throwable {
    try {
      StandardMapObjectInspector moi1 = ObjectInspectorFactory.getStandardMapObjectInspector(
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class),
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class)
          );
      StandardMapObjectInspector moi2 = ObjectInspectorFactory.getStandardMapObjectInspector(
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class),
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class)
          );
      assertEquals(moi1, moi2);
      
      // metadata
      assertEquals(Category.MAP, moi1.getCategory());
      assertEquals(moi1.getMapKeyObjectInspector(), 
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
      assertEquals(moi2.getMapValueObjectInspector(), 
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class));
      
      // null
      assertNull(moi1.getMap(null));
      assertNull(moi1.getMapValueElement(null, null));
      assertNull(moi1.getMapValueElement(null, "nokey"));
      assertEquals(-1, moi1.getMapSize(null));
      assertEquals("map<" 
          + ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class).getTypeName() + ","
          + ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class).getTypeName() + ">",
          moi1.getTypeName());
  
      // HashMap
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("one", 1);
      map.put("two", 2);
      map.put("three", 3);
      assertEquals(map, moi1.getMap(map));
      assertEquals(3, moi1.getMapSize(map));
      assertEquals(1, moi1.getMapValueElement(map, "one"));
      assertEquals(2, moi1.getMapValueElement(map, "two"));
      assertEquals(3, moi1.getMapValueElement(map, "three"));
      assertNull(moi1.getMapValueElement(map, null));
      assertNull(moi1.getMapValueElement(map, "null"));
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
      
  }

  
  @SuppressWarnings("unchecked")
  public void testStandardStructObjectInspector() throws Throwable {
    try {
      ArrayList<String> fieldNames = new ArrayList<String>();
      fieldNames.add("firstInteger");
      fieldNames.add("secondString");
      fieldNames.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
      fieldObjectInspectors.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class));
      fieldObjectInspectors.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
      fieldObjectInspectors.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Boolean.class));
      
      StandardStructObjectInspector soi1 = ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames, fieldObjectInspectors);
      StandardStructObjectInspector soi2 = ObjectInspectorFactory.getStandardStructObjectInspector(
          (ArrayList<String>)fieldNames.clone(), (ArrayList<ObjectInspector>)fieldObjectInspectors.clone());
      assertEquals(soi1, soi2);
      
      // metadata
      assertEquals(Category.STRUCT, soi1.getCategory());
      List<? extends StructField> fields = soi1.getAllStructFieldRefs();
      assertEquals(3, fields.size());
      for (int i=0; i<3; i++) {
        assertEquals(fieldNames.get(i).toLowerCase(), fields.get(i).getFieldName());
        assertEquals(fieldObjectInspectors.get(i), fields.get(i).getFieldObjectInspector());
      }
      assertEquals(fields.get(1), soi1.getStructFieldRef("secondString"));
      StringBuilder structTypeName = new StringBuilder(); 
      structTypeName.append("struct{");
      for(int i=0; i<fields.size(); i++) {
        if (i>0) structTypeName.append(",");
        structTypeName.append(fields.get(i).getFieldName());
        structTypeName.append(":");
        structTypeName.append(fields.get(i).getFieldObjectInspector().getTypeName());
      }
      structTypeName.append("}");
      assertEquals(structTypeName.toString(), soi1.getTypeName());
  
      // null
      assertNull(soi1.getStructFieldData(null, fields.get(0)));
      assertNull(soi1.getStructFieldData(null, fields.get(1)));
      assertNull(soi1.getStructFieldData(null, fields.get(2)));
      assertNull(soi1.getStructFieldsDataAsList(null));
  
      // HashStruct
      ArrayList<Object> struct = new ArrayList<Object>(3);
      struct.add(1);
      struct.add("two");
      struct.add(true);
      
      assertEquals(1, soi1.getStructFieldData(struct, fields.get(0)));
      assertEquals("two", soi1.getStructFieldData(struct, fields.get(1)));
      assertEquals(true, soi1.getStructFieldData(struct, fields.get(2)));
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
      
  }
  
}
