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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import junit.framework.TestCase;


public class TestReflectionObjectInspectors extends TestCase {

  public void testReflectionObjectInspectors() throws Throwable {
    try {
      ObjectInspector oi1 = ObjectInspectorFactory.getReflectionObjectInspector(
          MyStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      ObjectInspector oi2 = ObjectInspectorFactory.getReflectionObjectInspector(
          MyStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      assertEquals(oi1, oi2);
      
      // metadata
      assertEquals(Category.STRUCT, oi1.getCategory());
      StructObjectInspector soi = (StructObjectInspector)oi1;
      List<? extends StructField> fields = soi.getAllStructFieldRefs(); 
      assertEquals(6, fields.size());
      assertEquals(fields.get(2), soi.getStructFieldRef("myString"));
  
      // null
      for (int i=0; i<fields.size(); i++) {
        assertNull(soi.getStructFieldData(null, fields.get(i)));
      }
      assertNull(soi.getStructFieldsDataAsList(null));
      
      // non nulls
      MyStruct a = new MyStruct();
      a.myInt = 1;
      a.myInteger = 2;
      a.myString = "test";
      a.myStruct = a;
      a.myListString = Arrays.asList(new String[]{"a", "b", "c"});
      a.myMapStringString = new HashMap<String, String>();
      a.myMapStringString.put("key", "value");
      
      assertEquals(1, soi.getStructFieldData(a, fields.get(0)));
      assertEquals(2, soi.getStructFieldData(a, fields.get(1)));
      assertEquals("test", soi.getStructFieldData(a, fields.get(2)));
      assertEquals(a, soi.getStructFieldData(a, fields.get(3)));
      assertEquals(a.myListString, soi.getStructFieldData(a, fields.get(4)));
      assertEquals(a.myMapStringString, soi.getStructFieldData(a, fields.get(5)));
      ArrayList<Object> afields = new ArrayList<Object>();
      for(int i=0; i<6; i++) {
        afields.add(soi.getStructFieldData(a, fields.get(i)));
      }
      assertEquals(afields, soi.getStructFieldsDataAsList(a));
      
      // sub fields
      assertEquals(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class),
          fields.get(0).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class),
          fields.get(1).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class),
          fields.get(2).getFieldObjectInspector());
      assertEquals(soi, fields.get(3).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class)),
          fields.get(4).getFieldObjectInspector());
      assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class),
          ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class)),
          fields.get(5).getFieldObjectInspector());
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
