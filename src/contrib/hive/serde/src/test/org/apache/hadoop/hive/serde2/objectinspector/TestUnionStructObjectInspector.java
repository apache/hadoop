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
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import junit.framework.TestCase;

public class TestUnionStructObjectInspector extends TestCase {

  
  public void testUnionStructObjectInspector() throws Throwable {
    try {
      ArrayList<String> fieldNames1 = new ArrayList<String>();
      fieldNames1.add("firstInteger");
      fieldNames1.add("secondString");
      fieldNames1.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors1 = new ArrayList<ObjectInspector>();
      fieldObjectInspectors1.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Integer.class));
      fieldObjectInspectors1.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(String.class));
      fieldObjectInspectors1.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Boolean.class));
      StandardStructObjectInspector soi1 = ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames1, fieldObjectInspectors1);
  
      ArrayList<String> fieldNames2 = new ArrayList<String>();
      fieldNames2.add("fourthDate");
      fieldNames2.add("fifthLong");
      ArrayList<ObjectInspector> fieldObjectInspectors2 = new ArrayList<ObjectInspector>();
      fieldObjectInspectors2.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(java.sql.Date.class));
      fieldObjectInspectors2.add(ObjectInspectorFactory.getStandardPrimitiveObjectInspector(Long.class));
      StandardStructObjectInspector soi2 = ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames2, fieldObjectInspectors2);
      
      UnionStructObjectInspector usoi1 = ObjectInspectorFactory.getUnionStructObjectInspector(
          Arrays.asList(new StructObjectInspector[]{soi1, soi2}));
      UnionStructObjectInspector usoi2 = ObjectInspectorFactory.getUnionStructObjectInspector(
          Arrays.asList(new StructObjectInspector[]{soi1, soi2}));
  
      assertEquals(usoi1, usoi2);
      
      // metadata
      assertEquals(Category.STRUCT, usoi1.getCategory());
      List<? extends StructField> fields = usoi1.getAllStructFieldRefs();
      assertEquals(5, fields.size());
      for (int i=0; i<5; i++) {
        if (i<=2) {
          assertEquals(fieldNames1.get(i).toLowerCase(), fields.get(i).getFieldName());
          assertEquals(fieldObjectInspectors1.get(i), fields.get(i).getFieldObjectInspector());
        } else {
          assertEquals(fieldNames2.get(i-3).toLowerCase(), fields.get(i).getFieldName());
          assertEquals(fieldObjectInspectors2.get(i-3), fields.get(i).getFieldObjectInspector());
        }
      }
      assertEquals(fields.get(1), usoi1.getStructFieldRef("secondString"));
      assertEquals(fields.get(4), usoi1.getStructFieldRef("fifthLong"));
      
      // null
      for (int i=0; i<5; i++) {
        assertNull(usoi1.getStructFieldData(null, fields.get(i)));
      }
      
      // real struct
      ArrayList<Object> struct1 = new ArrayList<Object>(3);
      struct1.add(1);
      struct1.add("two");
      struct1.add(true);
      ArrayList<Object> struct2 = new ArrayList<Object>(2);
      struct2.add(java.sql.Date.valueOf("2008-08-08"));
      struct2.add(new Long(111));
      ArrayList<Object> struct = new ArrayList<Object>(2);
      struct.add(struct1);
      struct.add(struct2);
      ArrayList<Object> all = new ArrayList<Object>(5);
      all.addAll(struct1);
      all.addAll(struct2);
      
      for (int i=0; i<5; i++) {
        assertEquals(all.get(i), usoi1.getStructFieldData(struct, fields.get(i)));
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
  
}
