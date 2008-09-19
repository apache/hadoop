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

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * DefaultListObjectInspector works on list data that is stored as a Java List or Java Array object.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects, instead
 * of directly creating an instance of this class. 
 */
class StandardListObjectInspector implements ListObjectInspector {

  ObjectInspector listElementObjectInspector;
  
  /** Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   */
  protected StandardListObjectInspector(ObjectInspector listElementObjectInspector) {
    this.listElementObjectInspector = listElementObjectInspector;
  }

  public final Category getCategory() {
    return Category.LIST;
  }

  // without data
  public ObjectInspector getListElementObjectInspector() {
    return listElementObjectInspector;
  }
  
  // with data
  public Object getListElement(Object data, int index) {
    List<?> list = getList(data);
    if (list == null || index < 0 || index >= list.size()) {
      return null;
    }
    return list.get(index);
  }
  
  public int getListLength(Object data) {
    List<?> list = getList(data);
    if (list == null) return -1;
    return list.size();
  }
  
  public List<?> getList(Object data) {
    if (data == null) return null;
    if (data.getClass().isArray()) {
      data = java.util.Arrays.asList((Object[])data);
    }
    List<?> list = (List<?>) data;
    return list;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME 
        + "<" + listElementObjectInspector.getTypeName() + ">";
  }

}
