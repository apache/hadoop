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

package org.apache.hadoop.hive.ql.typeinfo;

import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

/** A List Type has homogeneous elements.  All elements of the List has
 *  the same TypeInfo which is returned by getListElementTypeInfo.
 *  
 *  Always use the TypeInfoFactory to create new TypeInfo objects, instead
 *  of directly creating an instance of this class. 
 */
public class ListTypeInfo extends TypeInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  TypeInfo listElementTypeInfo;
  
  /** For java serialization use only.
   */
  public ListTypeInfo() {}
  
  public String getTypeName() {
    return org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME 
    + "<" + listElementTypeInfo.getTypeName() + ">";
  }
  
  /** For java serialization use only.
   */
  public void setListElementTypeInfo(TypeInfo listElementTypeInfo) {
    this.listElementTypeInfo = listElementTypeInfo;
  }
  
  /** For TypeInfoFactory use only.
   */
  ListTypeInfo(TypeInfo elementTypeInfo) {
    this.listElementTypeInfo = elementTypeInfo;
  }

  public Category getCategory() {
    return Category.LIST;
  }  

  public TypeInfo getListElementTypeInfo() {
    return listElementTypeInfo;
  }

  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof TypeInfo)) {
      return false;
    }
    TypeInfo o = (TypeInfo) other;
    return o.getCategory().equals(getCategory())
        && o.getListElementTypeInfo().equals(getListElementTypeInfo());
  }
  
  public int hashCode() {
    return listElementTypeInfo.hashCode();
  }
  
}
