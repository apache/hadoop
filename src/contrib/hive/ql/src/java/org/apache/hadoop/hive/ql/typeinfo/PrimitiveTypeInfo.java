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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;


/** There are limited number of Primitive Types.
 *  All Primitive Types are defined by TypeInfoFactory.isPrimitiveClass().
 *  
 *  Always use the TypeInfoFactory to create new TypeInfo objects, instead
 *  of directly creating an instance of this class. 
 */
public class PrimitiveTypeInfo extends TypeInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  
  Class primitiveClass;
  
  /** For java serialization use only.
   */
  public PrimitiveTypeInfo() {}

  public String getTypeName() {
    return ObjectInspectorUtils.getClassShortName(primitiveClass.getName());
  }
  
  
  /** For java serialization use only.
   */
  public void setPrimitiveClass(Class primitiveClass) {
    this.primitiveClass = primitiveClass;
  }
  
  /** For TypeInfoFactory use only.
   */
  PrimitiveTypeInfo(Class primitiveClass) {
    this.primitiveClass = primitiveClass;
  }
  
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  public Class getPrimitiveClass() {
    return primitiveClass;
  }

  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof TypeInfo)) {
      return false;
    }
    TypeInfo o = (TypeInfo) other;
    return o.getCategory().equals(getCategory())
        && o.getPrimitiveClass().equals(getPrimitiveClass());
  }
  
  public int hashCode() {
    return primitiveClass.hashCode();
  }
  
}
