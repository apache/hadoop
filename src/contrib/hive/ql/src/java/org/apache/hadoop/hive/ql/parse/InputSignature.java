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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.lang.Class;
import java.lang.Object;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoFactory;

/**
 * The input signature of a function or operator. The signature basically consists
 * of name, list of parameter types.
 *
 **/

public class InputSignature {
  private String name;
  private ArrayList<TypeInfo> typeArray;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(InputSignature.class.getName());

  public InputSignature(String name) {
    this.name = name;
    typeArray = new ArrayList<TypeInfo>();
  }

  public InputSignature(String name, TypeInfo ... classList) {
    this(name);
    
    if (classList.length != 0) {
      for(TypeInfo cl: classList) {
        typeArray.add(cl);
      }
    }
  }

  public InputSignature(String name, Class<?> ... classList) {
    this(name);
    
    if (classList.length != 0) {
      for(Class<?> cl: classList) {
        typeArray.add(TypeInfoFactory.getPrimitiveTypeInfo(cl));
      }
    }
  }

  public void add(TypeInfo paramType) {
    typeArray.add(paramType);
  }

  public String getName() {
    return name.toUpperCase();
  }

  public ArrayList<TypeInfo> getTypeArray() {
    return typeArray;
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    InputSignature other = null;
    try {
      other = (InputSignature)obj;
    }
    catch (ClassCastException cce) {
      return false;
    }

    return name.equalsIgnoreCase(other.getName())
        && (other.typeArray.equals(typeArray));
  }

  public int hashCode() {
    return toString().hashCode();
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(getName());
    sb.append("(");
    boolean isfirst = true;
    for(TypeInfo cls: getTypeArray()) {
      if (!isfirst) {
        sb.append(",");
      }
      sb.append(cls.toString());
      isfirst = false;
    }

    sb.append(")");
    return sb.toString();
  }
}
