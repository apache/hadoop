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

package org.apache.hadoop.hive.ql.exec;

import java.util.*;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * fixed width composition of Hive Objects
 */
public class CompositeHiveObject extends HiveObject {

  public static class CompositeSerDeField implements SerDeField {
    int position;
    String expr;
    protected SerDeField field;

    public CompositeSerDeField (int position, String expr) {
      this.position = position;
      this.expr = expr;
    }

    public HiveObject getHiveObject(Object obj) throws SerDeException {
      ArrayList<HiveObject>  hol = (ArrayList<HiveObject>)obj;
      HiveObject ho = hol.get(this.position);

      if(ho == null)
        return new NullHiveObject();
      
      if(this.expr == null) {
        // no need to descend any further
        return ho;
      }

      try {
        if (this.field == null) {
          this.field = ho.getFieldFromExpression(this.expr);
        }
        // descend recursively
        return (ho.get(this.field));
      } catch (HiveException e) {
        throw new SerDeException (e);
      }
    }

    public Object get(Object obj) throws SerDeException {
      throw new RuntimeException ("get() cannot be called for CompositeSerDeField");
    }
    public boolean isList() { throw new RuntimeException("Not supported"); }
    public boolean isMap() { throw new RuntimeException("Not supported"); }
    public boolean isPrimitive() { throw new RuntimeException("Not supported"); }
    public Class getType() { throw new RuntimeException("Not supported"); }
    public Class getListElementType() {throw new RuntimeException("Not supported"); }
    public Class getMapKeyType() { throw new RuntimeException("Not supported"); }
    public Class getMapValueType() { throw new RuntimeException("Not supported"); }
    public String getName() {  throw new RuntimeException("Not supported"); }
  }

  ArrayList<HiveObject> hol;
  SerDe csd;
  int width;

  public CompositeHiveObject(int width) {
    assert(width > 0);
    this.hol = new ArrayList<HiveObject> (width);
    this.width = width;
  }

  public CompositeHiveObject(ArrayList<HiveObject> hol) {
    this.width = hol.size();
    this.hol = hol;
  }
  
  @Override
  public SerDeField getFieldFromExpression(String compositeExpr) throws HiveException {
    int position = this.width+1;
    String expr = null;
    int dot = compositeExpr.indexOf(".");
    try {
      if(dot == -1) {
        position = Integer.parseInt(compositeExpr);
        expr = null;
      } else {
        position = Integer.parseInt(compositeExpr.substring(0, dot));
        expr = compositeExpr.substring(dot+1);
        if(expr.equals("")) { expr = null; }
      }
    } catch (NumberFormatException e) {
      throw new HiveException("Field Name is not a number: "+compositeExpr);
    }
    
    if((position >= this.width) || (position < 0)) {
      throw new HiveException("Index=" + position + " is not between 0 and (width=)" + this.width);
    }
    
    return new CompositeSerDeField(position, expr);
  }

  @Override
  public HiveObject get(SerDeField field) throws HiveException {
    try {
      CompositeSerDeField csdField = (CompositeSerDeField)field;
      return (csdField.getHiveObject(this.hol));
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public List<SerDeField> getFields() throws HiveException {
    ArrayList<SerDeField> ret = new ArrayList<SerDeField> (this.width);
    for(int i=0; i<this.width; i++) {
      ret.add(new CompositeSerDeField(i, null));
    }
    return (ret);
  }

  public static List<SerDeField> getFields(int width) throws HiveException {
    ArrayList<SerDeField> ret = new ArrayList<SerDeField> (width);
    for(int i=0; i<width; i++) {
      ret.add(new CompositeSerDeField(i, null));
    }
    return (ret);
  }

  @SuppressWarnings("nls")
  public void addHiveObject(HiveObject ho) throws HiveException {
    if(this.hol.size() >= this.width) {
      throw new HiveException("Exceeded max size of Composite Hive Object ="+this.width);
    }
    this.hol.add(ho);
  }

  @Override
  public Object getJavaObject() throws HiveException {
    ArrayList result = new ArrayList();
    for(HiveObject ho: hol) {
      result.add(ho == null ? null : ho.getJavaObject());
    }
    return result;
  }

  @Override
  public boolean isPrimitive() { return false; }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CompositeHiveObject)) return false;
    CompositeHiveObject choOther = (CompositeHiveObject) other;
    return this.hol.equals(choOther.hol);
  }

  @Override
  public int hashCode() {
    int ret = 12345;
    for(HiveObject ho: this.hol) {
      ret = ret * 31 + ho.hashCode();
    }
    return ret;
  }
}
