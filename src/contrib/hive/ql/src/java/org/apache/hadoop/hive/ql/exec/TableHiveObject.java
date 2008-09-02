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
 * A TableHiveObject is Hive encapsulation of Objects returned from a table
 * It allows navigation using the SerDe associated with the Table. They also
 * encapsulate the position relative to the row object that the current object
 * represents.
 */
public class TableHiveObject extends HiveObject {

  // References to the table and initial table Object
  protected SerDe tableSerDe;
  protected Object rowObject;
  protected ArrayList<String> partCols;
  protected ArrayList<SerDeField> partFields;

  // References to the field of the row that the HiveObject refers to
  protected SerDeField myField;

  public TableHiveObject(Object javaObject, SerDe tableSerDe) {
      if(javaObject == null) {
          throw new RuntimeException("javaObject may not be null in TableHiveObject constructor");
      }


    this.javaObject = javaObject;
    this.tableSerDe = tableSerDe;
    this.rowObject = javaObject;
    this.partCols = null;
    this.partFields = null;
  }


  public TableHiveObject(Object javaObject, SerDe tableSerDe,
                         ArrayList<String> partCols,
                         ArrayList<SerDeField> partFields) {
      if(javaObject == null) {
          throw new RuntimeException("javaObject may not be null in TableHiveObject constructor");
      }


    this.javaObject = javaObject;
    this.tableSerDe = tableSerDe;
    this.rowObject = javaObject;
    this.partCols = partCols;
    this.partFields = partFields;
  }

  protected TableHiveObject(Object javaObject, SerDe tableSerDe,
                            SerDeField myField, Object rowObject,
                            ArrayList<String> partCols,
                            ArrayList<SerDeField> partFields) {
    if(javaObject == null) {
      throw new RuntimeException("javaObject may not be null in TableHiveObject constructor");
    }
    this.javaObject = javaObject;
    this.tableSerDe = tableSerDe;
    this.myField = myField;
    this.rowObject = rowObject;
    this.partCols = partCols;
    this.partFields = partFields;
  }

  public SerDeField getFieldFromExpression(String expr) throws HiveException {
    try {
      if(expr == null || expr.equals(""))
        throw new RuntimeException("Need non empty expression");

      // Check if this is a partition column
      if (partCols != null) {
        int pos = partCols.indexOf(expr);
        if (pos != -1) {
          return partFields.get(pos);
        }
      }

      String realExpr;
      if(myField != null) {
        if (expr.charAt(0) == '[') {
          realExpr = myField.getName() + expr;
        } else {
          realExpr = myField.getName() + "." + expr;
        }
      } else {
        realExpr = expr;
      }

      if(!ExpressionUtils.isComplexExpression(realExpr)) {
        return tableSerDe.getFieldFromExpression(null, realExpr);
      } else {
        return new ComplexSerDeField(null, realExpr, tableSerDe);
      }
    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException (e);
    }
  }

  public HiveObject get(SerDeField field) throws HiveException {
    try {
      Object o = field.get(rowObject);
      if (o == null) 
        return new NullHiveObject();

      if(field.isPrimitive())
        return new PrimitiveHiveObject(o);
      else 
        return new TableHiveObject(o, tableSerDe, field, rowObject, partCols, partFields);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  public List<SerDeField> getFields() throws HiveException {
    try {
      return tableSerDe.getFields(myField);
    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException (e);
    }
  }

  public boolean isPrimitive() { return false;}

  
  public String toString() {
    try {
      return tableSerDe.toJSONString(myField.get(rowObject), myField);
    } catch (SerDeException e) {
      throw new RuntimeException(e); 
    }
  }
  
  @Override
  public boolean equals(Object other) {
    throw new RuntimeException("not supported");
  }

  @Override
  public int hashCode() {
    throw new RuntimeException("not supported");
  }

}
