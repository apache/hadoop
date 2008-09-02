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

package org.apache.hadoop.hive.serde;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of SerDeField that provides support for nested and indexed accesses
 * Assumes that a class is composed of types - all of which can be handled by same SerDe
 * family
 *
 */

public class ComplexSerDeField implements SerDeField {
  public static final Log l4j = LogFactory.getLog("ComplexSerDeField");

  /**
   * A complex field is a series of units. Each unit is a field
   */
  public static class Unit implements SerDeField {

    SerDeField unitField;

    // List related state
    boolean isList;
    boolean isSlice;
    int lowIndex;
    int highIndex;

    // map related state
    boolean isMap;
    String keyString;
    Object keyValue;


    public Unit() {
      unitField = null;
      keyValue = null;
    }

    public void init(SerDeField h, String sliceExpression) throws SerDeException {
      unitField = h;
      l4j.debug("Unit.init HF= "+ReflectionSerDeField.fieldToString(unitField));

      isList = unitField.isList();
      isSlice = isList;
      lowIndex=0;
      highIndex=-1;

      isMap = unitField.isMap();

      if (sliceExpression == null) {
        return;
      } else if (!isList && !isMap) {
        throw new SerDeException("Trying to index type "+unitField.getType().getName()+" which is neither list nor map");
      }

      try {
        if (isList) {
          if (sliceExpression.indexOf(":") == -1) {
            isSlice = false;
            lowIndex = Integer.parseInt(sliceExpression);
          } else {
            String [] lh = sliceExpression.split(":");
            if (lh.length > 2) {
              throw new SerDeException("Illegal list slice: "+sliceExpression);
            }
            if (! lh[0].equals("")) {
              lowIndex = Integer.parseInt(lh[0]);
            }
            if (! lh[1].equals("")) {
              highIndex = Integer.parseInt(lh[1]);
            }
          }
        } else if (isMap) {
          keyString = sliceExpression;
        }
      } catch (NumberFormatException e) {
        throw new SerDeException("Illegal slice expression "+sliceExpression);
      }
    }

    public Object get(Object o) throws SerDeException {
      Object obj = unitField.get(o);

      if ((!isList && !isMap) || (obj==null)) {
        return obj;
      }

      if (isList) {

        if (isSlice && (lowIndex == 0) && (highIndex == -1))
          return obj;

        List lst = (List)obj;
        int realLow = (lowIndex >= 0) ? lowIndex : lowIndex + lst.size();

        if (!isSlice) {
          if ((realLow < 0) || (realLow >= lst.size())) {
            return null;
          }
          return lst.get(realLow);
        }

        int realHigh = (highIndex >= 0) ? highIndex : highIndex + lst.size();

        // if the slice does not find anything - return an empty list
        // this is not the same as null - which can be taken to mean an
        // undefined member
        if ((realLow < 0) || (realLow >= lst.size())) {
          return new ArrayList (0);
        }

        if ((realHigh < 0) || (realHigh >= lst.size())) {
          return new ArrayList (0);
        }

        if (realHigh < realLow) {
          return new ArrayList (0);
        }


        ArrayList ret = new ArrayList (realHigh - realLow + 1);
        for (int i=realLow; i<=realHigh; i++) {
          ret.add(lst.get(i));
        }
        return ret;

      } else { //isMap
        if (keyString != null) {
          return ((Map)obj).get(keyString);
        } else {
          return obj;
        }
      }
    }

    public Class getType() {
      if (isList && !isSlice) {
        return unitField.getListElementType();
      } else if (isMap && (keyString != null)) {
        return unitField.getMapValueType();
      } else  {
        return unitField.getType();
      }
    }

    public boolean isList() {
      return (isList && isSlice);
    }

    public boolean isPrimitive() {
      if (isList && !isSlice) {
        return ReflectionSerDeField.isClassPrimitive(unitField.getListElementType());
      } else if (isMap && (keyString != null)) {
        return ReflectionSerDeField.isClassPrimitive(unitField.getMapValueType());
      } else {
        return unitField.isPrimitive();
      }
    }

    public boolean isMap() {
      return (isMap && (keyString == null));
    }

    public Class getListElementType() {
      if (isList()) {
        return unitField.getListElementType();
      } else {
        throw new RuntimeException("Not a list");
      }
    }
    public Class getMapKeyType() {
      if (isMap()) {
        return unitField.getMapKeyType();
      } else {
        throw new RuntimeException("Not a map");
      }
    }
    public Class getMapValueType()  {
      if (isMap()) {
        return unitField.getMapValueType();
      } else {
        throw new RuntimeException("Not a map");
      }
    }

    public String getName() {
      if (isList) {
        return (unitField.getName() +
                "["+
                (isSlice ? lowIndex+":"+highIndex : lowIndex)
                + "]");
      } else if (isMap) {
        return (unitField.getName() + ((keyString != null) ? "["+keyString+"]" : ""));
      } else {
        return unitField.getName();
      }
    }
    public String toString() {
      return "ComplexSerDeField.Unit[" +
        "isList=" + isList +
        ",isSlice=" + isSlice +
        ",lowIndex=" + lowIndex +
        ",highIndex=" + highIndex +
        ",isMap=" + isMap +
        ",keyString=" + keyString +
        ",keyValue=" + keyValue +
        "]";
    }

  } // end unit class

  private ArrayList currentObjects, nextObjects;
  private static Pattern sUnit = Pattern.compile("([^\\.\\[\\]]+?)?(\\[([^\\[\\]\\.]+?)\\])?");

  protected Unit[] unitArray;
  protected boolean isList;
  protected boolean isMap;
  protected boolean isClove;
  protected boolean isLength;
  protected String fieldName;

  public ComplexSerDeField(SerDeField parentField, String fieldName, SerDe classSD)
    throws SerDeException {

    this.fieldName = fieldName;
    isMap = isList = isClove = false;
    if (fieldName.startsWith("#")) {
      fieldName = fieldName.substring(1);
      isLength = true;
    } else {
      isLength = false;
    }

    SerDeField curField = parentField;
    String [] component = fieldName.split("\\.");
    unitArray = new Unit [component.length];
    int firstListIndex = -1;

    l4j.debug("ComplexSerDeField: Parent="+ ((parentField != null) ? parentField.getName() : "")+
              ", Field="+fieldName);

    for (int i=0; i<component.length; i++) {

      Matcher m = sUnit.matcher(component[i]);
      if (!m.matches())
        throw new SerDeException("Illegal fieldName: "+fieldName);
      String cField = m.group(1);
      String cIndex = m.group(3);

      unitArray[i] = new Unit();
      if (cField != null) {
        curField = classSD.getFieldFromExpression(curField, cField);
      }
      unitArray[i].init(curField, cIndex);

      if (unitArray[i].isList()) {
        isList = true;
        if (firstListIndex == -1) {
          firstListIndex = i;
          if (i == (component.length - 1)) {
            isClove = true;
          }
        }
      } else if (unitArray[i].isMap()) {
        isMap = true;
        if (i != (component.length-1))
          throw new SerDeException("Trying to nest within map field: " + cField);

      } else if (unitArray[i].isPrimitive()) {
        if (i != (component.length-1))
          throw new SerDeException("Trying to nest within primitive field: " + cField);
      }

      l4j.debug("Unit="+ReflectionSerDeField.fieldToString(unitArray[i]));

      // next time when we pass the field to the SerDe - we will pass the unit field.
      curField = unitArray[i];
    }

    if (isLength && !isList) {
      throw new SerDeException("Cannot get length of non-list type: "+this.fieldName);
    }

    currentObjects = new ArrayList();
    nextObjects = new ArrayList();
  }

  private Object getScalar(Object obj) throws SerDeException {
    Object curObj = obj;
    for (int i=0; i<unitArray.length; i++) {
      curObj = unitArray[i].get(curObj);
      if (curObj == null)
        return null;
    }
    return curObj;
  }

  public Object get(Object obj) throws SerDeException {
    if (!isList) {
      return getScalar(obj);
    }

    List ret;
    if (isClove) {
      // a clove is a pattern where only the last element in the dotted notation is
      // a list and there is no slice desired on the list. in this case, the field
      // lookup algorithm simplifies to that of a scalar.
      ret = (List) getScalar(obj);
    } else {
      ret = (List) getList(obj);
    }

    if (isLength) {
      if (ret == null) {
        return Integer.valueOf(0);
      }
      return Integer.valueOf(ret.size());
    } else {
      return ret;
    }
  }

  private Object getList(Object obj) throws SerDeException {
    currentObjects.clear();
    nextObjects.clear();

    currentObjects.add(obj);

    for (int i=0; i<unitArray.length; i++) {
      Iterator iter = currentObjects.iterator();
      while (iter.hasNext()) {
        Object oneObj = iter.next();
        Object childObj = unitArray[i].get(oneObj);

        if (childObj == null)
          continue;

        if (unitArray[i].isList()) {
          Iterator childIter = ((AbstractList)childObj).iterator();
          while (childIter.hasNext()) {
            nextObjects.add(childIter.next());
          }
        } else {
          nextObjects.add(childObj);
        }
      }
      currentObjects.clear();
      ArrayList tmp = currentObjects;
      currentObjects = nextObjects;
      nextObjects = tmp;
    }
    return currentObjects;
  }

  public Class getType() {
    if (isLength) {
      return Integer.class;
    } else {
      if (isList)
        return List.class;

      // Type of nested expression is the type of the leaf component
      return unitArray[unitArray.length-1].getType();
    }
  }

  public Class getListElementType() {
    if (isList()) {
      if (unitArray[unitArray.length-1].isList()) {
        // Either the leaf elements are themselves lists - in which case
        // the final element type are the elements in the leaf lists
        return (unitArray[unitArray.length-1].getListElementType());
      } else {
        // or the list is composed of elements of the type of the leaf nodes
        return (unitArray[unitArray.length-1].getType());
      }
    } else {
      throw new RuntimeException("Not a list field ");
    }
  }

  public Class getMapKeyType() {
    if (isMap()) {
      return (unitArray[unitArray.length-1].getMapKeyType());
    } else {
      throw new RuntimeException("Not a map field ");
    }
  }

  public Class getMapValueType() {
    if (isMap()) {
      return (unitArray[unitArray.length-1].getMapValueType());
    } else {
      throw new RuntimeException("Not a map field ");
    }
  }

  public boolean isList() {
    return (isList && !isLength);
  }

  public boolean isMap() {
    return(isMap && !isLength);
  }

  public boolean isPrimitive() {
    if (isLength)
      return true;

    if (isList || isMap)
      return false;

    // Whether primitive or not depends on type of the leaf component
    return unitArray[unitArray.length-1].isPrimitive();
  }

  public String getName() {
    return this.fieldName;
  }

  public String toString() {
    return "ComplexSerDeField[" +
      "unitArray=" + unitArray +
      ",isList=" + isList +
      ",isMap=" + isMap +
      ",isClove=" + isClove +
      ",isLength=" + isLength +
      ",fieldName=" + this.fieldName +
      "]";
  }

  private static Pattern metachars = Pattern.compile(".*[\\.#\\[\\]].*");
  public static boolean isComplexExpression(String exp) {
    Matcher m = metachars.matcher(exp);
    return(m.matches());
  }
}
