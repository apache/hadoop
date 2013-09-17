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

package org.apache.hadoop.fs.slive;

import org.apache.hadoop.io.Text;

/**
 * An operation output has the following object format whereby simple types are
 * represented as a key of dataType:operationType*measurementType and these
 * simple types can be combined (mainly in the reducer) using there given types
 * into a single operation output.
 * 
 * Combination is done based on the data types and the following convention is
 * followed (in the following order). If one is a string then the other will be
 * concated as a string with a ";" separator. If one is a double then the other
 * will be added as a double and the output will be a double. If one is a float
 * then the other will be added as a float and the the output will be a float.
 * Following this if one is a long the other will be added as a long and the
 * output type will be a long and if one is a integer the other will be added as
 * a integer and the output type will be an integer.
 */
class OperationOutput {

  private OutputType dataType;
  private String opType, measurementType;
  private Object value;

  private static final String TYPE_SEP = ":";
  private static final String MEASUREMENT_SEP = "*";
  private static final String STRING_SEP = ";";

  static enum OutputType {
    STRING, FLOAT, LONG, DOUBLE, INTEGER
  }

  /**
   * Parses a given key according to the expected key format and forms the given
   * segments.
   * 
   * @param key
   *          the key in expected dataType:operationType*measurementType format
   * @param value
   *          a generic value expected to match the output type
   * @throws IllegalArgumentException
   *           if invalid format
   */
  OperationOutput(String key, Object value) {
    int place = key.indexOf(TYPE_SEP);
    if (place == -1) {
      throw new IllegalArgumentException(
          "Invalid key format - no type seperator - " + TYPE_SEP);
    }
    try {
      dataType = OutputType.valueOf(key.substring(0, place).toUpperCase());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid key format - invalid output type", e);
    }
    key = key.substring(place + 1);
    place = key.indexOf(MEASUREMENT_SEP);
    if (place == -1) {
      throw new IllegalArgumentException(
          "Invalid key format - no measurement seperator - " + MEASUREMENT_SEP);
    }
    opType = key.substring(0, place);
    measurementType = key.substring(place + 1);
    this.value = value;
  }

  OperationOutput(Text key, Object value) {
    this(key.toString(), value);
  }

  public String toString() {
    return getKeyString() + " (" + this.value + ")";
  }

  OperationOutput(OutputType dataType, String opType, String measurementType,
      Object value) {
    this.dataType = dataType;
    this.opType = opType;
    this.measurementType = measurementType;
    this.value = value;
  }

  /**
   * Merges according to the documented rules for merging. Only will merge if
   * measurement type and operation type is the same.
   * 
   * @param o1
   *          the first object to merge with the second
   * @param o2
   *          the second object.
   * 
   * @return OperationOutput merged output.
   * 
   * @throws IllegalArgumentException
   *           if unable to merge due to incompatible formats/types
   */
  static OperationOutput merge(OperationOutput o1, OperationOutput o2) {
    if (o1.getMeasurementType().equals(o2.getMeasurementType())
        && o1.getOperationType().equals(o2.getOperationType())) {
      Object newvalue = null;
      OutputType newtype = null;
      String opType = o1.getOperationType();
      String mType = o1.getMeasurementType();
      if (o1.getOutputType() == OutputType.STRING
          || o2.getOutputType() == OutputType.STRING) {
        newtype = OutputType.STRING;
        StringBuilder str = new StringBuilder();
        str.append(o1.getValue());
        str.append(STRING_SEP);
        str.append(o2.getValue());
        newvalue = str.toString();
      } else if (o1.getOutputType() == OutputType.DOUBLE
          || o2.getOutputType() == OutputType.DOUBLE) {
        newtype = OutputType.DOUBLE;
        try {
          newvalue = Double.parseDouble(o1.getValue().toString())
              + Double.parseDouble(o2.getValue().toString());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Unable to combine a type with a double " + o1 + " & " + o2, e);
        }
      } else if (o1.getOutputType() == OutputType.FLOAT
          || o2.getOutputType() == OutputType.FLOAT) {
        newtype = OutputType.FLOAT;
        try {
          newvalue = Float.parseFloat(o1.getValue().toString())
              + Float.parseFloat(o2.getValue().toString());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Unable to combine a type with a float " + o1 + " & " + o2, e);
        }
      } else if (o1.getOutputType() == OutputType.LONG
          || o2.getOutputType() == OutputType.LONG) {
        newtype = OutputType.LONG;
        try {
          newvalue = Long.parseLong(o1.getValue().toString())
              + Long.parseLong(o2.getValue().toString());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Unable to combine a type with a long " + o1 + " & " + o2, e);
        }
      } else if (o1.getOutputType() == OutputType.INTEGER
          || o2.getOutputType() == OutputType.INTEGER) {
        newtype = OutputType.INTEGER;
        try {
          newvalue = Integer.parseInt(o1.getValue().toString())
              + Integer.parseInt(o2.getValue().toString());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Unable to combine a type with an int " + o1 + " & " + o2, e);
        }
      }
      return new OperationOutput(newtype, opType, mType, newvalue);
    } else {
      throw new IllegalArgumentException("Unable to combine dissimilar types "
          + o1 + " & " + o2);
    }
  }

  /**
   * Formats the key for output
   * 
   * @return String
   */
  private String getKeyString() {
    StringBuilder str = new StringBuilder();
    str.append(getOutputType().name());
    str.append(TYPE_SEP);
    str.append(getOperationType());
    str.append(MEASUREMENT_SEP);
    str.append(getMeasurementType());
    return str.toString();
  }

  /**
   * Retrieves the key in a hadoop text object
   * 
   * @return Text text output
   */
  Text getKey() {
    return new Text(getKeyString());
  }

  /**
   * Gets the output value in text format
   * 
   * @return Text
   */
  Text getOutputValue() {
    StringBuilder valueStr = new StringBuilder();
    valueStr.append(getValue());
    return new Text(valueStr.toString());
  }

  /**
   * Gets the object that represents this value (expected to match the output
   * data type)
   * 
   * @return Object
   */
  Object getValue() {
    return value;
  }

  /**
   * Gets the output data type of this class.
   */
  OutputType getOutputType() {
    return dataType;
  }

  /**
   * Gets the operation type this object represents.
   * 
   * @return String
   */
  String getOperationType() {
    return opType;
  }

  /**
   * Gets the measurement type this object represents.
   * 
   * @return String
   */
  String getMeasurementType() {
    return measurementType;
  }

}
