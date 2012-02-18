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

package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a wrapper class.  It wraps a Writable implementation around
 * an array of primitives (e.g., int[], long[], etc.), with optimized 
 * wire format, and without creating new objects per element.
 * 
 * This is a wrapper class only; it does not make a copy of the 
 * underlying array.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayPrimitiveWritable implements Writable {
  
  //componentType is determined from the component type of the value array 
  //during a "set" operation.  It must be primitive.
  private Class<?> componentType = null; 
  //declaredComponentType need not be declared, but if you do (by using the
  //ArrayPrimitiveWritable(Class<?>) constructor), it will provide typechecking
  //for all "set" operations.
  private Class<?> declaredComponentType = null;
  private int length;
  private Object value; //must be an array of <componentType>[length]
  
  private static final Map<String, Class<?>> PRIMITIVE_NAMES = 
    new HashMap<String, Class<?>>(16);
  static {
    PRIMITIVE_NAMES.put(boolean.class.getName(), boolean.class);
    PRIMITIVE_NAMES.put(byte.class.getName(), byte.class);
    PRIMITIVE_NAMES.put(char.class.getName(), char.class);
    PRIMITIVE_NAMES.put(short.class.getName(), short.class);
    PRIMITIVE_NAMES.put(int.class.getName(), int.class);
    PRIMITIVE_NAMES.put(long.class.getName(), long.class);
    PRIMITIVE_NAMES.put(float.class.getName(), float.class);
    PRIMITIVE_NAMES.put(double.class.getName(), double.class);
  }
  
  private static Class<?> getPrimitiveClass(String className) {
    return PRIMITIVE_NAMES.get(className);
  }
  
  private static void checkPrimitive(Class<?> componentType) {
    if (componentType == null) { 
      throw new HadoopIllegalArgumentException("null component type not allowed"); 
    }
    if (! PRIMITIVE_NAMES.containsKey(componentType.getName())) {
      throw new HadoopIllegalArgumentException("input array component type "
          + componentType.getName() + " is not a candidate primitive type");
    }
  }
  
  private void checkDeclaredComponentType(Class<?> componentType) {
    if ((declaredComponentType != null) 
        && (componentType != declaredComponentType)) {
      throw new HadoopIllegalArgumentException("input array component type "
          + componentType.getName() + " does not match declared type "
          + declaredComponentType.getName());     
    }
  }
  
  private static void checkArray(Object value) {
    if (value == null) { 
      throw new HadoopIllegalArgumentException("null value not allowed"); 
    }
    if (! value.getClass().isArray()) {
      throw new HadoopIllegalArgumentException("non-array value of class "
          + value.getClass() + " not allowed");             
    }
  }
  
  /**
   * Construct an empty instance, for use during Writable read
   */
  public ArrayPrimitiveWritable() {
    //empty constructor
  }
  
  /**
   * Construct an instance of known type but no value yet
   * for use with type-specific wrapper classes
   */
  public ArrayPrimitiveWritable(Class<?> componentType) {
    checkPrimitive(componentType);
    this.declaredComponentType = componentType;
  }
  
  /**
   * Wrap an existing array of primitives
   * @param value - array of primitives
   */
  public ArrayPrimitiveWritable(Object value) {
    set(value);
  }
  
  /**
   * Get the original array.  
   * Client must cast it back to type componentType[]
   * (or may use type-specific wrapper classes).
   * @return - original array as Object
   */
  public Object get() { return value; }
  
  public Class<?> getComponentType() { return componentType; }
  
  public Class<?> getDeclaredComponentType() { return declaredComponentType; }
  
  public boolean isDeclaredComponentType(Class<?> componentType) {
    return componentType == declaredComponentType;
  }
  
  public void set(Object value) {
    checkArray(value);
    Class<?> componentType = value.getClass().getComponentType();
    checkPrimitive(componentType);
    checkDeclaredComponentType(componentType);
    this.componentType = componentType;
    this.value = value;
    this.length = Array.getLength(value);
  }
  
  /**
   * Do not use this class.
   * This is an internal class, purely for ObjectWritable to use as
   * a label class for transparent conversions of arrays of primitives
   * during wire protocol reads and writes.
   */
  static class Internal extends ArrayPrimitiveWritable {
    Internal() {             //use for reads
      super(); 
    }
    
    Internal(Object value) { //use for writes
      super(value);
    }
  } //end Internal subclass declaration

  /* 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  @SuppressWarnings("deprecation")
  public void write(DataOutput out) throws IOException {
    // write componentType 
    UTF8.writeString(out, componentType.getName());      
    // write length
    out.writeInt(length);

    // do the inner loop.  Walk the decision tree only once.
    if (componentType == Boolean.TYPE) {          // boolean
      writeBooleanArray(out);
    } else if (componentType == Character.TYPE) { // char
      writeCharArray(out);
    } else if (componentType == Byte.TYPE) {      // byte
      writeByteArray(out);
    } else if (componentType == Short.TYPE) {     // short
      writeShortArray(out);
    } else if (componentType == Integer.TYPE) {   // int
      writeIntArray(out);
    } else if (componentType == Long.TYPE) {      // long
      writeLongArray(out);
    } else if (componentType == Float.TYPE) {     // float
      writeFloatArray(out);
    } else if (componentType == Double.TYPE) {    // double
      writeDoubleArray(out);
    } else {
      throw new IOException("Component type " + componentType.toString()
          + " is set as the output type, but no encoding is implemented for this type.");
    }
  }

  /* 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    
    // read and set the component type of the array
    @SuppressWarnings("deprecation")
    String className = UTF8.readString(in);
    Class<?> componentType = getPrimitiveClass(className);
    if (componentType == null) {
      throw new IOException("encoded array component type "
          + className + " is not a candidate primitive type");
    }
    checkDeclaredComponentType(componentType);
    this.componentType = componentType;
  
    // read and set the length of the array
    int length = in.readInt();
    if (length < 0) {
      throw new IOException("encoded array length is negative " + length);
    }
    this.length = length;
    
    // construct and read in the array
    value = Array.newInstance(componentType, length);

    // do the inner loop.  Walk the decision tree only once.
    if (componentType == Boolean.TYPE) {             // boolean
      readBooleanArray(in);
    } else if (componentType == Character.TYPE) {    // char
      readCharArray(in);
    } else if (componentType == Byte.TYPE) {         // byte
      readByteArray(in);
    } else if (componentType == Short.TYPE) {        // short
      readShortArray(in);
    } else if (componentType == Integer.TYPE) {      // int
      readIntArray(in);
    } else if (componentType == Long.TYPE) {         // long
      readLongArray(in);
    } else if (componentType == Float.TYPE) {        // float
      readFloatArray(in);
    } else if (componentType == Double.TYPE) {       // double
      readDoubleArray(in);
    } else {
      throw new IOException("Encoded type " + className
          + " converted to valid component type " + componentType.toString()
          + " but no encoding is implemented for this type.");
    }
  }
  
  //For efficient implementation, there's no way around
  //the following massive code duplication.
  
  private void writeBooleanArray(DataOutput out) throws IOException {
    boolean[] v = (boolean[]) value;
    for (int i = 0; i < length; i++)
      out.writeBoolean(v[i]);
  }
  
  private void writeCharArray(DataOutput out) throws IOException {
    char[] v = (char[]) value;
    for (int i = 0; i < length; i++)
      out.writeChar(v[i]);
  }
  
  private void writeByteArray(DataOutput out) throws IOException {
    out.write((byte[]) value, 0, length);
  }
  
  private void writeShortArray(DataOutput out) throws IOException {
    short[] v = (short[]) value;
    for (int i = 0; i < length; i++)
      out.writeShort(v[i]);
  }
  
  private void writeIntArray(DataOutput out) throws IOException {
    int[] v = (int[]) value;
    for (int i = 0; i < length; i++)
      out.writeInt(v[i]);
  }
  
  private void writeLongArray(DataOutput out) throws IOException {
    long[] v = (long[]) value;
    for (int i = 0; i < length; i++)
      out.writeLong(v[i]);
  }
  
  private void writeFloatArray(DataOutput out) throws IOException {
    float[] v = (float[]) value;
    for (int i = 0; i < length; i++)
      out.writeFloat(v[i]);
  }
  
  private void writeDoubleArray(DataOutput out) throws IOException {
    double[] v = (double[]) value;
    for (int i = 0; i < length; i++)
      out.writeDouble(v[i]);
  }

  private void readBooleanArray(DataInput in) throws IOException {
    boolean[] v = (boolean[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readBoolean(); 
  }
  
  private void readCharArray(DataInput in) throws IOException {
    char[] v = (char[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readChar(); 
  }
  
  private void readByteArray(DataInput in) throws IOException {
    in.readFully((byte[]) value, 0, length);
  }
  
  private void readShortArray(DataInput in) throws IOException {
    short[] v = (short[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readShort(); 
  }
  
  private void readIntArray(DataInput in) throws IOException {
    int[] v = (int[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readInt(); 
  }
  
  private void readLongArray(DataInput in) throws IOException {
    long[] v = (long[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readLong(); 
  }
  
  private void readFloatArray(DataInput in) throws IOException {
    float[] v = (float[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readFloat(); 
  }
  
  private void readDoubleArray(DataInput in) throws IOException {
    double[] v = (double[]) value;
    for (int i = 0; i < length; i++)
      v[i] = in.readDouble(); 
  }
}

