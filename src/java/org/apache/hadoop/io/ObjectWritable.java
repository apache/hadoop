/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.lang.reflect.Array;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

/** A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
public class ObjectWritable implements Writable, Configurable {

  private Class declaredClass;
  private Object instance;
  private Configuration conf;

  public ObjectWritable() {}
  
  public ObjectWritable(Object instance) {
    set(instance);
  }

  public ObjectWritable(Class declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public Object get() { return instance; }
  
  /** Return the class this is meant to be. */
  public Class getDeclaredClass() { return declaredClass; }
  
  /** Reset the instance. */
  public void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }
  
  public void readFields(DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }
  
  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass);
  }

  private static final Map PRIMITIVE_NAMES = new HashMap();
  static {
    PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
    PRIMITIVE_NAMES.put("byte", Byte.TYPE);
    PRIMITIVE_NAMES.put("char", Character.TYPE);
    PRIMITIVE_NAMES.put("short", Short.TYPE);
    PRIMITIVE_NAMES.put("int", Integer.TYPE);
    PRIMITIVE_NAMES.put("long", Long.TYPE);
    PRIMITIVE_NAMES.put("float", Float.TYPE);
    PRIMITIVE_NAMES.put("double", Double.TYPE);
    PRIMITIVE_NAMES.put("void", Void.TYPE);
  }

  private static class NullInstance implements Writable {
    private Class declaredClass;
    public NullInstance() {}
    public NullInstance(Class declaredClass) {
      this.declaredClass = declaredClass;
    }
    public void readFields(DataInput in) throws IOException {
      String className = UTF8.readString(in);
      declaredClass = (Class)PRIMITIVE_NAMES.get(className);
      if (declaredClass == null) {
        try {
          declaredClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e.toString());
        }
      }
    }
    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, declaredClass.getName());
    }
  }

  /** Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static void writeObject(DataOutput out, Object instance,
                                 Class declaredClass) throws IOException {

    if (instance == null) {                       // null
      instance = new NullInstance(declaredClass);
      declaredClass = NullInstance.class;
    }

    if (instance instanceof Writable) {           // Writable

      // write instance's class, to support subclasses of the declared class
      UTF8.writeString(out, instance.getClass().getName());
      
      ((Writable)instance).write(out);

      return;
    }

    // write declared class for primitives, as they can't be subclassed, and
    // the class of the instance may be a wrapper
    UTF8.writeString(out, declaredClass.getName());

    if (declaredClass.isArray()) {                // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i),
                    declaredClass.getComponentType());
      }
      
    } else if (declaredClass == String.class) {   // String
      UTF8.writeString(out, (String)instance);
      
    } else if (declaredClass.isPrimitive()) {     // primitive type

      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
      
    } else {
      throw new IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }
  
  
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static Object readObject(DataInput in, Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }
    
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static Object readObject(DataInput in, ObjectWritable objectWritable, Configuration conf)
    throws IOException {
    String className = UTF8.readString(in);
    Class declaredClass = (Class)PRIMITIVE_NAMES.get(className);
    if (declaredClass == null) {
      try {
        declaredClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e.toString());
      }
    }    

    Object instance;
    
    if (declaredClass == NullInstance.class) {         // null
      NullInstance wrapper = new NullInstance();
      wrapper.readFields(in);
      declaredClass = wrapper.declaredClass;
      instance = null;

    } else if (declaredClass.isPrimitive()) {          // primitive types

      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = new Character(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = new Byte(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = new Short(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = new Integer(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = new Long(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = new Float(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = new Double(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }

    } else if (declaredClass.isArray()) {              // array
      int length = in.readInt();
      instance = Array.newInstance(declaredClass.getComponentType(), length);
      for (int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in, conf));
      }
      
    } else if (declaredClass == String.class) {        // String
      instance = UTF8.readString(in);
      
    } else {                                      // Writable
      Writable writable = WritableFactories.newInstance(declaredClass);
      if(writable instanceof Configurable) {
        ((Configurable) writable).setConf(conf);
      }
      writable.readFields(in);
      instance = writable;
    }

    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }

    return instance;
      
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  
}
