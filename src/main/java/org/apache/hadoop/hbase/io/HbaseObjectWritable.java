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

package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This is a customized version of the polymorphic hadoop
 * {@link ObjectWritable}.  It removes UTF8 (HADOOP-414).
 * Using {@link Text} intead of UTF-8 saves ~2% CPU between reading and writing
 * objects running a short sequentialWrite Performance Evaluation test just in
 * ObjectWritable alone; more when we're doing randomRead-ing.  Other
 * optimizations include our passing codes for classes instead of the
 * actual class names themselves.  This makes it so this class needs amendment
 * if non-Writable classes are introduced -- if passed a Writable for which we
 * have no code, we just do the old-school passing of the class name, etc. --
 * but passing codes the  savings are large particularly when cell
 * data is small (If < a couple of kilobytes, the encoding/decoding of class
 * name and reflection to instantiate class was costing in excess of the cell
 * handling).
 */
public class HbaseObjectWritable implements Writable, Configurable {
  protected final static Log LOG = LogFactory.getLog(HbaseObjectWritable.class);

  // Here we maintain two static maps of classes to code and vice versa.
  // Add new classes+codes as wanted or figure way to auto-generate these
  // maps from the HMasterInterface.
  static final Map<Byte, Class<?>> CODE_TO_CLASS =
    new HashMap<Byte, Class<?>>();
  static final Map<Class<?>, Byte> CLASS_TO_CODE =
    new HashMap<Class<?>, Byte>();
  // Special code that means 'not-encoded'; in this case we do old school
  // sending of the class name using reflection, etc.
  private static final byte NOT_ENCODED = 0;
  static {
    byte code = NOT_ENCODED + 1;
    // Primitive types.
    addToMap(Boolean.TYPE, code++);
    addToMap(Byte.TYPE, code++);
    addToMap(Character.TYPE, code++);
    addToMap(Short.TYPE, code++);
    addToMap(Integer.TYPE, code++);
    addToMap(Long.TYPE, code++);
    addToMap(Float.TYPE, code++);
    addToMap(Double.TYPE, code++);
    addToMap(Void.TYPE, code++);

    // Other java types
    addToMap(String.class, code++);
    addToMap(byte [].class, code++);
    addToMap(byte [][].class, code++);

    // Hadoop types
    addToMap(Text.class, code++);
    addToMap(Writable.class, code++);
    addToMap(Writable [].class, code++);
    addToMap(HbaseMapWritable.class, code++);
    addToMap(NullInstance.class, code++);

    // Hbase types
    addToMap(HColumnDescriptor.class, code++);
    addToMap(HConstants.Modify.class, code++);
    addToMap(HMsg.class, code++);
    addToMap(HMsg[].class, code++);
    addToMap(HRegion.class, code++);
    addToMap(HRegion[].class, code++);
    addToMap(HRegionInfo.class, code++);
    addToMap(HRegionInfo[].class, code++);
    addToMap(HServerAddress.class, code++);
    addToMap(HServerInfo.class, code++);
    addToMap(HTableDescriptor.class, code++);
    addToMap(MapWritable.class, code++);

    //
    // HBASE-880
    //
    addToMap(ClusterStatus.class, code++);
    addToMap(Delete.class, code++);
    addToMap(Get.class, code++);
    addToMap(KeyValue.class, code++);
    addToMap(KeyValue[].class, code++);
    addToMap(Put.class, code++);
    addToMap(Put[].class, code++);
    addToMap(Result.class, code++);
    addToMap(Result[].class, code++);
    addToMap(Scan.class, code++);

    addToMap(WhileMatchFilter.class, code++);
    addToMap(PrefixFilter.class, code++);
    addToMap(PageFilter.class, code++);
    addToMap(InclusiveStopFilter.class, code++);
    addToMap(ColumnCountGetFilter.class, code++);
    addToMap(SingleColumnValueFilter.class, code++);
    addToMap(SingleColumnValueExcludeFilter.class, code++);
    addToMap(BinaryComparator.class, code++);
    addToMap(CompareFilter.class, code++);
    addToMap(RowFilter.class, code++);
    addToMap(ValueFilter.class, code++);
    addToMap(QualifierFilter.class, code++);
    addToMap(SkipFilter.class, code++);
    addToMap(WritableByteArrayComparable.class, code++);
    addToMap(FirstKeyOnlyFilter.class, code++);
    addToMap(DependentColumnFilter.class, code++);
    addToMap(ColumnPrefixFilter.class, code++);

    addToMap(Delete [].class, code++);

    addToMap(MultiPut.class, code++);
    addToMap(MultiPutResponse.class, code++);

    addToMap(HLog.Entry.class, code++);
    addToMap(HLog.Entry[].class, code++);
    addToMap(HLogKey.class, code++);

    // List
    addToMap(List.class, code++);
  }

  private Class<?> declaredClass;
  private Object instance;
  private Configuration conf;

  /** default constructor for writable */
  public HbaseObjectWritable() {
    super();
  }

  /**
   * @param instance
   */
  public HbaseObjectWritable(Object instance) {
    set(instance);
  }

  /**
   * @param declaredClass
   * @param instance
   */
  public HbaseObjectWritable(Class<?> declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** @return the instance, or null if none. */
  public Object get() { return instance; }

  /** @return the class this is meant to be. */
  public Class<?> getDeclaredClass() { return declaredClass; }

  /**
   * Reset the instance.
   * @param instance
   */
  public void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }


  public void readFields(DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }

  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }

  private static class NullInstance extends Configured implements Writable {
    Class<?> declaredClass;
    /** default constructor for writable */
    @SuppressWarnings("unused")
    public NullInstance() { super(null); }

    /**
     * @param declaredClass
     * @param conf
     */
    public NullInstance(Class<?> declaredClass, Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }

    public void readFields(DataInput in) throws IOException {
      this.declaredClass = CODE_TO_CLASS.get(in.readByte());
    }

    public void write(DataOutput out) throws IOException {
      writeClassCode(out, this.declaredClass);
    }
  }

  /**
   * Write out the code byte for passed Class.
   * @param out
   * @param c
   * @throws IOException
   */
  static void writeClassCode(final DataOutput out, final Class<?> c)
  throws IOException {
    Byte code = CLASS_TO_CODE.get(c);
    if (code == null ) {
      if ( List.class.isAssignableFrom(c)) {
        code = CLASS_TO_CODE.get(List.class);
      }
    }
    if (code == null) {
      LOG.error("Unsupported type " + c);
      StackTraceElement[] els = new Exception().getStackTrace();
      for(StackTraceElement elem : els) {
        LOG.error(elem.getMethodName());
      }
//          new Exception().getStackTrace()[0].getMethodName());
//      throw new IOException(new Exception().getStackTrace()[0].getMethodName());
      throw new UnsupportedOperationException("No code for unexpected " + c);
    }
    out.writeByte(code);
  }

  /**
   * Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding.
   * @param out
   * @param instance
   * @param declaredClass
   * @param conf
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void writeObject(DataOutput out, Object instance,
                                 Class declaredClass,
                                 Configuration conf)
  throws IOException {

    Object instanceObj = instance;
    Class declClass = declaredClass;

    if (instanceObj == null) {                       // null
      instanceObj = new NullInstance(declClass, conf);
      declClass = Writable.class;
    }
    writeClassCode(out, declClass);
    if (declClass.isArray()) {                // array
      // If bytearray, just dump it out -- avoid the recursion and
      // byte-at-a-time we were previously doing.
      if (declClass.equals(byte [].class)) {
        Bytes.writeByteArray(out, (byte [])instanceObj);
      } else if(declClass.equals(Result [].class)) {
        Result.writeArray(out, (Result [])instanceObj);
      } else {
        int length = Array.getLength(instanceObj);
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
          writeObject(out, Array.get(instanceObj, i),
                    declClass.getComponentType(), conf);
        }
      }
    } else if (List.class.isAssignableFrom(declClass)) {
      List list = (List)instanceObj;
      int length = list.size();
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, list.get(i),
                  list.get(i).getClass(), conf);
      }
    } else if (declClass == String.class) {   // String
      Text.writeString(out, (String)instanceObj);
    } else if (declClass.isPrimitive()) {     // primitive type
      if (declClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instanceObj).booleanValue());
      } else if (declClass == Character.TYPE) { // char
        out.writeChar(((Character)instanceObj).charValue());
      } else if (declClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instanceObj).byteValue());
      } else if (declClass == Short.TYPE) {   // short
        out.writeShort(((Short)instanceObj).shortValue());
      } else if (declClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instanceObj).intValue());
      } else if (declClass == Long.TYPE) {    // long
        out.writeLong(((Long)instanceObj).longValue());
      } else if (declClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instanceObj).floatValue());
      } else if (declClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instanceObj).doubleValue());
      } else if (declClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declClass);
      }
    } else if (declClass.isEnum()) {         // enum
      Text.writeString(out, ((Enum)instanceObj).name());
    } else if (Writable.class.isAssignableFrom(declClass)) { // Writable
      Class <?> c = instanceObj.getClass();
      Byte code = CLASS_TO_CODE.get(c);
      if (code == null) {
        out.writeByte(NOT_ENCODED);
        Text.writeString(out, c.getName());
      } else {
        writeClassCode(out, c);
      }
      ((Writable)instanceObj).write(out);
    } else {
      throw new IOException("Can't write: "+instanceObj+" as "+declClass);
    }
  }


  /**
   * Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding.
   * @param in
   * @param conf
   * @return the object
   * @throws IOException
   */
  public static Object readObject(DataInput in, Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }

  /**
   * Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding.
   * @param in
   * @param objectWritable
   * @param conf
   * @return the object
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static Object readObject(DataInput in,
      HbaseObjectWritable objectWritable, Configuration conf)
  throws IOException {
    Class<?> declaredClass = CODE_TO_CLASS.get(in.readByte());
    Object instance;
    if (declaredClass.isPrimitive()) {            // primitive types
      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isArray()) {              // array
      if (declaredClass.equals(byte [].class)) {
        instance = Bytes.readByteArray(in);
      } else if(declaredClass.equals(Result [].class)) {
        instance = Result.readArray(in);
      } else {
        int length = in.readInt();
        instance = Array.newInstance(declaredClass.getComponentType(), length);
        for (int i = 0; i < length; i++) {
          Array.set(instance, i, readObject(in, conf));
        }
      }
    } else if (List.class.isAssignableFrom(declaredClass)) {              // List
      int length = in.readInt();
      instance = new ArrayList(length);
      for (int i = 0; i < length; i++) {
        ((ArrayList)instance).add(readObject(in, conf));
      }
    } else if (declaredClass == String.class) {        // String
      instance = Text.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((Class<? extends Enum>) declaredClass,
        Text.readString(in));
    } else {                                      // Writable
      Class instanceClass = null;
      Byte b = in.readByte();
      if (b.byteValue() == NOT_ENCODED) {
        String className = Text.readString(in);
        try {
          instanceClass = getClassByName(conf, className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Can't find class " + className);
        }
      } else {
        instanceClass = CODE_TO_CLASS.get(b);
      }
      Writable writable = WritableFactories.newInstance(instanceClass, conf);
      writable.readFields(in);
      instance = writable;
      if (instanceClass == NullInstance.class) {  // null
        declaredClass = ((NullInstance)instance).declaredClass;
        instance = null;
      }
    }
    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }
    return instance;
  }

  @SuppressWarnings("unchecked")
  private static Class getClassByName(Configuration conf, String className)
  throws ClassNotFoundException {
    if(conf != null) {
      return conf.getClassByName(className);
    }
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if(cl == null) {
      cl = HbaseObjectWritable.class.getClassLoader();
    }
    return Class.forName(className, true, cl);
  }

  private static void addToMap(final Class<?> clazz, final byte code) {
    CLASS_TO_CODE.put(clazz, code);
    CODE_TO_CLASS.put(code, clazz);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}