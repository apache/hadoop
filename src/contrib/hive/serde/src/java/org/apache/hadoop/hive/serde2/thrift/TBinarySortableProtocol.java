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


package org.apache.hadoop.hive.serde2.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.facebook.thrift.TException;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.protocol.*;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;

import java.util.Properties;

/**
 * An implementation of the Thrift Protocol for binary sortable records.
 * 
 * The data format:
 * NULL:  a single byte \0
 * NON-NULL Primitives: ALWAYS prepend a single byte \1, and then:
 *   Boolean: FALSE = \1, TRUE = \2
 *   Byte:    flip the sign-bit to make sure negative comes before positive
 *   Short:   flip the sign-bit to make sure negative comes before positive
 *   Int:     flip the sign-bit to make sure negative comes before positive
 *   Long:    flip the sign-bit to make sure negative comes before positive
 *   Double:  flip the sign-bit for positive double, and all bits for negative double values
 *   String:  NULL-terminated string
 * NON-NULL Complex Types:
 *   Struct:  first the single byte \1, and then one field by one field.
 *   List:    size stored as Int (see above), then one element by one element. 
 *   Map:     size stored as Int (see above), then one key by one value, and then the next pair and so on.
 *   Binary:  size stored as Int (see above), then the binary data in its original form
 *   
 * Note that the relative order of list/map/binary will be based on the size first (and elements one by one if 
 * the sizes are equal). 
 * 
 * This protocol takes an additional parameter SERIALIZATION_SORT_ORDER which is a string containing only "+" and "-".
 * The length of the string should equal to the number of fields in the top-level struct for serialization.
 * "+" means the field should be sorted ascendingly, and "-" means descendingly. The sub fields in the same top-level
 * field will have the same sort order. 
 * 
 * This is not thrift compliant in that it doesn't write out field ids
 * so things cannot actually be versioned.
 */
public class TBinarySortableProtocol extends TProtocol implements ConfigurableTProtocol, WriteNullsProtocol {

  final static Log LOG = LogFactory.getLog(TBinarySortableProtocol.class.getName());
  
  /**
   * Factory for TBinarySortableProtocol objects
   */
  public static class Factory implements TProtocolFactory {

    public TProtocol getProtocol(TTransport trans) {
      return new TBinarySortableProtocol(trans);
    }
  }

  public TBinarySortableProtocol(TTransport trans) {
    super(trans);
    stackLevel = 0;
  }

  /**
   * The stack level of the current field.  Top-level fields have a stackLevel value of 1.
   * Each nested struct/list/map will increase the stackLevel value by 1.
   */
  int stackLevel;
  /**
   * The field ID in the top level struct.  This is used to determine whether this field
   * should be sorted ascendingly or descendingly.  
   */
  int topLevelStructFieldID;
  /**
   * A string that consists of only "+" and "-".  It should have the same length as the number
   * of fields in the top level struct. "+" means the corresponding field is sorted ascendingly
   * and "-" means the corresponding field is sorted descendingly.
   */
  String sortOrder;
  /**
   * Whether the current field is sorted ascendingly.  Always equals to
   * sortOrder.charAt(topLevelStructFieldID) != '-'
   */
  boolean ascending; 
  
  @Override
  public void initialize(Configuration conf, Properties tbl) throws TException {
    sortOrder = tbl.getProperty(Constants.SERIALIZATION_SORT_ORDER);
    if (sortOrder == null) {
      sortOrder = "";
    }
    for(int i=0; i<sortOrder.length(); i++) {
      char c = sortOrder.charAt(i); 
      if (c != '+' && c != '-') {
        throw new TException(Constants.SERIALIZATION_SORT_ORDER + " should be a string consists of only '+' and '-'!");
      }
    }
    LOG.info("Sort order is \"" + sortOrder + "\"");
  }

  public void writeMessageBegin(TMessage message) throws TException {
  }

  public void writeMessageEnd() throws TException {
  }

  public void writeStructBegin(TStruct struct) throws TException {
    stackLevel++;
    if (stackLevel == 1) {
      topLevelStructFieldID = 0;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder.charAt(topLevelStructFieldID) != '-'); 
    } else {
      writeRawBytes(nonNullByte, 0, 1);
      // If the struct is null and level > 1, DynamicSerDe will call writeNull();
    }    
  }

  public void writeStructEnd() throws TException {
    stackLevel --;
  }

  public void writeFieldBegin(TField field) throws TException {
  }

  public void writeFieldEnd() throws TException {
    if (stackLevel == 1) {
      topLevelStructFieldID ++;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder.charAt(topLevelStructFieldID) != '-'); 
    }
  }

  public void writeFieldStop() {
  }

  public void writeMapBegin(TMap map) throws TException {
    stackLevel++;
    if (map == null) {
      writeRawBytes(nonNullByte, 0, 1);
    } else {
      writeI32(map.size);
    }
  }

  public void writeMapEnd() throws TException {
    stackLevel --;
  }

  public void writeListBegin(TList list) throws TException {
    stackLevel++;
    if (list == null) {
      writeRawBytes(nonNullByte, 0, 1);
    } else {
      writeI32(list.size);
    }
  }

  public void writeListEnd() throws TException {
    stackLevel --;
  }
  
  public void writeSetBegin(TSet set) throws TException {
    stackLevel++;
    if (set == null) {
      writeRawBytes(nonNullByte, 0, 1);
    } else {
      writeI32(set.size);
    }
  }

  public void writeSetEnd() throws TException {
    stackLevel --;
  }

  byte[] rawBytesBuffer;
  // This method takes care of bit-flipping for descending order
  // Declare this method as final for performance reasons
  final private void writeRawBytes(byte[] bytes, int begin, int length) throws TException {
    if (ascending) {
      trans_.write(bytes, begin, length);
    } else {
      // For fields in descending order, do a bit flip first.
      if (rawBytesBuffer == null || rawBytesBuffer.length < bytes.length) {
        rawBytesBuffer = new byte[bytes.length];
      }
      for(int i=begin; i<begin+length; i++) {
        rawBytesBuffer[i] = (byte)(~bytes[i]);
      }
      trans_.write(rawBytesBuffer, begin, length);
    }
  }
  
  private byte [] bout = new byte[1];
  public void writeBool(boolean b) throws TException {
    bout[0] = (b ? (byte)2 : (byte)1); 
    writeRawBytes(bout, 0, 1);
  }

  public void writeByte(byte b) throws TException {
    writeRawBytes(nonNullByte, 0, 1);
    // Make sure negative numbers comes before positive numbers 
    bout[0] = (byte) (b ^ 0x80); 
    writeRawBytes(bout, 0, 1);
  }

  private byte[] i16out = new byte[2];
  public void writeI16(short i16) throws TException {
    i16out[0] = (byte)(0xff & ((i16 >> 8) ^ 0x80));
    i16out[1] = (byte)(0xff & (i16));
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i16out, 0, 2);
  }

  private byte[] i32out = new byte[4];
  public void writeI32(int i32) throws TException {
    i32out[0] = (byte)(0xff & ((i32 >> 24) ^ 0x80));
    i32out[1] = (byte)(0xff & (i32 >> 16));
    i32out[2] = (byte)(0xff & (i32 >> 8));
    i32out[3] = (byte)(0xff & (i32));
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i32out, 0, 4);
  }

  private byte[] i64out = new byte[8];
  public void writeI64(long i64) throws TException {
    i64out[0] = (byte)(0xff & ((i64 >> 56) ^ 0x80));
    i64out[1] = (byte)(0xff & (i64 >> 48));
    i64out[2] = (byte)(0xff & (i64 >> 40));
    i64out[3] = (byte)(0xff & (i64 >> 32));
    i64out[4] = (byte)(0xff & (i64 >> 24));
    i64out[5] = (byte)(0xff & (i64 >> 16));
    i64out[6] = (byte)(0xff & (i64 >> 8));
    i64out[7] = (byte)(0xff & (i64));
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i64out, 0, 8);
  }

  public void writeDouble(double dub) throws TException {
    long i64 = Double.doubleToLongBits(dub);
    if ((i64 & (1L << 63)) != 0) {
      // negative numbers, flip all bits
      i64out[0] = (byte)(0xff & ((i64 >> 56) ^ 0xff));
      i64out[1] = (byte)(0xff & ((i64 >> 48) ^ 0xff));
      i64out[2] = (byte)(0xff & ((i64 >> 40) ^ 0xff));
      i64out[3] = (byte)(0xff & ((i64 >> 32) ^ 0xff));
      i64out[4] = (byte)(0xff & ((i64 >> 24) ^ 0xff));
      i64out[5] = (byte)(0xff & ((i64 >> 16) ^ 0xff));
      i64out[6] = (byte)(0xff & ((i64 >> 8) ^ 0xff));
      i64out[7] = (byte)(0xff & ((i64) ^ 0xff));
    } else {
      // positive numbers, flip just the first bit
      i64out[0] = (byte)(0xff & ((i64 >> 56) ^ 0x80));
      i64out[1] = (byte)(0xff & (i64 >> 48));
      i64out[2] = (byte)(0xff & (i64 >> 40));
      i64out[3] = (byte)(0xff & (i64 >> 32));
      i64out[4] = (byte)(0xff & (i64 >> 24));
      i64out[5] = (byte)(0xff & (i64 >> 16));
      i64out[6] = (byte)(0xff & (i64 >> 8));
      i64out[7] = (byte)(0xff & (i64));
    }
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i64out, 0, 8);
  }

  final protected byte[] nullByte = new byte[] {0};
  final protected byte[] nonNullByte = new byte[] {1};
  public void writeString(String str) throws TException {
    byte[] dat;
    try {
      dat = str.getBytes("UTF-8");
    } catch (UnsupportedEncodingException uex) {
      throw new TException("JVM DOES NOT SUPPORT UTF-8: " + uex.getMessage());
    }
    for(int i=0; i<str.length(); i++) {
      if (str.charAt(i) == '\0') {
        throw new TException( getClass().getName() + " does not support serializing strings with null bytes!");
      }
    }
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(dat, 0, dat.length);
    writeRawBytes(nullByte, 0, 1);
  }

  public void writeBinary(byte[] bin) throws TException {
    if (bin == null) {
      writeRawBytes(nullByte, 0, 1);
    } else {
      writeI32(bin.length);
      writeRawBytes(bin, 0, bin.length);
    }
  }

  public TMessage readMessageBegin() throws TException {
    return new TMessage(); 
  }

  public void readMessageEnd() throws TException {
  }

  TStruct tstruct = new TStruct();
  public TStruct readStructBegin() throws TException {
    stackLevel++;
    if (stackLevel == 1) {
      topLevelStructFieldID = 0;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder.charAt(topLevelStructFieldID) != '-'); 
    } else {
      // is this a null?
      // only read the is-null byte for level > 1 because the top-level struct can never be null.
      if (readIsNull()) return null;
    }
    return tstruct;
  }

  public void readStructEnd() throws TException {
    stackLevel --;
  }

  TField f = new TField();
  public TField readFieldBegin() throws TException {
    // slight hack to communicate to DynamicSerDe that the field ids are not being set but things are ordered.
    f.type = -1;
    return  f;
  }

  public void readFieldEnd() throws TException {
    if (stackLevel == 1) {
      topLevelStructFieldID ++;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder.charAt(topLevelStructFieldID) != '-'); 
    }
  }

  private TMap tmap = new TMap();
  /**
   * This method always return the same instance of TMap to avoid creating new instances.
   * It is the responsibility of the caller to read the value before calling this method again.
   */
  public TMap readMapBegin() throws TException {
    stackLevel ++;
    tmap.size = readI32();
    if (tmap.size == 0 && lastPrimitiveWasNull()) {
      return null;
    }
    return tmap;
  }

  public void readMapEnd() throws TException {
    stackLevel --;
  }

  private TList tlist = new TList();
  /**
   * This method always return the same instance of TList to avoid creating new instances.
   * It is the responsibility of the caller to read the value before calling this method again.
   */
  public TList readListBegin() throws TException {
    stackLevel ++;
    tlist.size = readI32();
    if (tlist.size == 0 && lastPrimitiveWasNull()) {
      return null;
    }    
    return tlist;
  }

  public void readListEnd() throws TException {
    stackLevel --;
  }

  private TSet set = new TSet();
  /**
   * This method always return the same instance of TSet to avoid creating new instances.
   * It is the responsibility of the caller to read the value before calling this method again.
   */
  public TSet readSetBegin() throws TException {
    stackLevel ++;
    set.size = readI32();
    if (set.size == 0 && lastPrimitiveWasNull()) {
      return null;
    }    
    return set;
  }

  public void readSetEnd() throws TException {
    stackLevel --;
  }
  
  // This method takes care of bit-flipping for descending order
  // Make this method final to improve performance. 
  final private int readRawAll(byte[] buf, int off, int len) throws TException {
    int bytes = trans_.readAll(buf, off, len);
    if (!ascending) {
      for(int i=off; i<off+bytes; i++) {
        buf[i] = (byte)~buf[i];
      }
    }
    return bytes;
  }

  public boolean readBool() throws TException {
    readRawAll(bin, 0, 1);
    lastPrimitiveWasNull = (bin[0] == 0);
    return lastPrimitiveWasNull ? false : bin[0] == 2;
  }

  private byte[] wasNull = new byte[1];
  final public boolean readIsNull() throws TException { 
    readRawAll(wasNull, 0, 1);
    lastPrimitiveWasNull = (wasNull[0] == 0);
    return lastPrimitiveWasNull;
  }
  
  private byte[] bin = new byte[1];
  public byte readByte() throws TException {
    if (readIsNull()) return 0;
    readRawAll(bin, 0, 1);
    return (byte)(bin[0] ^ 0x80);
  }

  private byte[] i16rd = new byte[2];
  public short readI16() throws TException {
    if (readIsNull()) return 0;
    readRawAll(i16rd, 0, 2);
    return
      (short)
      ((((i16rd[0]^0x80) & 0xff) << 8) |
       ((i16rd[1] & 0xff)));
  }

  private byte[] i32rd = new byte[4];
  public int readI32() throws TException {
    if (readIsNull()) return 0;
    readRawAll(i32rd, 0, 4);
    return
      (((i32rd[0]^0x80) & 0xff) << 24) |
      ((i32rd[1] & 0xff) << 16) |
      ((i32rd[2] & 0xff) <<  8) |
      ((i32rd[3] & 0xff));
  }

  private byte[] i64rd = new byte[8];
  public long readI64() throws TException {
    if (readIsNull()) return 0;
    readRawAll(i64rd, 0, 8);
    return
      ((long)((i64rd[0]^0x80) & 0xff) << 56) |
      ((long)(i64rd[1] & 0xff) << 48) |
      ((long)(i64rd[2] & 0xff) << 40) |
      ((long)(i64rd[3] & 0xff) << 32) |
      ((long)(i64rd[4] & 0xff) << 24) |
      ((long)(i64rd[5] & 0xff) << 16) |
      ((long)(i64rd[6] & 0xff) <<  8) |
      ((long)(i64rd[7] & 0xff));
  }

  public double readDouble() throws TException {
    if (readIsNull()) return 0;
    readRawAll(i64rd, 0, 8);
    long v = 0;
    if ((i64rd[0] & 0x80) != 0) {
      // Positive number
      v = ((long)((i64rd[0]^0x80) & 0xff) << 56) |
        ((long)(i64rd[1] & 0xff) << 48) |
        ((long)(i64rd[2] & 0xff) << 40) |
        ((long)(i64rd[3] & 0xff) << 32) |
        ((long)(i64rd[4] & 0xff) << 24) |
        ((long)(i64rd[5] & 0xff) << 16) |
        ((long)(i64rd[6] & 0xff) <<  8) |
        ((long)(i64rd[7] & 0xff));
    } else {
      // Negative number
      v = ((long)((i64rd[0]^0xff) & 0xff) << 56) |
        ((long)((i64rd[1]^0xff) & 0xff) << 48) |
        ((long)((i64rd[2]^0xff) & 0xff) << 40) |
        ((long)((i64rd[3]^0xff) & 0xff) << 32) |
        ((long)((i64rd[4]^0xff) & 0xff) << 24) |
        ((long)((i64rd[5]^0xff) & 0xff) << 16) |
        ((long)((i64rd[6]^0xff) & 0xff) <<  8) |
        ((long)((i64rd[7]^0xff) & 0xff));
    }
    return Double.longBitsToDouble(v);
  }

  private byte[] stringBytes = new byte[1000];
  public String readString() throws TException {
    if (readIsNull()) {
      return null;
    }
    int i = 0;
    while (true) {
      readRawAll(bin, 0, 1);
      if (bin[0] == 0) {
        break;
      } else {
        if (i == stringBytes.length) {
          stringBytes = Arrays.copyOf(stringBytes, stringBytes.length*2);
        }
        stringBytes[i] = bin[0];
        i++;
      }
    }
    try {
      String r = new String(stringBytes, 0, i, "UTF-8");
      return r;
    } catch (UnsupportedEncodingException uex) {
      throw new TException("JVM DOES NOT SUPPORT UTF-8: " + uex.getMessage());
    }
  }

  public byte[] readBinary() throws TException {
    int size = readI32();
    if (lastPrimitiveWasNull) return null;
    byte[] buf = new byte[size];
    readRawAll(buf, 0, size);
    return buf;
  }

  boolean lastPrimitiveWasNull;
  @Override
  public boolean lastPrimitiveWasNull() throws TException {
    return lastPrimitiveWasNull;
  }

  @Override
  public void writeNull() throws TException {
    writeRawBytes(nullByte, 0, 1);
  }
  
}
