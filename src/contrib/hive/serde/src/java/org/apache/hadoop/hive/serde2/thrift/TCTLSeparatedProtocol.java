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
import org.apache.hadoop.hive.serde.Constants;
import com.facebook.thrift.TException;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.*;
import com.facebook.thrift.protocol.*;
import java.util.*;
import java.util.regex.Pattern;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import java.util.Properties;

/**
 *
 * An implementation of the Thrift Protocol for ctl separated
 * records.
 * This is not thrift compliant in that it doesn't write out field ids
 * so things cannot actually be versioned.
 */
public class TCTLSeparatedProtocol extends TProtocol implements ConfigurableTProtocol {

  final static Log LOG = LogFactory.getLog(TCTLSeparatedProtocol.class.getName());
  
  /**
   * Factory for JSON protocol objects
   */
  public static class Factory implements TProtocolFactory {

    public TProtocol getProtocol(TTransport trans) {
      return new TCTLSeparatedProtocol(trans);
    }

  }

  /**
   * These are defaults, but for now leaving them like this
   */
  final static protected byte defaultPrimarySeparatorByte = 1;
  final static protected byte defaultSecondarySeparatorByte = 2;
  final static protected byte defaultRowSeparatorByte = (byte)'\n';
  final static protected byte defaultMapSeparatorByte = 3;

  /**
   * The separators for this instance
   */
  protected byte primarySeparatorByte;
  protected byte secondarySeparatorByte;
  protected byte rowSeparatorByte;
  protected byte mapSeparatorByte;
  protected Pattern primaryPattern;
  protected Pattern secondaryPattern;
  protected Pattern mapPattern;

  /**
   * Inspect the separators this instance is configured with.
   */
  public byte getPrimarySeparator() { return primarySeparatorByte; }
  public byte getSecondarySeparator() { return secondarySeparatorByte; }
  public byte getRowSeparator() { return rowSeparatorByte; }
  public byte getMapSeparator() { return mapSeparatorByte; }


  /**
   * The transport stream is tokenized on the row separator
   */
  protected SimpleTransportTokenizer transportTokenizer;

  /**
   * For a single row, the split on the primary separator
   */
  protected String columns[];

  /**
   * An index into what column we're on
   */

  protected int index;

  /**
   * For a single column, a split on the secondary separator
   */
  protected String fields[];

  /**
   * An index into what field within a column we're on
   */
  protected int innerIndex;


  /**
   * Is this the first field we're writing
   */
  protected boolean firstField;

  /**
   * Is this the first list/map/set field we're writing for the current element
   */
  protected boolean firstInnerField;


  /**
   * Are we writing a map and need to worry about k/v separator?
   */
  protected boolean isMap;


  /**
   * For writes, on what element are we on so we know when to use normal list separator or 
   * for a map know when to use the k/v separator
   */
  protected long elemIndex;


  /**
   * Are we currently on the top-level columns or parsing a column itself
   */
  protected boolean inner;


  /**
   * For places where the separators are back to back, should we return a null or an empty string since it is ambiguous.
   * This also applies to extra columns that are read but aren't in the current record.
   */
  protected boolean returnNulls;

  /**
   * The transport being wrapped.
   *
   */
  final protected TTransport innerTransport;


  /**
   * Strings used to lookup the various configurable paramaters of this protocol.
   */
  public final static String ReturnNullsKey = "separators.return_nulls";
  public final static String BufferSizeKey = "separators.buffer_size";

  /**
   * The size of the internal buffer to use.
   */
  protected int bufferSize;

  /**
   * A convenience class for tokenizing a TTransport
   */

  class SimpleTransportTokenizer {

    TTransport trans;
    StringTokenizer tokenizer;
    final String separator;
    byte buf[];

    public SimpleTransportTokenizer(TTransport trans, byte separator, int buffer_length) {
      this.trans = trans;
      byte [] separators = new byte[1];
      separators[0] = separator;
      this.separator = new String(separators);
      buf = new byte[buffer_length];
      fillTokenizer();
    }

    private boolean fillTokenizer() {
      try {
          int length = trans.read(buf, 0, buf.length);
          if(length <=0 ) {
            tokenizer = new StringTokenizer("", separator, true);
            return false;
          }
          String row = new String(buf, 0, length);
          tokenizer = new StringTokenizer(row, new String(separator), true);
        } catch(TTransportException e) {
          e.printStackTrace();
          tokenizer = null;
          return false;
        }
        return true;
    }

    public String nextToken() throws EOFException {
      StringBuffer ret = null;
      boolean done = false;

      while(! done) {

        if(! tokenizer.hasMoreTokens()) {
          if(! fillTokenizer()) {
            break;
          }
        }

        try {
          final String nextToken = tokenizer.nextToken();

          if(nextToken.equals(separator)) {
            done = true;
          } else if(ret == null) {
            ret = new StringBuffer(nextToken);
          } else {
            ret.append(nextToken);
          }
        } catch(NoSuchElementException e) {
          if (ret == null) {
            throw new EOFException(e.getMessage());
          }
          done = true;
        }
      } // while ! done
      return ret == null ? null : ret.toString();
    }
  };


  /**
   * The simple constructor which assumes ctl-a, ctl-b and '\n' separators and to return empty strings for empty fields.
   *
   * @param trans - the ttransport to use as input or output
   *
   */

  public TCTLSeparatedProtocol(TTransport trans) {
    this(trans, defaultPrimarySeparatorByte, defaultSecondarySeparatorByte, defaultMapSeparatorByte, defaultRowSeparatorByte, false, 4096);
  }

  public TCTLSeparatedProtocol(TTransport trans, int buffer_size) {
    this(trans, defaultPrimarySeparatorByte, defaultSecondarySeparatorByte, defaultMapSeparatorByte, defaultRowSeparatorByte, false, buffer_size);
  }

  /**
   * @param trans - the ttransport to use as input or output
   * @param primarySeparatorByte the separator between columns (aka fields)
   * @param secondarySeparatorByte the separator within a field for things like sets and maps and lists
   * @param mapSeparatorByte - the key/value separator
   * @param rowSeparatorByte - the record separator
   * @param returnNulls - whether to return a null or an empty string for fields that seem empty (ie two primary separators back to back)
   */

  public TCTLSeparatedProtocol(TTransport trans, byte primarySeparatorByte, byte secondarySeparatorByte, byte mapSeparatorByte, byte rowSeparatorByte, 
                               boolean returnNulls,
                               int bufferSize) {
    super(trans);

    returnNulls = returnNulls;


    this.primarySeparatorByte = primarySeparatorByte;
    this.secondarySeparatorByte = secondarySeparatorByte;
    this.rowSeparatorByte = rowSeparatorByte;
    this.mapSeparatorByte = mapSeparatorByte;

    this.innerTransport = trans;
    this.bufferSize = bufferSize;

    internalInitialize();
  }


  /**
   * Sets the internal separator patterns and creates the internal tokenizer.
   */
  protected void internalInitialize() {
    byte []primarySeparator = new byte[1];
    byte []secondarySeparator = new byte[1];
    primarySeparator[0] = primarySeparatorByte;
    secondarySeparator[0] = secondarySeparatorByte;

    primaryPattern = Pattern.compile(new String(primarySeparator));
    secondaryPattern = Pattern.compile(new String(secondarySeparator));
    mapPattern = Pattern.compile("\\0" + secondarySeparatorByte + "|\\0" + mapSeparatorByte);

    transportTokenizer = new SimpleTransportTokenizer(innerTransport, rowSeparatorByte, bufferSize);
  }

  /**
   * Initialize the TProtocol
   * @param conf System properties
   * @param tbl  table properties
   * @throws TException
   */
  public void initialize(Configuration conf, Properties tbl) throws TException {
    primarySeparatorByte = Byte.valueOf(tbl.getProperty(Constants.FIELD_DELIM, String.valueOf(primarySeparatorByte))).byteValue();
    LOG.debug("collections delim=<" + tbl.getProperty(Constants.COLLECTION_DELIM) + ">" );
    secondarySeparatorByte = Byte.valueOf(tbl.getProperty(Constants.COLLECTION_DELIM, String.valueOf(secondarySeparatorByte))).byteValue();
    rowSeparatorByte = Byte.valueOf(tbl.getProperty(Constants.LINE_DELIM, String.valueOf(rowSeparatorByte))).byteValue();
    mapSeparatorByte = Byte.valueOf(tbl.getProperty(Constants.MAPKEY_DELIM, String.valueOf(mapSeparatorByte))).byteValue();
    returnNulls = Boolean.valueOf(tbl.getProperty(ReturnNullsKey, String.valueOf(returnNulls))).booleanValue();
    bufferSize =  Integer.valueOf(tbl.getProperty(BufferSizeKey, String.valueOf(bufferSize))).intValue();

    internalInitialize();

  }

  public void writeMessageBegin(TMessage message) throws TException {
  }

  public void writeMessageEnd() throws TException {
  }

  public void writeStructBegin(TStruct struct) throws TException {
    firstField = true;
  }

  public void writeStructEnd() throws TException {
    // We don't write rowSeparatorByte because that should be handled by file format.
  }

  public void writeFieldBegin(TField field) throws TException {
    if(! firstField) {
      writeByte(primarySeparatorByte);
    }
    firstField = false;
  }

  public void writeFieldEnd() throws TException {
  }

  public void writeFieldStop() {
  }

  public void writeMapBegin(TMap map) throws TException {
    // nesting not allowed!
    if(map.keyType == TType.STRUCT ||
       map.keyType == TType.MAP ||
       map.keyType == TType.LIST ||
       map.keyType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    // nesting not allowed!
    if(map.valueType == TType.STRUCT ||
       map.valueType == TType.MAP ||
       map.valueType == TType.LIST ||
       map.valueType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }

    firstInnerField = true;
    isMap = true;
    inner = true;
    elemIndex = 0;
  }

  public void writeMapEnd() throws TException {
    isMap = false;
    inner = false;
  }

  public void writeListBegin(TList list) throws TException {
    if(list.elemType == TType.STRUCT ||
       list.elemType == TType.MAP ||
       list.elemType == TType.LIST ||
       list.elemType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    firstInnerField = true;
    inner = true;
  }

  public void writeListEnd() throws TException {
    inner = false;
  }
  
  public void writeSetBegin(TSet set) throws TException {
    if(set.elemType == TType.STRUCT ||
       set.elemType == TType.MAP ||
       set.elemType == TType.LIST ||
       set.elemType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    firstInnerField = true;
    inner = true;
  }

  public void writeSetEnd() throws TException {
    inner = false;
  }

  public void writeBool(boolean b) throws TException {
    writeString(String.valueOf(b));
  }

  // for writing out single byte
  private byte buf[] = new byte[1];
  public void writeByte(byte b) throws TException {
    buf[0] = b;
    trans_.write(buf);
  }

  public void writeI16(short i16) throws TException {
    writeString(String.valueOf(i16));
  }

  public void writeI32(int i32) throws TException {
    writeString(String.valueOf(i32));
  }

  public void writeI64(long i64) throws TException {
    writeString(String.valueOf(i64));
  }

  public void writeDouble(double dub) throws TException {
    writeString(String.valueOf(dub));
  }

  public void writeString(String str) throws TException {
    if(inner) {
      if(!firstInnerField) {
        // super hack city notice the mod plus only happens after firstfield hit, so == 0 is right.
        if(isMap && elemIndex++ % 2 == 0) {
          writeByte(mapSeparatorByte);
        } else {
          writeByte(secondarySeparatorByte);
        }
      } else {
        firstInnerField = false;
      }
    }
    final byte buf[] = str.getBytes();
    trans_.write(buf, 0, buf.length);
  }

  public void writeBinary(byte[] bin) throws TException {
    throw new TException("Ctl separated protocol cannot support writing Binary data!");
  }

  public TMessage readMessageBegin() throws TException {
    return new TMessage(); 
  }

  public void readMessageEnd() throws TException {
  }

 public TStruct readStructBegin() throws TException {
   assert(!inner);
   try {
     final String tmp = transportTokenizer.nextToken();
     columns = primaryPattern.split(tmp);
     index = 0;
     return new TStruct();
   } catch(EOFException e) {
     return null;
   }
  }

  public void readStructEnd() throws TException {
    columns = null;
  }

  public TField readFieldBegin() throws TException {
    assert( !inner);
    TField f = new TField();
    // slight hack to communicate to DynamicSerDe that the field ids are not being set but things are ordered.
    f.type = -1;
    return  f;
  }

  public void readFieldEnd() throws TException {
    fields = null;
  }

  public TMap readMapBegin() throws TException {
    assert( !inner);
    TMap map = new TMap();
    fields = mapPattern.split(columns[index++]);
    if(fields != null) {
      map.size = fields.length/2;
    } else {
      map.size = 0;
    }
    innerIndex = 0;
    inner = true;
    isMap = true;
    return map;
  }

  public void readMapEnd() throws TException {
    inner = false;
    isMap = false;
  }

  public TList readListBegin() throws TException {
    assert( !inner);
    TList list = new TList();
    fields = secondaryPattern.split(columns[index++]);
    if(fields != null) {
      list.size = fields.length ;
    } else {
      list.size = 0;
    }
    innerIndex = 0;
    inner = true;
    return list;
  }

  public void readListEnd() throws TException {
    inner = false;
  }

  public TSet readSetBegin() throws TException {
    assert( !inner);
    TSet set = new TSet();
    fields = secondaryPattern.split(columns[index++]);
    if(fields != null) {
      set.size = fields.length ;
    } else {
      set.size = 0;
    }
    inner = true;
    innerIndex = 0;
    return set;
  }

  public void readSetEnd() throws TException {
    inner = false;
  }
  public boolean readBool() throws TException {
    return Boolean.valueOf(readString()).booleanValue();
  }

  public byte readByte() throws TException {
    return Byte.valueOf(readString()).byteValue();
  }

  public short readI16() throws TException {
    return Short.valueOf(readString()).shortValue();
  }

  public int readI32() throws TException {
    return Integer.valueOf(readString()).intValue();
  }

  public long readI64() throws TException {
    return Long.valueOf(readString()).longValue();
  }

  public double readDouble() throws TException {
    return Double.valueOf(readString()).doubleValue();
  }

  protected String [] curMapPair;
  public String readString() throws TException {
    String ret;
    if(!inner) {
      ret = columns != null && index < columns.length ? columns[index++] : null;
    } else {
      ret = fields != null && innerIndex < fields.length ? fields[innerIndex++] : null;
    }
    return ret == null && ! returnNulls ? "" : ret;
  }

  public byte[] readBinary() throws TException {
    throw new TException("Not implemented for control separated data");
  }
}
