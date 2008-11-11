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
import java.util.regex.Matcher;
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
public class TCTLSeparatedProtocol extends TProtocol 
  implements ConfigurableTProtocol, WriteNullsProtocol, SkippableTProtocol {

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
  final static protected String defaultPrimarySeparator = "\001";
  final static protected String defaultSecondarySeparator = "\002";
  final static protected String defaultRowSeparator = "\n";
  final static protected String defaultMapSeparator = "\003";

  /**
   * The separators for this instance
   */
  protected String primarySeparator;
  protected String secondarySeparator;
  protected String rowSeparator;
  protected String mapSeparator;
  protected Pattern primaryPattern;
  protected Pattern secondaryPattern;
  protected Pattern mapPattern;

  /**
   * The quote character when supporting quotes with ability to not split across quoted entries. Like csv.
   * Note that escaping the quote is not currently supported.
   */
  protected String quote;
  

  /**
   * Inspect the separators this instance is configured with.
   */
  public String getPrimarySeparator() { return primarySeparator; }
  public String getSecondarySeparator() { return secondarySeparator; }
  public String getRowSeparator() { return rowSeparator; }
  public String getMapSeparator() { return mapSeparator; }


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
   * The string representing nulls in the serialized data. e.g., \N as in mysql
   */
  protected String nullString;

  /**
   * The nullString in bytes
   */ 
  protected byte nullBuf[];


  /**
   * A convenience class for tokenizing a TTransport
   */

  class SimpleTransportTokenizer {

    TTransport trans;
    StringTokenizer tokenizer;
    final String separator;
    byte buf[];

    public SimpleTransportTokenizer(TTransport trans, String separator, int buffer_length) {
      this.trans = trans;
      this.separator = separator;
      buf = new byte[buffer_length];
      // do not fill tokenizer until user requests since filling it could read in data
      // not meant for this instantiation.
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
          tokenizer = new StringTokenizer(row, separator, true);
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

      if(tokenizer == null) {
        fillTokenizer();
      }

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
      final String theRet = ret == null ? null : ret.toString();
      return theRet;
    }
  };


  /**
   * The simple constructor which assumes ctl-a, ctl-b and '\n' separators and to return empty strings for empty fields.
   *
   * @param trans - the ttransport to use as input or output
   *
   */

  public TCTLSeparatedProtocol(TTransport trans) {
    this(trans, defaultPrimarySeparator, defaultSecondarySeparator, defaultMapSeparator, defaultRowSeparator, false, 4096);
  }

  public TCTLSeparatedProtocol(TTransport trans, int buffer_size) {
    this(trans, defaultPrimarySeparator, defaultSecondarySeparator, defaultMapSeparator, defaultRowSeparator, false, buffer_size);
  }

  /**
   * @param trans - the ttransport to use as input or output
   * @param primarySeparator the separator between columns (aka fields)
   * @param secondarySeparator the separator within a field for things like sets and maps and lists
   * @param mapSeparator - the key/value separator
   * @param rowSeparator - the record separator
   * @param returnNulls - whether to return a null or an empty string for fields that seem empty (ie two primary separators back to back)
   */

  public TCTLSeparatedProtocol(TTransport trans, String primarySeparator, String secondarySeparator, String mapSeparator, String rowSeparator, 
                               boolean returnNulls,
                               int bufferSize) {
    super(trans);

    returnNulls = returnNulls;


    this.primarySeparator = primarySeparator;
    this.secondarySeparator = secondarySeparator;
    this.rowSeparator = rowSeparator;
    this.mapSeparator = mapSeparator;

    this.innerTransport = trans;
    this.bufferSize = bufferSize;
    this.nullString  = "\\N";

    internalInitialize();
  }



  /**
   * Sets the internal separator patterns and creates the internal tokenizer.
   */
  protected void internalInitialize() {

    // in the future could allow users to specify a quote character that doesn't need escaping but for now ...
    final String primaryPatternString = 
      quote == null ? primarySeparator : 
      "(?:^|" + primarySeparator + ")(" + quote + "(?:[^"  + quote + "]+|" + quote + quote + ")*" + quote + "|[^" + primarySeparator + "]*)";

    if (quote != null) {
      stripSeparatorPrefix = Pattern.compile("^" + primarySeparator);
      stripQuotePrefix = Pattern.compile("^" + quote);
      stripQuotePostfix = Pattern.compile(quote + "$");
    }      

    primaryPattern = Pattern.compile(primaryPatternString);
    secondaryPattern = Pattern.compile(secondarySeparator);
    mapPattern = Pattern.compile(secondarySeparator + "|" + mapSeparator);
    nullBuf = nullString.getBytes();
    transportTokenizer = new SimpleTransportTokenizer(innerTransport, rowSeparator, bufferSize);
  }

  /**
   * For quoted fields, strip away the quotes and also need something to strip away the control separator when using
   * complex split method defined here.
   */
  protected Pattern stripSeparatorPrefix;
  protected Pattern stripQuotePrefix;
  protected Pattern stripQuotePostfix;


  /** 
   *
   * Split the line based on a complex regex pattern
   *
   * @param line the current row
   * @param p the pattern for matching fields in the row
   * @return List of Strings - not including the separator in them
   */
  protected String[] complexSplit(String line, Pattern p) {

    ArrayList<String> list = new ArrayList<String>();
    Matcher m = p.matcher(line);
    // For each field
    while (m.find()) {
      String match = m.group();
      if (match == null)
        break;
      if (match.length() == 0)
        match = null;
      else {
        if(stripSeparatorPrefix.matcher(match).find()) {
          match = match.substring(1);
        }
        if(stripQuotePrefix.matcher(match).find()) {
          match = match.substring(1);
        }
        if(stripQuotePostfix.matcher(match).find()) {
          match = match.substring(0,match.length() - 1);
        }
      }
      list.add(match);
    }
    return (String[])list.toArray(new String[1]);
  }



  protected String getByteValue(String altValue, String defaultVal) {
    if (altValue != null && altValue.length() > 0) {
      try {
        byte b [] = new byte[1];
        b[0] = Byte.valueOf(altValue).byteValue();
        return new String(b);
      } catch(NumberFormatException e) {
        return altValue;
      }
    }
    return defaultVal;
  }
  

  /**
   * Initialize the TProtocol
   * @param conf System properties
   * @param tbl  table properties
   * @throws TException
   */
  public void initialize(Configuration conf, Properties tbl) throws TException {


    primarySeparator = getByteValue(tbl.getProperty(Constants.FIELD_DELIM), primarySeparator);
    secondarySeparator = getByteValue(tbl.getProperty(Constants.COLLECTION_DELIM), secondarySeparator);
    rowSeparator = getByteValue(tbl.getProperty(Constants.LINE_DELIM), rowSeparator);
    mapSeparator = getByteValue(tbl.getProperty(Constants.MAPKEY_DELIM), mapSeparator);
    returnNulls = Boolean.valueOf(tbl.getProperty(ReturnNullsKey, String.valueOf(returnNulls))).booleanValue();
    bufferSize =  Integer.valueOf(tbl.getProperty(BufferSizeKey, String.valueOf(bufferSize))).intValue();
    nullString  = tbl.getProperty(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
    quote  = tbl.getProperty(Constants.QUOTE_CHAR, null);

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
      internalWriteString(primarySeparator);
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

  public void internalWriteString(String str) throws TException {
    if(str != null) {
      final byte buf[] = str.getBytes();
      trans_.write(buf, 0, buf.length);
    } else {
      trans_.write(nullBuf, 0, nullBuf.length);
    }
  }

  public void writeString(String str) throws TException {
    if(inner) {
      if(!firstInnerField) {
        // super hack city notice the mod plus only happens after firstfield hit, so == 0 is right.
        if(isMap && elemIndex++ % 2 == 0) {
          internalWriteString(mapSeparator);
        } else {
          internalWriteString(secondarySeparator);
        }
      } else {
        firstInnerField = false;
      }
    }
    if(str != null) {
      final byte buf[] = str.getBytes();
      trans_.write(buf, 0, buf.length);
    } else {
      trans_.write(nullBuf, 0, nullBuf.length);
    }
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
     columns = quote == null ? primaryPattern.split(tmp) : complexSplit(tmp, primaryPattern);
     index = 0;
     return new TStruct();
   } catch(EOFException e) {
     return null;
   }
  }

  public void readStructEnd() throws TException {
    columns = null;
  }


  /**
   * Skip past the current field
   * Just increments the field index counter.
   */
  public void skip(byte type) {
    if( inner) {
      innerIndex++;
    } else {
      index++;
    }
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
    if(columns[index] == null ||
       columns[index].equals(nullString)) {
      index++;
      if(returnNulls) {
        return null;
      }
      map.size = 0;
    } else if(columns[index].isEmpty()) {
      map.size = 0;
      index++;
    } else {
      fields = mapPattern.split(columns[index++]);
      map.size = fields.length/2;
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
    if(columns[index] == null ||
       columns[index].equals(nullString)) {
      index++;
      if(returnNulls) {
        return null;
      }
     list.size = 0;
    } else if(columns[index].isEmpty()) {
      list.size = 0;
      index++;
    } else {
      fields = secondaryPattern.split(columns[index++]);
      list.size = fields.length ;
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
    if(columns[index] == null ||
       columns[index].equals(nullString)) {
      index++;
      if(returnNulls) {
        return null;
      }
      set.size = 0;
    } else if(columns[index].isEmpty()) {
      set.size = 0;
      index++;
    } else {
      fields = secondaryPattern.split(columns[index++]);
      set.size = fields.length ;
    }
    inner = true;
    innerIndex = 0;
    return set;
  }

  protected boolean lastPrimitiveWasNullFlag;

  public boolean lastPrimitiveWasNull() throws TException {
    return lastPrimitiveWasNullFlag;
  }

  public void writeNull() throws TException {
    writeString(null);
  }

  public void readSetEnd() throws TException {
    inner = false;
  }

  public boolean readBool() throws TException {
    String val = readString();
    lastPrimitiveWasNullFlag = val == null;
    return val == null || val.isEmpty() ? false : Boolean.valueOf(val).booleanValue();
  }

  public byte readByte() throws TException {
    String val = readString();
    lastPrimitiveWasNullFlag = val == null;
    return  val == null || val.isEmpty() ? 0 : Byte.valueOf(val).byteValue();
  }

  public short readI16() throws TException {
    String val = readString();
    lastPrimitiveWasNullFlag = val == null;
    return val == null || val.isEmpty() ? 0 : Short.valueOf(val).shortValue();
  }

  public int readI32() throws TException {
    String val = readString();
    lastPrimitiveWasNullFlag = val == null;
    return val == null || val.isEmpty() ? 0 : Integer.valueOf(val).intValue();
  }

  public long readI64() throws TException {
    String val = readString();
    lastPrimitiveWasNullFlag = val == null;
    return val == null || val.isEmpty() ? 0 : Long.valueOf(val).longValue();
  }

  public double readDouble() throws TException {
    String val = readString();
    lastPrimitiveWasNullFlag = val == null;
    return val == null || val.isEmpty() ? 0 :Double.valueOf(val).doubleValue();
  }

  public String readString() throws TException {
    String ret;
    if(!inner) {
      ret = columns != null && index < columns.length ? columns[index] :  null;
      index++;
    } else {
      ret = fields != null && innerIndex < fields.length ? fields[innerIndex] : null;
      innerIndex++;
    }
    if(ret == null || ret.equals(nullString))
      return returnNulls ? null : "";
    else
      return ret;
  }

  public byte[] readBinary() throws TException {
    throw new TException("Not implemented for control separated data");
  }
}
