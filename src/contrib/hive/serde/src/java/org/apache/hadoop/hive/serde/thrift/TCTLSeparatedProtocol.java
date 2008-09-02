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


package org.apache.hadoop.hive.serde.thrift;

import com.facebook.thrift.TException;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.*;
import com.facebook.thrift.protocol.*;
import java.util.*;
import java.io.*;


/**
 *
 * An implementation of the Thrift Protocol for ctl separated
 * records.
 * This is not thrift compliant in that it doesn't write out field ids
 * so things cannot actually be versioned.
 */
public class TCTLSeparatedProtocol extends TProtocol {

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
  final static protected byte defaultPrimarySeparatorChar_ = 1;
  final static protected byte defaultSecondarySeparatorChar_ = 2;
  final static protected byte defaultRowSeparatorChar_ = (byte)'\n';

  /**
   * The separators for this instance
   */
  final protected byte primarySeparatorChar_;
  final protected byte secondarySeparatorChar_;
  final protected byte rowSeparatorChar_;
  final protected byte primarySeparator_[];
  final protected byte secondarySeparator_[];
  final protected byte rowSeparator_[];

  /**
   * The transport stream is tokenized on the row separator
   */
  protected SimpleTransportTokenizer transportTokenizer_;

  /**
   * For a single row, the split on the primary separator
   */
  protected String columns_[];

  /**
   * An index into what column we're on
   */

  protected int index_;

  /**
   * For a single column, a split on the secondary separator
   */
  protected String fields_[];

  /**
   * An index into what field within a column we're on
   */
  protected int innerIndex_;


  /**
   * The current separator for writes
   */

  protected byte separator_;

  /**
   * Are we currently on the top-level columns or parsing a column itself
   */
  protected boolean inner_;


  /**
   * For places where the separators are back to back, should we return a null or an empty string since it is ambiguous.
   * This also applies to extra columns that are read but aren't in the current record.
   */
  final protected boolean returnNulls_;


  /**
   * A convenience class for tokenizing a TTransport
   */

  class SimpleTransportTokenizer {

    TTransport trans_;
    StringTokenizer tokenizer_;
    final String separator_;
    byte buf[];

    public SimpleTransportTokenizer(TTransport trans, byte separator, int buffer_length) {
      trans_ = trans;
      byte [] separators = new byte[1];
      separators[0] = separator;
      separator_ = new String(separators);
      buf = new byte[buffer_length];
      fillTokenizer();
    }

    private boolean fillTokenizer() {
      try {
          int length = trans_.read(buf, 0, buf.length);
          if(length <=0 ) {
            tokenizer_ = new StringTokenizer("", separator_, true);
            return false;
          }
          String row = new String(buf, 0, length);
          tokenizer_ = new StringTokenizer(row, new String(separator_), true);
        } catch(TTransportException e) {
          e.printStackTrace();
          tokenizer_ = null;
          return false;
        }
        return true;
    }

    public String nextToken() throws EOFException {
      StringBuffer ret = null;
      boolean done = false;

      while(! done) {

        if(! tokenizer_.hasMoreTokens()) {
          if(! fillTokenizer()) {
            break;
          }
        }

        try {
          final String nextToken = tokenizer_.nextToken();

          if(nextToken.equals(separator_)) {
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
    this(trans, defaultPrimarySeparatorChar_, defaultSecondarySeparatorChar_, defaultRowSeparatorChar_, false, 4096);
  }

  public TCTLSeparatedProtocol(TTransport trans, int buffer_size) {
    this(trans, defaultPrimarySeparatorChar_, defaultSecondarySeparatorChar_, defaultRowSeparatorChar_, false, buffer_size);
  }

  /**
   * @param trans - the ttransport to use as input or output
   * @param primarySeparatorChar the separator between columns (aka fields)
   * @param secondarySeparatorChar the separator within a field for things like sets and maps and lists
   * @param rowSeparatorChar - the record separator
   * @param returnNulls - whether to return a null or an empty string for fields that seem empty (ie two primary separators back to back)
   */

  public TCTLSeparatedProtocol(TTransport trans, byte primarySeparatorChar, byte secondarySeparatorChar, byte rowSeparatorChar, boolean returnNulls,
                               int buffer_length) {
    super(trans);

    returnNulls_ = returnNulls;

    primarySeparatorChar_ = primarySeparatorChar;
    secondarySeparatorChar_ = secondarySeparatorChar;
    rowSeparatorChar_ = rowSeparatorChar;

    primarySeparator_ = new byte[1];
    primarySeparator_[0] = primarySeparatorChar_;
    secondarySeparator_ = new byte[1];
    secondarySeparator_[0] = secondarySeparatorChar_;
    rowSeparator_ = new byte[1];
    rowSeparator_[0] = rowSeparatorChar_;

    transportTokenizer_ = new SimpleTransportTokenizer(trans, rowSeparatorChar, buffer_length);
  }

  public void writeMessageBegin(TMessage message) throws TException {
  }

  public void writeMessageEnd() throws TException {
  }

  public void writeStructBegin(TStruct struct) throws TException {
    // do nothing
  }

  public void writeStructEnd() throws TException {
    writeByte(rowSeparatorChar_);
  }

  public void writeFieldBegin(TField field) throws TException {
    // do nothing
  }

  public void writeFieldEnd() throws TException {
    writeByte(separator_);
  }

  public void writeFieldStop() {}

  public void writeMapBegin(TMap map) throws TException {
    if(map.keyType == TType.STRUCT ||
       map.keyType == TType.MAP ||
       map.keyType == TType.LIST ||
       map.keyType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    if(map.valueType == TType.STRUCT ||
       map.valueType == TType.MAP ||
       map.valueType == TType.LIST ||
       map.valueType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    separator_ = secondarySeparatorChar_;
  }

  public void writeMapEnd() throws TException {
    separator_ = primarySeparatorChar_;
  }

  public void writeListBegin(TList list) throws TException {
    if(list.elemType == TType.STRUCT ||
       list.elemType == TType.MAP ||
       list.elemType == TType.LIST ||
       list.elemType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    separator_ = secondarySeparatorChar_;
  }

  public void writeListEnd() throws TException {
    separator_ = primarySeparatorChar_;
  }

  public void writeSetBegin(TSet set) throws TException {
    if(set.elemType == TType.STRUCT ||
       set.elemType == TType.MAP ||
       set.elemType == TType.LIST ||
       set.elemType == TType.SET) {
      throw new TException("Not implemented: nested structures");
    }
    separator_ = secondarySeparatorChar_;
  }

  public void writeSetEnd() throws TException {
    separator_ = primarySeparatorChar_;
  }

  public void writeBool(boolean b) throws TException {
    writeString(String.valueOf(b));
  }

  public void writeByte(byte b) throws TException {
    writeString(String.valueOf(b));
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
    final byte buf[] = str.getBytes();
    trans_.write(buf, 0, buf.length);
  }

  public void writeBinary(byte[] bin) throws TException {
    throw new TException("Ctl separated protocol cannot support writing Binary data!");
  }

  public TMessage readMessageBegin() throws TException {
    return new TMessage(); // xxx check on fields in here
  }

  public void readMessageEnd() throws TException {
  }

 public TStruct readStructBegin() throws TException {
   try {
     final String tmp = transportTokenizer_.nextToken();
     columns_ = tmp.split(new String(primarySeparator_));
     index_ = 0;
     return new TStruct();
   } catch(EOFException e) {
     return null;
   }
  }

  public void readStructEnd() throws TException {
    columns_ = null;
  }

  public TField readFieldBegin() throws TException {
    if( !inner_) {
      if(index_ < columns_.length) {
        fields_ = columns_[index_++].split(new String(secondarySeparator_));
      } else {
        fields_ = null;
      }
        innerIndex_ = 0;
    }
    TField f = new TField();
    f.type = -1;
    return  f;
  }

  public void readFieldEnd() throws TException {
    fields_ = null;
  }

  public TMap readMapBegin() throws TException {
    TMap map = new TMap();
    if(fields_ != null) {
      map.size = fields_.length / 2;
    } else {
      map.size = 0;
    }
    inner_ = true;
    return map;
  }

  public void readMapEnd() throws TException {
    inner_ = false;
  }

  public TList readListBegin() throws TException {
    TList list = new TList();
    if(fields_ != null) {
      list.size = fields_.length ;
    } else {
      list.size = 0;
    }
    inner_ = true;
    return list;
  }

  public void readListEnd() throws TException {
    inner_ = false;
  }

  public TSet readSetBegin() throws TException {
    TSet set = new TSet();
    if(fields_ != null) {
      set.size = fields_.length ;
    } else {
      set.size = 0;
    }
    inner_ = true;
    return set;
  }

  public void readSetEnd() throws TException {
    inner_ = false;
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

  public String readString() throws TException {
    if(fields_ == null) {
      return returnNulls_ ? null : "";
    }
    if(innerIndex_ < fields_.length) {
      if(returnNulls_ && fields_.length == 0) {
        return null;
      } else {
        return fields_[innerIndex_++];
      }
    } else {
      // treat extra columns/strings as nulls
      return returnNulls_ ? null : "";
    }
  }

  public byte[] readBinary() throws TException {
    throw new TException("Not implemented");
  }
}
