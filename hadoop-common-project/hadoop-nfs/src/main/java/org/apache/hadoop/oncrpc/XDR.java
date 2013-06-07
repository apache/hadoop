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
package org.apache.hadoop.oncrpc;

import java.io.PrintStream;
import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class for building XDR messages based on RFC 4506.
 * <p>
 * This class maintains a buffer into which java types are written as
 * XDR types for building XDR messages. Similarly this class can
 * be used to get java types from an XDR request or response.
 * <p>
 * Currently only a subset of XDR types defined in RFC 4506 are supported.
 */
public class XDR {
  private final static  String HEXES = "0123456789abcdef";
  
  /** Internal buffer for reading or writing to */
  private byte[] bytearr;
  
  /** Place to read from or write to */
  private int cursor;

  public XDR() {
    this(new byte[0]);
  }

  public XDR(byte[] data) {
    bytearr = Arrays.copyOf(data, data.length);
    cursor = 0;
  }

  /**
   * @param bytes bytes to be appended to internal buffer
   */
  private void append(byte[] bytesToAdd) {
    bytearr = append(bytearr, bytesToAdd);
  }

  public int size() {
    return bytearr.length;
  }

  /** Skip some bytes by moving the cursor */
  public void skip(int size) {
    cursor += size;
  }

  /**
   * Write Java primitive integer as XDR signed integer.
   * 
   * Definition of XDR signed integer from RFC 4506:
   * <pre>
   * An XDR signed integer is a 32-bit datum that encodes an integer in
   * the range [-2147483648,2147483647].  The integer is represented in
   * two's complement notation.  The most and least significant bytes are
   * 0 and 3, respectively.  Integers are declared as follows:
   * 
   *       int identifier;
   * 
   *            (MSB)                   (LSB)
   *          +-------+-------+-------+-------+
   *          |byte 0 |byte 1 |byte 2 |byte 3 |                      INTEGER
   *          +-------+-------+-------+-------+
   *          <------------32 bits------------>
   * </pre>
   */
  public void writeInt(int data) {
    append(toBytes(data));
  }

  /**
   * Read an XDR signed integer and return as Java primitive integer.
   */
  public int readInt() {
    byte byte0 = bytearr[cursor++];
    byte byte1 = bytearr[cursor++];
    byte byte2 = bytearr[cursor++];
    byte byte3 = bytearr[cursor++];
    return (XDR.toShort(byte0) << 24) + (XDR.toShort(byte1) << 16)
        + (XDR.toShort(byte2) << 8) + XDR.toShort(byte3);
  }

  /**
   * Write Java primitive boolean as an XDR boolean.
   * 
   * Definition of XDR boolean from RFC 4506:
   * <pre>
   *    Booleans are important enough and occur frequently enough to warrant
   *    their own explicit type in the standard.  Booleans are declared as
   *    follows:
   * 
   *          bool identifier;
   * 
   *    This is equivalent to:
   * 
   *          enum { FALSE = 0, TRUE = 1 } identifier;
   * </pre>
   */
  public void writeBoolean(boolean data) {
    this.writeInt(data ? 1 : 0);
  }

  /**
   * Read an XDR boolean and return as Java primitive boolean.
   */
  public boolean readBoolean() {
    return readInt() == 0 ? false : true;
  }

  /**
   * Write Java primitive long to an XDR signed long.
   * 
   * Definition of XDR signed long from RFC 4506:
   * <pre>
   *    The standard also defines 64-bit (8-byte) numbers called hyper
   *    integers and unsigned hyper integers.  Their representations are the
   *    obvious extensions of integer and unsigned integer defined above.
   *    They are represented in two's complement notation.The most and
   *    least significant bytes are 0 and 7, respectively. Their
   *    declarations:
   * 
   *    hyper identifier; unsigned hyper identifier;
   * 
   *         (MSB)                                                   (LSB)
   *       +-------+-------+-------+-------+-------+-------+-------+-------+
   *       |byte 0 |byte 1 |byte 2 |byte 3 |byte 4 |byte 5 |byte 6 |byte 7 |
   *       +-------+-------+-------+-------+-------+-------+-------+-------+
   *       <----------------------------64 bits---------------------------->
   *                                                  HYPER INTEGER
   *                                                  UNSIGNED HYPER INTEGER
   * </pre>
   */
  public void writeLongAsHyper(long data) {
       byte byte0 = (byte) ((data & 0xff00000000000000l) >> 56);
    byte byte1 = (byte) ((data & 0x00ff000000000000l) >> 48);
    byte byte2 = (byte) ((data & 0x0000ff0000000000l) >> 40);
    byte byte3 = (byte) ((data & 0x000000ff00000000l) >> 32);
    byte byte4 = (byte) ((data & 0x00000000ff000000l) >> 24);
    byte byte5 = (byte) ((data & 0x0000000000ff0000l) >> 16);
    byte byte6 = (byte) ((data & 0x000000000000ff00l) >> 8);
    byte byte7 = (byte) ((data & 0x00000000000000ffl));
    this.append(new byte[] { byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7 });
  }

  /**
   * Read XDR signed hyper and return as java primitive long.
   */
  public long readHyper() {
    byte byte0 = bytearr[cursor++];
    byte byte1 = bytearr[cursor++];
    byte byte2 = bytearr[cursor++];
    byte byte3 = bytearr[cursor++];
    byte byte4 = bytearr[cursor++];
    byte byte5 = bytearr[cursor++];
    byte byte6 = bytearr[cursor++];
    byte byte7 = bytearr[cursor++];
    return ((long) XDR.toShort(byte0) << 56)
        + ((long) XDR.toShort(byte1) << 48) + ((long) XDR.toShort(byte2) << 40)
        + ((long) XDR.toShort(byte3) << 32) + ((long) XDR.toShort(byte4) << 24)
        + ((long) XDR.toShort(byte5) << 16) + ((long) XDR.toShort(byte6) << 8)
        + XDR.toShort(byte7);
  }

  /**
   * Write a Java primitive byte array to XDR fixed-length opaque data.
   * 
   * Defintion of fixed-length opaque data from RFC 4506:
   * <pre>
   *    At times, fixed-length uninterpreted data needs to be passed among
   *    machines.  This data is called "opaque" and is declared as follows:
   * 
   *          opaque identifier[n];
   * 
   *    where the constant n is the (static) number of bytes necessary to
   *    contain the opaque data.  If n is not a multiple of four, then the n
   *    bytes are followed by enough (0 to 3) residual zero bytes, r, to make
   *    the total byte count of the opaque object a multiple of four.
   * 
   *           0        1     ...
   *       +--------+--------+...+--------+--------+...+--------+
   *       | byte 0 | byte 1 |...|byte n-1|    0   |...|    0   |
   *       +--------+--------+...+--------+--------+...+--------+
   *       |<-----------n bytes---------->|<------r bytes------>|
   *       |<-----------n+r (where (n+r) mod 4 = 0)------------>|
   *                                                    FIXED-LENGTH OPAQUE
   * </pre>
   */
  public void writeFixedOpaque(byte[] data) {
    writeFixedOpaque(data, data.length);
  }

  public void writeFixedOpaque(byte[] data, int length) {
    append(Arrays.copyOf(data, length + XDR.pad(length, 4)));
  }

  public byte[] readFixedOpaque(int size) {
    byte[] ret = new byte[size];
    for(int i = 0; i < size; i++) {
      ret[i] = bytearr[cursor];
      cursor++;
    }

    for(int i = 0; i < XDR.pad(size, 4); i++) {
      cursor++;
    }
    return ret;
  }

  /**
   * Write a Java primitive byte array as XDR variable-length opque data.
   * 
   * Definition of XDR variable-length opaque data RFC 4506:
   * 
   * <pre>
   *    The standard also provides for variable-length (counted) opaque data,
   *    defined as a sequence of n (numbered 0 through n-1) arbitrary bytes
   *    to be the number n encoded as an unsigned integer (as described
   *    below), and followed by the n bytes of the sequence.
   * 
   *    Byte m of the sequence always precedes byte m+1 of the sequence, and
   *    byte 0 of the sequence always follows the sequence's length (count).
   *    If n is not a multiple of four, then the n bytes are followed by
   *    enough (0 to 3) residual zero bytes, r, to make the total byte count
   *    a multiple of four.  Variable-length opaque data is declared in the
   *    following way:
   * 
   *          opaque identifier<m>;
   *       or
   *          opaque identifier<>;
   * 
   *    The constant m denotes an upper bound of the number of bytes that the
   *    sequence may contain.  If m is not specified, as in the second
   *    declaration, it is assumed to be (2**32) - 1, the maximum length.
   * 
   *    The constant m would normally be found in a protocol specification.
   *    For example, a filing protocol may state that the maximum data
   *    transfer size is 8192 bytes, as follows:
   * 
   *          opaque filedata<8192>;
   * 
   *             0     1     2     3     4     5   ...
   *          +-----+-----+-----+-----+-----+-----+...+-----+-----+...+-----+
   *          |        length n       |byte0|byte1|...| n-1 |  0  |...|  0  |
   *          +-----+-----+-----+-----+-----+-----+...+-----+-----+...+-----+
   *          |<-------4 bytes------->|<------n bytes------>|<---r bytes--->|
   *                                  |<----n+r (where (n+r) mod 4 = 0)---->|
   *                                                   VARIABLE-LENGTH OPAQUE
   * 
   *    It is an error to encode a length greater than the maximum described
   *    in the specification.
   * </pre>
   */
  public void writeVariableOpaque(byte[] data) {
    this.writeInt(data.length);
    this.writeFixedOpaque(data);
  }

  public byte[] readVariableOpaque() {
    int size = this.readInt();
    return size != 0 ? this.readFixedOpaque(size) : null;
  }

  public void skipVariableOpaque() {
    int length= this.readInt();
    this.skip(length+XDR.pad(length, 4));
  }
  
  /**
   * Write Java String as XDR string.
   * 
   * Definition of XDR string from RFC 4506:
   * 
   * <pre>
   *    The standard defines a string of n (numbered 0 through n-1) ASCII
   *    bytes to be the number n encoded as an unsigned integer (as described
   *    above), and followed by the n bytes of the string.  Byte m of the
   *    string always precedes byte m+1 of the string, and byte 0 of the
   *    string always follows the string's length.  If n is not a multiple of
   *    four, then the n bytes are followed by enough (0 to 3) residual zero
   *    bytes, r, to make the total byte count a multiple of four.  Counted
   *    byte strings are declared as follows:
   * 
   *          string object<m>;
   *       or
   *          string object<>;
   * 
   *    The constant m denotes an upper bound of the number of bytes that a
   *    string may contain.  If m is not specified, as in the second
   *    declaration, it is assumed to be (2**32) - 1, the maximum length.
   *    The constant m would normally be found in a protocol specification.
   *    For example, a filing protocol may state that a file name can be no
   *    longer than 255 bytes, as follows:
   * 
   *          string filename<255>;
   * 
   *             0     1     2     3     4     5   ...
   *          +-----+-----+-----+-----+-----+-----+...+-----+-----+...+-----+
   *          |        length n       |byte0|byte1|...| n-1 |  0  |...|  0  |
   *          +-----+-----+-----+-----+-----+-----+...+-----+-----+...+-----+
   *          |<-------4 bytes------->|<------n bytes------>|<---r bytes--->|
   *                                  |<----n+r (where (n+r) mod 4 = 0)---->|
   *                                                                   STRING
   *    It is an error to encode a length greater than the maximum described
   *    in the specification.
   * </pre>
   */
  public void writeString(String data) {
    this.writeVariableOpaque(data.getBytes());
  }

  public String readString() {
    return new String(this.readVariableOpaque());
  }

  public void dump(PrintStream out) {
    for(int i = 0; i < bytearr.length; i += 4) {
      out.println(hex(bytearr[i]) + " " + hex(bytearr[i + 1]) + " "
          + hex(bytearr[i + 2]) + " " + hex(bytearr[i + 3]));
    }
  }

  @VisibleForTesting
  public byte[] getBytes() {
    return Arrays.copyOf(bytearr, bytearr.length);
  }

  public static byte[] append(byte[] bytes, byte[] bytesToAdd) {
    byte[] newByteArray = new byte[bytes.length + bytesToAdd.length];
    System.arraycopy(bytes, 0, newByteArray, 0, bytes.length);
    System.arraycopy(bytesToAdd, 0, newByteArray, bytes.length, bytesToAdd.length);
    return newByteArray;
  }

  private static int pad(int x, int y) {
    return x % y == 0 ? 0 : y - (x % y);
  }

  static byte[] toBytes(int n) {
    byte[] ret = { (byte) ((n & 0xff000000) >> 24),
        (byte) ((n & 0x00ff0000) >> 16), (byte) ((n & 0x0000ff00) >> 8),
        (byte) (n & 0x000000ff) };
    return ret;
  }

  private static short toShort(byte b) {
    return b < 0 ? (short) (b + 256): (short) b;
  }

  private static String hex(byte b) {
    return "" + HEXES.charAt((b & 0xF0) >> 4) + HEXES.charAt((b & 0x0F));
  }

  private static byte[] recordMark(int size, boolean last) {
    return toBytes(!last ? size : size | 0x80000000);
  }

  public static byte[] getVariableOpque(byte[] data) {
    byte[] bytes = toBytes(data.length);
    return append(bytes, Arrays.copyOf(data, data.length + XDR.pad(data.length, 4)));
  }

  public static int fragmentSize(byte[] mark) {
    int n = (XDR.toShort(mark[0]) << 24) + (XDR.toShort(mark[1]) << 16)
        + (XDR.toShort(mark[2]) << 8) + XDR.toShort(mark[3]);
    return n & 0x7fffffff;
  }

  public static boolean isLastFragment(byte[] mark) {
    int n = (XDR.toShort(mark[0]) << 24) + (XDR.toShort(mark[1]) << 16)
        + (XDR.toShort(mark[2]) << 8) + XDR.toShort(mark[3]);
    return (n & 0x80000000) != 0;
  }

  /** check if the rest of data has more than <len> bytes */
  public static boolean verifyLength(XDR xdr, int len) {
    return (xdr.bytearr.length - xdr.cursor) >= len;
  }

  /** Write an XDR message to a TCP ChannelBuffer */
  public static ChannelBuffer writeMessageTcp(XDR request, boolean last) {
    byte[] fragmentHeader = XDR.recordMark(request.bytearr.length, last);
    ChannelBuffer outBuf = ChannelBuffers.buffer(fragmentHeader.length
        + request.bytearr.length);
    outBuf.writeBytes(fragmentHeader);
    outBuf.writeBytes(request.bytearr);
    return outBuf;
  }

  /** Write an XDR message to a UDP ChannelBuffer */
  public static ChannelBuffer writeMessageUdp(XDR response) {
    ChannelBuffer outBuf = ChannelBuffers.buffer(response.bytearr.length);
    outBuf.writeBytes(response.bytearr);
    return outBuf;
  }
}
