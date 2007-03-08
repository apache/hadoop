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

package org.apache.hadoop.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Various utility functions for Hadooop record I/O runtime.
 * @author Milind Bhandarkar
 */
public class Utils {
  
  /** Cannot create a new instance of Utils */
  private Utils() {
  }
  
  public static final char[] hexchars = { '0', '1', '2', '3', '4', '5',
  '6', '7', '8', '9', 'A', 'B',
  'C', 'D', 'E', 'F' };
  /**
   *
   * @param s
   * @return
   */
  static String toXMLString(String s) {
    StringBuffer sb = new StringBuffer();
    for (int idx = 0; idx < s.length(); idx++) {
      char ch = s.charAt(idx);
      if (ch == '<') {
        sb.append("&lt;");
      } else if (ch == '&') {
        sb.append("&amp;");
      } else if (ch == '%') {
        sb.append("%25");
      } else if (ch < 0x20) {
        sb.append("%");
        sb.append(hexchars[ch/16]);
        sb.append(hexchars[ch%16]);
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }
  
  static private int h2c(char ch) {
    if (ch >= '0' && ch <= '9') {
      return ch - '0';
    } else if (ch >= 'A' && ch <= 'F') {
      return ch - 'A';
    } else if (ch >= 'a' && ch <= 'f') {
      return ch - 'a';
    }
    return 0;
  }
  
  /**
   *
   * @param s
   * @return
   */
  static String fromXMLString(String s) {
    StringBuffer sb = new StringBuffer();
    for (int idx = 0; idx < s.length();) {
      char ch = s.charAt(idx++);
      if (ch == '%') {
        char ch1 = s.charAt(idx++);
        char ch2 = s.charAt(idx++);
        char res = (char)(h2c(ch1)*16 + h2c(ch2));
        sb.append(res);
      } else {
        sb.append(ch);
      }
    }
    
    return sb.toString();
  }
  
  /**
   *
   * @param s
   * @return
   */
  static String toCSVString(String s) {
    StringBuffer sb = new StringBuffer(s.length()+1);
    sb.append('\'');
    int len = s.length();
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      switch(c) {
        case '\0':
          sb.append("%00");
          break;
        case '\n':
          sb.append("%0A");
          break;
        case '\r':
          sb.append("%0D");
          break;
        case ',':
          sb.append("%2C");
          break;
        case '}':
          sb.append("%7D");
          break;
        case '%':
          sb.append("%25");
          break;
        default:
          sb.append(c);
      }
    }
    return sb.toString();
  }
  
  /**
   *
   * @param s
   * @throws java.io.IOException
   * @return
   */
  static String fromCSVString(String s) throws IOException {
    if (s.charAt(0) != '\'') {
      throw new IOException("Error deserializing string.");
    }
    int len = s.length();
    StringBuffer sb = new StringBuffer(len-1);
    for (int i = 1; i < len; i++) {
      char c = s.charAt(i);
      if (c == '%') {
        char ch1 = s.charAt(i+1);
        char ch2 = s.charAt(i+2);
        i += 2;
        if (ch1 == '0' && ch2 == '0') {
          sb.append('\0');
        } else if (ch1 == '0' && ch2 == 'A') {
          sb.append('\n');
        } else if (ch1 == '0' && ch2 == 'D') {
          sb.append('\r');
        } else if (ch1 == '2' && ch2 == 'C') {
          sb.append(',');
        } else if (ch1 == '7' && ch2 == 'D') {
          sb.append('}');
        } else if (ch1 == '2' && ch2 == '5') {
          sb.append('%');
        } else {
          throw new IOException("Error deserializing string.");
        }
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
  
  /**
   *
   * @param s
   * @return
   */
  static String toXMLBuffer(Buffer s) {
    return s.toString();
  }
  
  /**
   *
   * @param s
   * @throws java.io.IOException
   * @return
   */
  static Buffer fromXMLBuffer(String s)
  throws IOException {
    if (s.length() == 0) { return new Buffer(); }
    int blen = s.length()/2;
    byte[] barr = new byte[blen];
    for (int idx = 0; idx < blen; idx++) {
      char c1 = s.charAt(2*idx);
      char c2 = s.charAt(2*idx+1);
      barr[idx] = (byte)Integer.parseInt(""+c1+c2, 16);
    }
    return new Buffer(barr);
  }
  
  /**
   *
   * @param buf
   * @return
   */
  static String toCSVBuffer(Buffer buf) {
    StringBuffer sb = new StringBuffer("#");
    sb.append(buf.toString());
    return sb.toString();
  }
  
  /**
   * Converts a CSV-serialized representation of buffer to a new
   * Buffer
   * @param s CSV-serialized representation of buffer
   * @throws java.io.IOException
   * @return Deserialized Buffer
   */
  static Buffer fromCSVBuffer(String s)
  throws IOException {
    if (s.charAt(0) != '#') {
      throw new IOException("Error deserializing buffer.");
    }
    if (s.length() == 1) { return new Buffer(); }
    int blen = (s.length()-1)/2;
    byte[] barr = new byte[blen];
    for (int idx = 0; idx < blen; idx++) {
      char c1 = s.charAt(2*idx+1);
      char c2 = s.charAt(2*idx+2);
      barr[idx] = (byte)Integer.parseInt(""+c1+c2, 16);
    }
    return new Buffer(barr);
  }
  
  /** Parse a float from a byte array. */
  public static float readFloat(byte[] bytes, int start) {
    return WritableComparator.readFloat(bytes, start);
  }
  
  /** Parse a double from a byte array. */
  public static double readDouble(byte[] bytes, int start) {
    return WritableComparator.readDouble(bytes, start);
  }
  
  /**
   * Reads a zero-compressed encoded long from a byte array and returns it.
   * @param bytes byte array with decode long
   * @param start starting index
   * @throws java.io.IOException
   * @return deserialized long
   */
  public static long readVLong(byte[] bytes, int start) throws IOException {
    return WritableComparator.readVLong(bytes, start);
  }
  
  /**
   * Reads a zero-compressed encoded integer from a byte array and returns it.
   * @param bytes byte array with the encoded integer
   * @param start start index
   * @throws java.io.IOException
   * @return deserialized integer
   */
  public static int readVInt(byte[] bytes, int start) throws IOException {
    return WritableComparator.readVInt(bytes, start);
  }
  
  /**
   * Reads a zero-compressed encoded long from a stream and return it.
   * @param in input stream
   * @throws java.io.IOException
   * @return deserialized long
   */
  public static long readVLong(DataInput in) throws IOException {
    return WritableUtils.readVLong(in);
  }
  
  /**
   * Reads a zero-compressed encoded integer from a stream and returns it.
   * @param in input stream
   * @throws java.io.IOException
   * @return deserialized integer
   */
  public static int readVInt(DataInput in) throws IOException {
    return WritableUtils.readVInt(in);
  }
  
  /**
   * Get the encoded length if an integer is stored in a variable-length format
   * @return the encoded length
   */
  public static int getVIntSize(long i) {
    return WritableUtils.getVIntSize(i);
  }
  
  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param stream Binary output stream
   * @param i Long to be serialized
   * @throws java.io.IOException
   */
  public static void writeVLong(DataOutput stream, long i) throws IOException {
    WritableUtils.writeVLong(stream, i);
  }
  
  /**
   * Serializes an int to a binary stream with zero-compressed encoding.
   *
   * @param stream Binary output stream
   * @param i int to be serialized
   * @throws java.io.IOException
   */
  public static void writeVInt(DataOutput stream, int i) throws IOException {
    WritableUtils.writeVInt(stream, i);
  }
  
  /** Lexicographic order of binary data. */
  public static int compareBytes(byte[] b1, int s1, int l1,
      byte[] b2, int s2, int l2) {
    return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
  }
}
