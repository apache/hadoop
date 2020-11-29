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
package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * The value of <code>XAttr</code> is byte[], this class is to 
 * covert byte[] to some kind of string representation or convert back.
 * String representation is convenient for display and input. For example
 * display in screen as shell response and json response, input as http
 * or shell parameter. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum XAttrCodec {
  /**
   * Value encoded as text 
   * string is enclosed in double quotes (\").
   */
  TEXT,
  
  /**
   * Value encoded as hexadecimal string 
   * is prefixed with 0x.
   */
  HEX,
  
  /**
   * Value encoded as base64 string 
   * is prefixed with 0s.
   */
  BASE64;
  
  private static final String HEX_PREFIX = "0x";
  private static final String BASE64_PREFIX = "0s";
  private static final Base64 base64 = new Base64(0);
  
  /**
   * Decode string representation of a value and check whether it's 
   * encoded. If the given string begins with 0x or 0X, it expresses
   * a hexadecimal number. If the given string begins with 0s or 0S,
   * base64 encoding is expected. If the given string is enclosed in 
   * double quotes, the inner string is treated as text. Otherwise 
   * the given string is treated as text. 
   * @param value string representation of the value.
   * @return byte[] the value
   * @throws IOException
   */
  public static byte[] decodeValue(String value) throws IOException {
    byte[] result = null;
    if (value != null) {
      if (value.length() >= 2) {
        String en = value.substring(0, 2);
        if (value.startsWith("\"") && value.endsWith("\"")) {
          value = value.substring(1, value.length()-1);
          result = value.getBytes("utf-8");
        } else if (en.equalsIgnoreCase(HEX_PREFIX)) {
          value = value.substring(2, value.length());
          try {
            result = Hex.decodeHex(value.toCharArray());
          } catch (DecoderException e) {
            throw new IOException(e);
          }
        } else if (en.equalsIgnoreCase(BASE64_PREFIX)) {
          value = value.substring(2, value.length());
          result = base64.decode(value);
        }
      }
      if (result == null) {
        result = value.getBytes("utf-8");
      }
    }
    return result;
  }
  
  /**
   * Encode byte[] value to string representation with encoding. 
   * Values encoded as text strings are enclosed in double quotes (\"), 
   * while strings encoded as hexadecimal and base64 are prefixed with 
   * 0x and 0s, respectively.
   * @param value byte[] value
   * @param encoding
   * @return String string representation of value
   * @throws IOException
   */
  public static String encodeValue(byte[] value, XAttrCodec encoding) 
      throws IOException {
    Preconditions.checkNotNull(value, "Value can not be null.");
    if (encoding == HEX) {
      return HEX_PREFIX + Hex.encodeHexString(value);
    } else if (encoding == BASE64) {
      return BASE64_PREFIX + base64.encodeToString(value);
    } else {
      return "\"" + new String(value, "utf-8") + "\"";
    }
  }
}
