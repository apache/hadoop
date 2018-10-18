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

package org.apache.hadoop.fs.azurebfs.utils;

/**
 * Base64
 */
public final class Base64 {
  /**
   * The Base 64 Characters.
   */
  private static final String BASE_64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  /**
   * Decoded values, -1 is invalid character, -2 is = pad character.
   */
  private static final byte[] DECODE_64 = {
          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 0-15

          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /*
                                                                             * 16- 31
                                                                             */
          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, /*
                                                                             * 32- 47
                                                                             */
          52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -2, -1, -1, /*
                                                                             * 48- 63
                                                                             */
          -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, /* 64-79 */
          15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, /*
                                                                             * 80- 95
                                                                             */
          -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, /*
                                                                             * 96- 111
                                                                             */
          41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1 /*
                                                                            * 112- 127
                                                                            */
  };

  /**
   * Decodes a given Base64 string into its corresponding byte array.
   *
   * @param data
   *            the Base64 string, as a <code>String</code> object, to decode
   *
   * @return the corresponding decoded byte array
   * @throws IllegalArgumentException
   *             If the string is not a valid base64 encoded string
   */
  public static byte[] decode(final String data) {
    if (data == null) {
      throw new IllegalArgumentException("The data parameter is not a valid base64-encoded string.");
    }

    int byteArrayLength = 3 * data.length() / 4;

    if (data.endsWith("==")) {
      byteArrayLength -= 2;
    }
    else if (data.endsWith("=")) {
      byteArrayLength -= 1;
    }

    final byte[] retArray = new byte[byteArrayLength];
    int byteDex = 0;
    int charDex = 0;

    for (; charDex < data.length(); charDex += 4) {
      // get 4 chars, convert to 3 bytes
      final int char1 = DECODE_64[(byte) data.charAt(charDex)];
      final int char2 = DECODE_64[(byte) data.charAt(charDex + 1)];
      final int char3 = DECODE_64[(byte) data.charAt(charDex + 2)];
      final int char4 = DECODE_64[(byte) data.charAt(charDex + 3)];

      if (char1 < 0 || char2 < 0 || char3 == -1 || char4 == -1) {
        // invalid character(-1), or bad padding (-2)
        throw new IllegalArgumentException("The data parameter is not a valid base64-encoded string.");
      }

      int tVal = char1 << 18;
      tVal += char2 << 12;
      tVal += (char3 & 0xff) << 6;
      tVal += char4 & 0xff;

      if (char3 == -2) {
        // two "==" pad chars, check bits 12-24
        tVal &= 0x00FFF000;
        retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
      }
      else if (char4 == -2) {
        // one pad char "=" , check bits 6-24.
        tVal &= 0x00FFFFC0;
        retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
        retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);

      }
      else {
        // No pads take all 3 bytes, bits 0-24
        retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
        retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);
        retArray[byteDex++] = (byte) (tVal & 0xFF);
      }
    }
    return retArray;
  }

  /**
   * Decodes a given Base64 string into its corresponding byte array.
   *
   * @param data
   *            the Base64 string, as a <code>String</code> object, to decode
   *
   * @return the corresponding decoded byte array
   * @throws IllegalArgumentException
   *             If the string is not a valid base64 encoded string
   */
  public static Byte[] decodeAsByteObjectArray(final String data) {
    int byteArrayLength = 3 * data.length() / 4;

    if (data.endsWith("==")) {
      byteArrayLength -= 2;
    }
    else if (data.endsWith("=")) {
      byteArrayLength -= 1;
    }

    final Byte[] retArray = new Byte[byteArrayLength];
    int byteDex = 0;
    int charDex = 0;

    for (; charDex < data.length(); charDex += 4) {
      // get 4 chars, convert to 3 bytes
      final int char1 = DECODE_64[(byte) data.charAt(charDex)];
      final int char2 = DECODE_64[(byte) data.charAt(charDex + 1)];
      final int char3 = DECODE_64[(byte) data.charAt(charDex + 2)];
      final int char4 = DECODE_64[(byte) data.charAt(charDex + 3)];

      if (char1 < 0 || char2 < 0 || char3 == -1 || char4 == -1) {
        // invalid character(-1), or bad padding (-2)
        throw new IllegalArgumentException("The data parameter is not a valid base64-encoded string.");
      }

      int tVal = char1 << 18;
      tVal += char2 << 12;
      tVal += (char3 & 0xff) << 6;
      tVal += char4 & 0xff;

      if (char3 == -2) {
        // two "==" pad chars, check bits 12-24
        tVal &= 0x00FFF000;
        retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
      }
      else if (char4 == -2) {
        // one pad char "=" , check bits 6-24.
        tVal &= 0x00FFFFC0;
        retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
        retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);

      }
      else {
        // No pads take all 3 bytes, bits 0-24
        retArray[byteDex++] = (byte) (tVal >> 16 & 0xFF);
        retArray[byteDex++] = (byte) (tVal >> 8 & 0xFF);
        retArray[byteDex++] = (byte) (tVal & 0xFF);
      }
    }
    return retArray;
  }

  /**
   * Encodes a byte array as a Base64 string.
   *
   * @param data
   *            the byte array to encode
   * @return the Base64-encoded string, as a <code>String</code> object
   */
  public static String encode(final byte[] data) {
    final StringBuilder builder = new StringBuilder();
    final int dataRemainder = data.length % 3;

    int j = 0;
    int n = 0;
    for (; j < data.length; j += 3) {

      if (j < data.length - dataRemainder) {
        n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8) + (data[j + 2] & 0xFF);
      }
      else {
        if (dataRemainder == 1) {
          n = (data[j] & 0xFF) << 16;
        }
        else if (dataRemainder == 2) {
          n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8);
        }
      }

      // Left here for readability
      // byte char1 = (byte) ((n >>> 18) & 0x3F);
      // byte char2 = (byte) ((n >>> 12) & 0x3F);
      // byte char3 = (byte) ((n >>> 6) & 0x3F);
      // byte char4 = (byte) (n & 0x3F);
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 18) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 12) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 6) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) (n & 0x3F)));
    }

    final int bLength = builder.length();

    // append '=' to pad
    if (data.length % 3 == 1) {
      builder.replace(bLength - 2, bLength, "==");
    }
    else if (data.length % 3 == 2) {
      builder.replace(bLength - 1, bLength, "=");
    }

    return builder.toString();
  }

  /**
   * Encodes a byte array as a Base64 string.
   *
   * @param data
   *            the byte array to encode
   * @return the Base64-encoded string, as a <code>String</code> object
   */
  public static String encode(final Byte[] data) {
    final StringBuilder builder = new StringBuilder();
    final int dataRemainder = data.length % 3;

    int j = 0;
    int n = 0;
    for (; j < data.length; j += 3) {

      if (j < data.length - dataRemainder) {
        n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8) + (data[j + 2] & 0xFF);
      }
      else {
        if (dataRemainder == 1) {
          n = (data[j] & 0xFF) << 16;
        }
        else if (dataRemainder == 2) {
          n = ((data[j] & 0xFF) << 16) + ((data[j + 1] & 0xFF) << 8);
        }
      }

      // Left here for readability
      // byte char1 = (byte) ((n >>> 18) & 0x3F);
      // byte char2 = (byte) ((n >>> 12) & 0x3F);
      // byte char3 = (byte) ((n >>> 6) & 0x3F);
      // byte char4 = (byte) (n & 0x3F);
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 18) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 12) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) ((n >>> 6) & 0x3F)));
      builder.append(BASE_64_CHARS.charAt((byte) (n & 0x3F)));
    }

    final int bLength = builder.length();

    // append '=' to pad
    if (data.length % 3 == 1) {
      builder.replace(bLength - 2, bLength, "==");
    }
    else if (data.length % 3 == 2) {
      builder.replace(bLength - 1, bLength, "=");
    }

    return builder.toString();
  }

  /**
   * Determines whether the given string contains only Base64 characters.
   *
   * @param data
   *            the string, as a <code>String</code> object, to validate
   * @return <code>true</code> if <code>data</code> is a valid Base64 string, otherwise <code>false</code>
   */
  public static boolean validateIsBase64String(final String data) {

    if (data == null || data.length() % 4 != 0) {
      return false;
    }

    for (int m = 0; m < data.length(); m++) {
      final byte charByte = (byte) data.charAt(m);

      // pad char detected
      if (DECODE_64[charByte] == -2) {
        if (m < data.length() - 2) {
          return false;
        }
        else if (m == data.length() - 2 && DECODE_64[(byte) data.charAt(m + 1)] != -2) {
          return false;
        }
      }

      if (charByte < 0 || DECODE_64[charByte] == -1) {
        return false;
      }
    }

    return true;
  }

  /**
   * Private Default Ctor.
   */
  private Base64() {
    // No op
  }
}