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

package org.apache.hadoop.runc.squashfs.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class BinUtils {

  private static final String NL = String.format("%n");

  private BinUtils() {
  }

  public enum DumpOptions {
    UNSIGNED,
    DECIMAL,
    OCTAL,
    BINARY,
    UNIX_TIMESTAMP;
  }

  public static void dumpBin(
      StringBuilder buf,
      int width,
      String name,
      byte value,
      DumpOptions... options) {
    name(buf, width, name);
    value(buf, value, 8, options);
    buf.append(NL);
  }

  public static void dumpBin(
      StringBuilder buf,
      int width,
      String name,
      short value,
      DumpOptions... options) {
    name(buf, width, name);
    value(buf, value, 16, options);
    buf.append(NL);
  }

  public static void dumpBin(
      StringBuilder buf,
      int width,
      String name,
      int value,
      DumpOptions... options) {
    name(buf, width, name);
    value(buf, value, 32, options);
    buf.append(NL);
  }

  public static void dumpBin(
      StringBuilder buf,
      int width,
      String name,
      long value,
      DumpOptions... options) {
    name(buf, width, name);
    value(buf, value, 64, options);
    buf.append(NL);
  }

  public static void dumpBin(
      StringBuilder buf,
      int width,
      String name,
      String value) {
    name(buf, width, name);
    buf.append("                  ");
    buf.append(value);
    buf.append(NL);
  }

  public static void dumpBin(
      StringBuilder buf,
      int width,
      String name,
      byte[] data,
      int offset,
      int length,
      int bytesPerLine, int bytesPerGroup) {
    name(buf, width, name);
    String padding = spaces(width + 7);
    for (int i = 0; i < length; i += bytesPerLine) {
      if (i > 0) {
        buf.append(padding);
      }
      buf.append(String.format("        %08x  ", i));
      for (int j = 0; j < bytesPerLine; j++) {
        if (j > 0 && j % bytesPerGroup == 0) {
          buf.append(" ");
        }
        if (i + j < length) {
          byte b = data[offset + i + j];
          buf.append(String.format("%02x", b & 0xFF));
        } else {
          // spaces
          buf.append("  ");
        }
      }
      buf.append("  ");
      for (int j = 0; j < bytesPerLine; j++) {
        if (i + j < length) {
          char c = (char) (data[offset + i + j] & 0xff);
          if (c >= 32 && c <= 127) {
            buf.append(c);
          } else {
            buf.append(".");
          }
        } else {
          // space
          buf.append(" ");
        }
      }
      buf.append(NL);
    }

  }

  private static String spaces(int length) {
    String pattern = String.format("%%%ds", length);
    return String.format(pattern, "");
  }

  private static void name(StringBuilder buf, int width, String name) {
    String pattern = String.format("%%%ds:  ", width < 1 ? 1 : width);
    buf.append(String.format(pattern, name));
  }

  private static boolean has(DumpOptions value, DumpOptions[] options) {
    for (DumpOptions option : options) {
      if (option == value) {
        return true;
      }
    }
    return false;
  }

  private static void value(
      StringBuilder buf,
      Number num,
      int bits,
      DumpOptions... options) {
    long raw = num.longValue();
    boolean unsigned = has(DumpOptions.UNSIGNED, options);
    boolean decimal = has(DumpOptions.DECIMAL, options);
    boolean binary = has(DumpOptions.BINARY, options);
    boolean octal = has(DumpOptions.OCTAL, options);
    boolean unixTimestamp = has(DumpOptions.UNIX_TIMESTAMP, options);

    if (bits == 8) {
      buf.append(
          String.format("              %02x", unsigned ? (raw & 0xFFL) : raw));
      if (decimal) {
        buf.append(String.format("  %d", unsigned ? (raw & 0xFFL) : raw));
      }
      if (octal) {
        buf.append(String.format("  0%o", unsigned ? (raw & 0xFFL) : raw));
      }
      if (binary) {
        String value = "00000000" + Long.toBinaryString(raw & 0xFFL);
        buf.append(String.format("  %s", value.substring(value.length() - 8)));
      }
    } else if (bits == 16) {
      buf.append(
          String.format("            %04x", unsigned ? (raw & 0xFFFFL) : raw));
      if (decimal) {
        buf.append(String.format("  %d", unsigned ? (raw & 0xFFFFL) : raw));
      }
      if (octal) {
        buf.append(String.format("  0%o", unsigned ? (raw & 0xFFFFL) : raw));
      }
      if (binary) {
        String value = "0000000000000000" + Long.toBinaryString(raw & 0xFFFFL);
        buf.append(String.format("  %s", value.substring(value.length() - 16)));
      }
    } else if (bits == 32) {
      buf.append(
          String.format("        %08x", unsigned ? (raw & 0xFFFFFFFFL) : raw));
      if (decimal) {
        buf.append(String.format("  %d", unsigned ? (raw & 0xFFFFFFFFL) : raw));
      }
      if (octal) {
        buf.append(
            String.format("  0%o", unsigned ? (raw & 0xFFFFFFFFL) : raw));
      }
      if (binary) {
        String value = "00000000000000000000000000000000" + Long
            .toBinaryString(raw & 0xFFFFFFFFL);
        buf.append(String.format("  %s", value.substring(value.length() - 32)));
      }
      if (unixTimestamp) {
        buf.append("  ");
        Date date = new Date(raw * 1000L);
        buf.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
      }
    } else if (bits == 64) {
      buf.append(
          String.format("%016x", unsigned ? (raw & 0xFFFFFFFFFFFFFFFFL) : raw));
      if (decimal) {
        buf.append(String
            .format("  %d", unsigned ? (raw & 0xFFFFFFFFFFFFFFFFL) : raw));
      }
      if (octal) {
        buf.append(String
            .format("  0%o", unsigned ? (raw & 0xFFFFFFFFFFFFFFFFL) : raw));
      }
      if (binary) {
        String value =
            "0000000000000000000000000000000000000000000000000000000000000000"
                + Long.toBinaryString(raw);
        buf.append(String.format("  %s", value.substring(value.length() - 64)));
      }
      if (unixTimestamp) {
        buf.append("  ");
        Date date = new Date(raw);
        buf.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
      }
    }
  }

}
