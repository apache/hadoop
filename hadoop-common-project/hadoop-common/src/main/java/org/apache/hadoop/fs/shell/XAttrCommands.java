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
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Enums;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * XAttr related operations
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class XAttrCommands extends FsCommand {
  private static final String GET_FATTR = "getfattr";
  private static final String SET_FATTR = "setfattr";

  public static void registerCommands(CommandFactory factory) {
    factory.addClass(GetfattrCommand.class, "-" + GET_FATTR);
    factory.addClass(SetfattrCommand.class, "-" + SET_FATTR);
  }

  private static enum ENCODE {
    TEXT,
    HEX,
    BASE64;
  }

  private static final String ENCODE_HEX = "0x";
  private static final String ENCODE_BASE64 = "0s";

  private static String convert(String name, byte[] value, ENCODE encode)
      throws IOException {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(name);
    if (value != null && value.length != 0) {
      buffer.append("=");
      if (encode == ENCODE.TEXT) {
        buffer.append("\"").append(new String(value, "utf-8")).append("\"");
      } else if (encode == ENCODE.HEX) {
        buffer.append(ENCODE_HEX).append(Hex.encodeHexString(value));
      } else if (encode == ENCODE.BASE64) {
        buffer.append(ENCODE_BASE64).append(Base64.encodeBase64String(value));
      }
    }
    return buffer.toString();
  }

  private static byte[] convert(String valueArg) throws IOException {
    String value = valueArg;
    byte[] result = null;
    if (value != null) {
      if (value.length() >= 2) {
        final String en = value.substring(0, 2);
        if (value.startsWith("\"") && value.endsWith("\"")) {
          value = value.substring(1, value.length()-1);
          result = value.getBytes("utf-8");
        } else if (en.equalsIgnoreCase(ENCODE_HEX)) {
          value = value.substring(2, value.length());
          try {
            result = Hex.decodeHex(value.toCharArray());
          } catch (DecoderException e) {
            throw new IOException(e);
          }
        } else if (en.equalsIgnoreCase(ENCODE_BASE64)) {
          value = value.substring(2, value.length());
          result = Base64.decodeBase64(value);
        }
      }
      if (result == null) {
        result = value.getBytes("utf-8");
      }
    }
    return result;
  }

  /**
   * Implements the '-getfattr' command for the FsShell.
   */
  public static class GetfattrCommand extends FsCommand {
    public static final String NAME = GET_FATTR;
    public static final String USAGE = "[-R] {-n name | -d} [-e en] <path>";
    public static final String DESCRIPTION =
      "Displays the extended attribute names and values (if any) for a " +
      "file or directory.\n" +
      "-R: Recurisively list the attributes for all files and directories.\n" +
      "-n name: Dump the named extended attribute value.\n" + 
      "-d: Dump all extended attribute values associated with " +
      "pathname.\n" +
      "-e <encoding>: Encode values after retrieving them. Valid encodings " +
      "are \"text\", \"hex\", and \"base64\". Values encoded as text " +
      "strings are enclosed in double quotes (\"), and values encoded" +
      " as hexadecimal and base64 are prefixed with 0x and 0s, respectively.\n" +
      "<path>: The file or directory.\n";
    private static final String NAME_OPT = "-n";
    private static final String ENCODE_OPT = "-e";
    final CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "d", "R");

    private String name = null;
    private boolean dump = false;
    private ENCODE encode = ENCODE.TEXT;

    private final static Function<String, ENCODE> encodeValueOfFunc =
      Enums.valueOfFunction(ENCODE.class);

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      name = StringUtils.popOptionWithArgument(NAME_OPT, args);
      String en = StringUtils.popOptionWithArgument(ENCODE_OPT, args);
      if (en != null) {
        encode = encodeValueOfFunc.apply(en.toUpperCase());
      }
      Preconditions.checkArgument(encode != null,
        "Invalid/unsupported encoding option specified: " + en);

      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      dump = cf.getOpt("d");

      if (!dump && name == null) {
        throw new HadoopIllegalArgumentException(
            "Must specify '-n name' or '-d' option.");
      }

      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing.");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments.");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      out.println("# file: " + item);
      if (dump) {
        Map<String, byte[]> xattrs = item.fs.getXAttrs(item.path);
        if (xattrs != null) {
          Iterator<Entry<String, byte[]>> iter = xattrs.entrySet().iterator();
          while(iter.hasNext()) {
            Entry<String, byte[]> entry = iter.next();
            out.println(convert(entry.getKey(), entry.getValue(), encode));
          }
        }
      } else {
        byte[] value = item.fs.getXAttr(item.path, name);
        if (value != null) {
          out.println(convert(name, value, encode));
        }
      }
    }
  }

  /**
   * Implements the '-setfattr' command for the FsShell.
   */
  public static class SetfattrCommand extends FsCommand {
    public static final String NAME = SET_FATTR;
    public static final String USAGE = "{-n name [-v value] | -x name} <path>";
    public static final String DESCRIPTION =
      "Sets an extended attribute name and value for a file or directory.\n" +
      "-n name: The extended attribute name.\n" +
      "-v value: The extended attribute value. There are three different\n" +
      "encoding methods for the value. If the argument is enclosed in double\n" +
      "quotes, then the value is the string inside the quotes. If the\n" +
      "argument is prefixed with 0x or 0X, then it is taken as a hexadecimal\n" +
      "number. If the argument begins with 0s or 0S, then it is taken as a\n" +
      "base64 encoding.\n" +
      "-x name: Remove the extended attribute.\n" +
      "<path>: The file or directory.\n";
    private static final String NAME_OPT = "-n";
    private static final String VALUE_OPT = "-v";
    private static final String REMOVE_OPT = "-x";

    private String name = null;
    private byte[] value = null;
    private String xname = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      name = StringUtils.popOptionWithArgument(NAME_OPT, args);
      String v = StringUtils.popOptionWithArgument(VALUE_OPT, args);
      if (v != null) {
        value = convert(v);
      }
      xname = StringUtils.popOptionWithArgument(REMOVE_OPT, args);

      if (name != null && xname != null) {
        throw new HadoopIllegalArgumentException(
            "Can not specify both '-n name' and '-x name' option.");
      }
      if (name == null && xname == null) {
        throw new HadoopIllegalArgumentException(
            "Must specify '-n name' or '-x name' option.");
      }

      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing.");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments.");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (name != null) {
        item.fs.setXAttr(item.path, name, value);
      } else if (xname != null) {
        item.fs.removeXAttr(item.path, xname);
      }
    }
  }
}
