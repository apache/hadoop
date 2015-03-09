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
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Enums;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.XAttrCodec;
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

  /**
   * Implements the '-getfattr' command for the FsShell.
   */
  public static class GetfattrCommand extends FsCommand {
    public static final String NAME = GET_FATTR;
    public static final String USAGE = "[-R] {-n name | -d} [-e en] <path>";
    public static final String DESCRIPTION =
      "Displays the extended attribute names and values (if any) for a " +
      "file or directory.\n" +
      "-R: Recursively list the attributes for all files and directories.\n" +
      "-n name: Dump the named extended attribute value.\n" +
      "-d: Dump all extended attribute values associated with pathname.\n" +
      "-e <encoding>: Encode values after retrieving them." +
      "Valid encodings are \"text\", \"hex\", and \"base64\". " +
      "Values encoded as text strings are enclosed in double quotes (\")," +
      " and values encoded as hexadecimal and base64 are prefixed with " +
      "0x and 0s, respectively.\n" +
      "<path>: The file or directory.\n";
    private final static Function<String, XAttrCodec> enValueOfFunc =
        Enums.valueOfFunction(XAttrCodec.class);

    private String name = null;
    private boolean dump = false;
    private XAttrCodec encoding = XAttrCodec.TEXT;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      name = StringUtils.popOptionWithArgument("-n", args);
      String en = StringUtils.popOptionWithArgument("-e", args);
      if (en != null) {
        try {
          encoding = enValueOfFunc.apply(StringUtils.toUpperCase(en));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Invalid/unsupported encoding option specified: " + en);
        }
        Preconditions.checkArgument(encoding != null,
            "Invalid/unsupported encoding option specified: " + en);
      }

      boolean r = StringUtils.popOption("-R", args);
      setRecursive(r);
      dump = StringUtils.popOption("-d", args);

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
            printXAttr(entry.getKey(), entry.getValue());
          }
        }
      } else {
        byte[] value = item.fs.getXAttr(item.path, name);
        printXAttr(name, value);
      }
    }
    
    private void printXAttr(String name, byte[] value) throws IOException{
      if (value != null) {
        if (value.length != 0) {
          out.println(name + "=" + XAttrCodec.encodeValue(value, encoding));
        } else {
          out.println(name);
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
      "-v value: The extended attribute value. There are three different " +
      "encoding methods for the value. If the argument is enclosed in double " +
      "quotes, then the value is the string inside the quotes. If the " +
      "argument is prefixed with 0x or 0X, then it is taken as a hexadecimal " +
      "number. If the argument begins with 0s or 0S, then it is taken as a " +
      "base64 encoding.\n" +
      "-x name: Remove the extended attribute.\n" +
      "<path>: The file or directory.\n";

    private String name = null;
    private byte[] value = null;
    private String xname = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      name = StringUtils.popOptionWithArgument("-n", args);
      String v = StringUtils.popOptionWithArgument("-v", args);
      if (v != null) {
        value = XAttrCodec.decodeValue(v);
      }
      xname = StringUtils.popOptionWithArgument("-x", args);

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
