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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.TimeZone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * Print statistics about path in specified format.
 * Format sequences:
 *   %b: Size of file in blocks
 *   %g: Group name of owner
 *   %n: Filename
 *   %o: Block size
 *   %r: replication
 *   %u: User name of owner
 *   %y: UTC date as &quot;yyyy-MM-dd HH:mm:ss&quot;
 *   %Y: Milliseconds since January 1, 1970 UTC
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Stat extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Stat.class, "-stat");
  }

  public static final String NAME = "stat";
  public static final String USAGE = "[format] <path> ...";
  public static final String DESCRIPTION =
    "Print statistics about the file/directory at <path> " +
    "in the specified format. Format accepts filesize in blocks (%b), group name of owner(%g), " +
    "filename (%n), block size (%o), replication (%r), user name of owner(%u), modification date (%y, %Y)\n";

  protected static final SimpleDateFormat timeFmt;
  static {
    timeFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    timeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  // default format string
  protected String format = "%y";

  @Override
  protected void processOptions(LinkedList<String> args) throws IOException {
    CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "R");
    cf.parse(args);
    setRecursive(cf.getOpt("R"));
    if (args.getFirst().contains("%")) format = args.removeFirst();
    cf.parse(args); // make sure there's still at least one arg
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    FileStatus stat = item.stat;
    StringBuilder buf = new StringBuilder();

    char[] fmt = format.toCharArray();
    for (int i = 0; i < fmt.length; ++i) {
      if (fmt[i] != '%') {
        buf.append(fmt[i]);
      } else {
        // this silently drops a trailing %?
        if (i + 1 == fmt.length) break;
        switch (fmt[++i]) {
          case 'b':
            buf.append(stat.getLen());
            break;
          case 'F':
            buf.append(stat.isDirectory()
                ? "directory" 
                : (stat.isFile() ? "regular file" : "symlink"));
            break;
          case 'g':
            buf.append(stat.getGroup());
            break;
          case 'n':
            buf.append(item.path.getName());
            break;
          case 'o':
            buf.append(stat.getBlockSize());
            break;
          case 'r':
            buf.append(stat.getReplication());
            break;
          case 'u':
            buf.append(stat.getOwner());
            break;
          case 'y':
            buf.append(timeFmt.format(new Date(stat.getModificationTime())));
            break;
          case 'Y':
            buf.append(stat.getModificationTime());
            break;
          default:
            // this leaves %<unknown> alone, which causes the potential for
            // future format options to break strings; should use %% to
            // escape percents
            buf.append(fmt[i]);
            break;
        }
      }
    }
    out.println(buf.toString());
  }
}
