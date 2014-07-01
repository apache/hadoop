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
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Get a listing of all files in that match the file patterns.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Ls extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Ls.class, "-ls");
    factory.addClass(Lsr.class, "-lsr");
  }
  
  public static final String NAME = "ls";
  public static final String USAGE = "[-d] [-h] [-R] [<path> ...]";
  public static final String DESCRIPTION =
		    "List the contents that match the specified file pattern. If " +
		    "path is not specified, the contents of /user/<currentUser> " +
		    "will be listed. Directory entries are of the form:\n" +
		    "\tpermissions - userId groupId sizeOfDirectory(in bytes) modificationDate(yyyy-MM-dd HH:mm) directoryName\n\n" +
		    "and file entries are of the form:\n" +
		    "\tpermissions numberOfReplicas userId groupId sizeOfFile(in bytes) modificationDate(yyyy-MM-dd HH:mm) fileName\n" +
		    "-d:  Directories are listed as plain files.\n" +
		    "-h:  Formats the sizes of files in a human-readable fashion " +
		    "rather than a number of bytes.\n" +
		    "-R:  Recursively list the contents of directories.";
		  
  

  protected static final SimpleDateFormat dateFormat = 
    new SimpleDateFormat("yyyy-MM-dd HH:mm");

  protected int maxRepl = 3, maxLen = 10, maxOwner = 0, maxGroup = 0;
  protected String lineFormat;
  protected boolean dirRecurse;

  protected boolean humanReadable = false;

  protected String formatSize(long size) {
    return humanReadable
      ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
      : String.valueOf(size);
  }

  @Override
  protected void processOptions(LinkedList<String> args)
  throws IOException {
    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "d", "h", "R");
    cf.parse(args);
    dirRecurse = !cf.getOpt("d");
    setRecursive(cf.getOpt("R") && dirRecurse);
    humanReadable = cf.getOpt("h");
    if (args.isEmpty()) args.add(Path.CUR_DIR);
  }

  @Override
  protected void processPathArgument(PathData item) throws IOException {
    // implicitly recurse once for cmdline directories
    if (dirRecurse && item.stat.isDirectory()) {
      recursePath(item);
    } else {
      super.processPathArgument(item);
    }
  }

  @Override
  protected void processPaths(PathData parent, PathData ... items)
  throws IOException {
    if (parent != null && !isRecursive() && items.length != 0) {
      out.println("Found " + items.length + " items");
    }
    adjustColumnWidths(items);
    super.processPaths(parent, items);
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    FileStatus stat = item.stat;
    String line = String.format(lineFormat,
        (stat.isDirectory() ? "d" : "-"),
        stat.getPermission() + (stat.getPermission().getAclBit() ? "+" : " "),
        (stat.isFile() ? stat.getReplication() : "-"),
        stat.getOwner(),
        stat.getGroup(),
        formatSize(stat.getLen()),
        dateFormat.format(new Date(stat.getModificationTime())),
        item
    );
    out.println(line);
  }

  /**
   * Compute column widths and rebuild the format string
   * @param items to find the max field width for each column
   */
  private void adjustColumnWidths(PathData items[]) {
    for (PathData item : items) {
      FileStatus stat = item.stat;
      maxRepl  = maxLength(maxRepl, stat.getReplication());
      maxLen   = maxLength(maxLen, stat.getLen());
      maxOwner = maxLength(maxOwner, stat.getOwner());
      maxGroup = maxLength(maxGroup, stat.getGroup());
    }

    StringBuilder fmt = new StringBuilder();
    fmt.append("%s%s"); // permission string
    fmt.append("%"  + maxRepl  + "s ");
    // Do not use '%-0s' as a formatting conversion, since it will throw a
    // a MissingFormatWidthException if it is used in String.format().
    // http://docs.oracle.com/javase/1.5.0/docs/api/java/util/Formatter.html#intFlags
    fmt.append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s");
    fmt.append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s");
    fmt.append("%"  + maxLen   + "s ");
    fmt.append("%s %s"); // mod time & path
    lineFormat = fmt.toString();
  }

  private int maxLength(int n, Object value) {
    return Math.max(n, (value != null) ? String.valueOf(value).length() : 0);
  }

  /**
   * Get a recursive listing of all files in that match the file patterns.
   * Same as "-ls -R"
   */
  public static class Lsr extends Ls {
    public static final String NAME = "lsr";

    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      args.addFirst("-R");
      super.processOptions(args);
    }
    
    @Override
    public String getReplacementCommand() {
      return "ls -R";
    }
  }
}
