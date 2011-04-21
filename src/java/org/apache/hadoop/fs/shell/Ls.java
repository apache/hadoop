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
  public static final String USAGE = "[<path> ...]";
  public static final String DESCRIPTION =
    "List the contents that match the specified file pattern. If\n" + 
    "path is not specified, the contents of /user/<currentUser>\n" +
    "will be listed. Directory entries are of the form \n" +
    "\tdirName (full path) <dir> \n" +
    "and file entries are of the form \n" + 
    "\tfileName(full path) <r n> size \n" +
    "where n is the number of replicas specified for the file \n" + 
    "and size is the size of the file, in bytes.";

  protected static final SimpleDateFormat dateFormat = 
    new SimpleDateFormat("yyyy-MM-dd HH:mm");

  protected int maxRepl = 3, maxLen = 10, maxOwner = 0, maxGroup = 0;
  protected String lineFormat;

  @Override
  protected void processOptions(LinkedList<String> args)
  throws IOException {
    CommandFormat cf = new CommandFormat(null, 0, Integer.MAX_VALUE, "R");
    cf.parse(args);
    setRecursive(cf.getOpt("R"));
    if (args.isEmpty()) args.add(Path.CUR_DIR);
  }

  @Override
  protected void processPaths(PathData parent, PathData ... items)
  throws IOException {
    // implicitly recurse once for cmdline directories
    if (parent == null && items[0].stat.isDirectory()) {
      recursePath(items[0]);
      return;
    }

    if (!isRecursive() && items.length != 0) {
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
        stat.getPermission(),
        (stat.isFile() ? stat.getReplication() : "-"),
        stat.getOwner(),
        stat.getGroup(),
        stat.getLen(),
        dateFormat.format(new Date(stat.getModificationTime())),
        item.path.toUri().getPath()
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
    fmt.append("%s%s "); // permission string
    fmt.append("%"  + maxRepl  + "s ");
    fmt.append("%-" + maxOwner + "s ");
    fmt.append("%-" + maxGroup + "s ");
    fmt.append("%"  + maxLen   + "d ");
    fmt.append("%s %s"); // mod time & path
    lineFormat = fmt.toString();
  }

  private int maxLength(int n, Object value) {
    return Math.max(n, (value != null) ? String.valueOf(value).length() : 0);
  }

  // TODO: remove when the error is commonized...
  @Override
  protected String getFnfText(Path path) {
    return "Cannot access " + path.toUri() + ": No such file or directory.";
  }

  /**
   * Get a recursive listing of all files in that match the file patterns.
   * Same as "-ls -R"
   */
  public static class Lsr extends Ls {
    public static final String NAME = "lsr";
    public static final String USAGE = Ls.USAGE;
    public static final String DESCRIPTION =
      "Recursively list the contents that match the specified\n" +
      "file pattern.  Behaves very similarly to hadoop fs -ls,\n" + 
      "except that the data is shown for all the entries in the\n" +
      "subtree.";

    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      args.addFirst("-R");
      super.processOptions(args);
    }
  }
}
