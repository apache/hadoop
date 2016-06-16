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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
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

  private static final String OPTION_PATHONLY = "C";
  private static final String OPTION_DIRECTORY = "d";
  private static final String OPTION_HUMAN = "h";
  private static final String OPTION_HIDENONPRINTABLE = "q";
  private static final String OPTION_RECURSIVE = "R";
  private static final String OPTION_REVERSE = "r";
  private static final String OPTION_MTIME = "t";
  private static final String OPTION_ATIME = "u";
  private static final String OPTION_SIZE = "S";

  public static final String NAME = "ls";
  public static final String USAGE = "[-" + OPTION_PATHONLY + "] [-" +
      OPTION_DIRECTORY + "] [-" + OPTION_HUMAN + "] [-" +
      OPTION_HIDENONPRINTABLE + "] [-" + OPTION_RECURSIVE + "] [-" +
      OPTION_MTIME + "] [-" + OPTION_SIZE + "] [-" + OPTION_REVERSE + "] [-" +
      OPTION_ATIME + "] [<path> ...]";

  public static final String DESCRIPTION =
      "List the contents that match the specified file pattern. If " +
          "path is not specified, the contents of /user/<currentUser> " +
          "will be listed. For a directory a list of its direct children " +
          "is returned (unless -" + OPTION_DIRECTORY +
          " option is specified).\n\n" +
          "Directory entries are of the form:\n" +
          "\tpermissions - userId groupId sizeOfDirectory(in bytes) modificationDate(yyyy-MM-dd HH:mm) directoryName\n\n" +
          "and file entries are of the form:\n" +
          "\tpermissions numberOfReplicas userId groupId sizeOfFile(in bytes) modificationDate(yyyy-MM-dd HH:mm) fileName\n\n" +
          "  -" + OPTION_PATHONLY +
          "  Display the paths of files and directories only.\n" +
          "  -" + OPTION_DIRECTORY +
          "  Directories are listed as plain files.\n" +
          "  -" + OPTION_HUMAN +
          "  Formats the sizes of files in a human-readable fashion\n" +
          "      rather than a number of bytes.\n" +
          "  -" + OPTION_HIDENONPRINTABLE +
          "  Print ? instead of non-printable characters.\n" +
          "  -" + OPTION_RECURSIVE +
          "  Recursively list the contents of directories.\n" +
          "  -" + OPTION_MTIME +
          "  Sort files by modification time (most recent first).\n" +
          "  -" + OPTION_SIZE +
          "  Sort files by size.\n" +
          "  -" + OPTION_REVERSE +
          "  Reverse the order of the sort.\n" +
          "  -" + OPTION_ATIME +
          "  Use time of last access instead of modification for\n" +
          "      display and sorting.";

  protected final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy-MM-dd HH:mm");

  protected int maxRepl = 3, maxLen = 10, maxOwner = 0, maxGroup = 0;
  protected String lineFormat;
  private boolean pathOnly;
  protected boolean dirRecurse;
  private boolean orderReverse;
  private boolean orderTime;
  private boolean orderSize;
  private boolean useAtime;
  private Comparator<PathData> orderComparator;

  protected boolean humanReadable = false;

  /** Whether to print ? instead of non-printable characters. */
  private boolean hideNonPrintable = false;

  protected Ls() {}

  protected Ls(Configuration conf) {
    super(conf);
  }

  protected String formatSize(long size) {
    return humanReadable
      ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
      : String.valueOf(size);
  }

  @Override
  protected void processOptions(LinkedList<String> args)
  throws IOException {
    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE,
        OPTION_PATHONLY, OPTION_DIRECTORY, OPTION_HUMAN,
        OPTION_HIDENONPRINTABLE, OPTION_RECURSIVE, OPTION_REVERSE,
        OPTION_MTIME, OPTION_SIZE, OPTION_ATIME);
    cf.parse(args);
    pathOnly = cf.getOpt(OPTION_PATHONLY);
    dirRecurse = !cf.getOpt(OPTION_DIRECTORY);
    setRecursive(cf.getOpt(OPTION_RECURSIVE) && dirRecurse);
    humanReadable = cf.getOpt(OPTION_HUMAN);
    hideNonPrintable = cf.getOpt(OPTION_HIDENONPRINTABLE);
    orderReverse = cf.getOpt(OPTION_REVERSE);
    orderTime = cf.getOpt(OPTION_MTIME);
    orderSize = !orderTime && cf.getOpt(OPTION_SIZE);
    useAtime = cf.getOpt(OPTION_ATIME);
    if (args.isEmpty()) args.add(Path.CUR_DIR);

    initialiseOrderComparator();
  }

  /**
   * Should display only paths of files and directories.
   * @return true display paths only, false display all fields
   */
  @InterfaceAudience.Private
  boolean isPathOnly() {
    return this.pathOnly;
  }

  /**
   * Should the contents of the directory be shown or just the directory?
   * @return true if directory contents, false if just directory
   */
  @InterfaceAudience.Private
  boolean isDirRecurse() {
    return this.dirRecurse;
  }

  /**
   * Should file sizes be returned in human readable format rather than bytes?
   * @return true is human readable, false if bytes
   */
  @InterfaceAudience.Private
  boolean isHumanReadable() {
    return this.humanReadable;
  }

  @InterfaceAudience.Private
  private boolean isHideNonPrintable() {
    return hideNonPrintable;
  }

  /**
   * Should directory contents be displayed in reverse order
   * @return true reverse order, false default order
   */
  @InterfaceAudience.Private
  boolean isOrderReverse() {
    return this.orderReverse;
  }

  /**
   * Should directory contents be displayed in mtime order.
   * @return true mtime order, false default order
   */
  @InterfaceAudience.Private
  boolean isOrderTime() {
    return this.orderTime;
  }

  /**
   * Should directory contents be displayed in size order.
   * @return true size order, false default order
   */
  @InterfaceAudience.Private
  boolean isOrderSize() {
    return this.orderSize;
  }

  /**
   * Should access time be used rather than modification time.
   * @return true use access time, false use modification time
   */
  @InterfaceAudience.Private
  boolean isUseAtime() {
    return this.useAtime;
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
      if (!pathOnly) {
        out.println("Found " + items.length + " items");
      }
      Arrays.sort(items, getOrderComparator());
    }
    if (!pathOnly) {
      adjustColumnWidths(items);
    }
    super.processPaths(parent, items);
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    if (pathOnly) {
      out.println(item.toString());
      return;
    }
    FileStatus stat = item.stat;
    String line = String.format(lineFormat,
        (stat.isDirectory() ? "d" : "-"),
        stat.getPermission() + (stat.getPermission().getAclBit() ? "+" : " "),
        (stat.isFile() ? stat.getReplication() : "-"),
        stat.getOwner(),
        stat.getGroup(),
        formatSize(stat.getLen()),
        dateFormat.format(new Date(isUseAtime()
            ? stat.getAccessTime()
            : stat.getModificationTime())),
        isHideNonPrintable() ? new PrintableString(item.toString()) : item);
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
   * Get the comparator to be used for sorting files.
   * @return comparator
   */
  private Comparator<PathData> getOrderComparator() {
    return this.orderComparator;
  }

  /**
   * Initialise the comparator to be used for sorting files. If multiple options
   * are selected then the order is chosen in the following precedence: -
   * Modification time (or access time if requested) - File size - File name
   */
  private void initialiseOrderComparator() {
    if (isOrderTime()) {
      // mtime is ordered latest date first in line with the unix ls -t command
      this.orderComparator = new Comparator<PathData>() {
        public int compare(PathData o1, PathData o2) {
          Long o1Time = (isUseAtime() ? o1.stat.getAccessTime()
              : o1.stat.getModificationTime());
          Long o2Time = (isUseAtime() ? o2.stat.getAccessTime()
              : o2.stat.getModificationTime());
          return o2Time.compareTo(o1Time) * (isOrderReverse() ? -1 : 1);
        }
      };
    } else if (isOrderSize()) {
      // size is ordered largest first in line with the unix ls -S command
      this.orderComparator = new Comparator<PathData>() {
        public int compare(PathData o1, PathData o2) {
          Long o1Length = o1.stat.getLen();
          Long o2Length = o2.stat.getLen();
          return o2Length.compareTo(o1Length) * (isOrderReverse() ? -1 : 1);
        }
      };
    } else {
      this.orderComparator = new Comparator<PathData>() {
        public int compare(PathData o1, PathData o2) {
          return o1.compareTo(o2) * (isOrderReverse() ? -1 : 1);
        }
      };
    }
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
