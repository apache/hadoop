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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/** Base class for commands related to viewing filesystem usage, such as
 * du and df
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class FsUsage extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Df.class, "-df");
    factory.addClass(Du.class, "-du");
    factory.addClass(Dus.class, "-dus");
  }

  protected boolean humanReadable = false;
  protected TableBuilder usagesTable;
  
  protected String formatSize(long size) {
    return humanReadable
        ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
        : String.valueOf(size);
  }

  /** Show the size of a partition in the filesystem */
  public static class Df extends FsUsage {
    public static final String NAME = "df";
    public static final String USAGE = "[-h] [<path> ...]";
    public static final String DESCRIPTION =
      "Shows the capacity, free and used space of the filesystem.\n"+
      "If the filesystem has multiple partitions, and no path to a\n" +
      "particular partition is specified, then the status of the root\n" +
      "partitions will be shown.\n" +
      "  -h   Formats the sizes of files in a human-readable fashion\n" +
      "       rather than a number of bytes.\n\n";
    
    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "h");
      cf.parse(args);
      humanReadable = cf.getOpt("h");
      if (args.isEmpty()) args.add(Path.SEPARATOR);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      usagesTable = new TableBuilder(
          "Filesystem", "Size", "Used", "Available", "Use%");
      usagesTable.setRightAlign(1, 2, 3, 4);
      
      super.processArguments(args);
      if (!usagesTable.isEmpty()) {
        usagesTable.printToStream(out);
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      FsStatus fsStats = item.fs.getStatus(item.path);
      long size = fsStats.getCapacity();
      long used = fsStats.getUsed();
      long free = fsStats.getRemaining();

      usagesTable.addRow(
          item.fs.getUri(),
          formatSize(size),
          formatSize(used),
          formatSize(free),
          StringUtils.formatPercent((double)used/(double)size, 0)
      );
    }
  }

  /** show disk usage */
  public static class Du extends FsUsage {
    public static final String NAME = "du";
    public static final String USAGE = "[-s] [-h] <path> ...";
    public static final String DESCRIPTION =
    "Show the amount of space, in bytes, used by the files that\n" +
    "match the specified file pattern. The following flags are optional:\n" +
    "  -s   Rather than showing the size of each individual file that\n" +
    "       matches the pattern, shows the total (summary) size.\n" +
    "  -h   Formats the sizes of files in a human-readable fashion\n" +
    "       rather than a number of bytes.\n\n" +
    "Note that, even without the -s option, this only shows size summaries\n" +
    "one level deep into a directory.\n" +
    "The output is in the form \n" + 
    "\tsize\tname(full path)\n"; 

    protected boolean summary = false;
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "h", "s");
      cf.parse(args);
      humanReadable = cf.getOpt("h");
      summary = cf.getOpt("s");
      if (args.isEmpty()) args.add(Path.CUR_DIR);
    }

    @Override
    protected void processPathArgument(PathData item) throws IOException {
      usagesTable = new TableBuilder(2);
      // go one level deep on dirs from cmdline unless in summary mode
      if (!summary && item.stat.isDirectory()) {
        recursePath(item);
      } else {
        super.processPathArgument(item);
      }
      usagesTable.printToStream(out);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      long length;
      if (item.stat.isDirectory()) {
        length = item.fs.getContentSummary(item.path).getLength();
      } else {
        length = item.stat.getLen();
      }
      usagesTable.addRow(formatSize(length), item);
    }
  }

  /** show disk usage summary */
  public static class Dus extends Du {
    public static final String NAME = "dus";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      args.addFirst("-s");
      super.processOptions(args);
    }
    
    @Override
    public String getReplacementCommand() {
      return "du -s";
    }
  }

  /**
   * Creates a table of aligned values based on the maximum width of each
   * column as a string
   */
  private static class TableBuilder {
    protected boolean hasHeader = false;
    protected List<String[]> rows;
    protected int[] widths;
    protected boolean[] rightAlign;
    
    /**
     * Create a table w/o headers
     * @param columns number of columns
     */
    public TableBuilder(int columns) {
      rows = new ArrayList<String[]>();
      widths = new int[columns];
      rightAlign = new boolean[columns];
    }

    /**
     * Create a table with headers
     * @param headers list of headers
     */
    public TableBuilder(Object ... headers) {
      this(headers.length);
      this.addRow(headers);
      hasHeader = true;
    }

    /**
     * Change the default left-align of columns
     * @param indexes of columns to right align
     */
    public void setRightAlign(int ... indexes) {
      for (int i : indexes) rightAlign[i] = true;
    }
    
    /**
     * Add a row of objects to the table
     * @param objects the values
     */
    public void addRow(Object ... objects) {
      String[] row = new String[widths.length];
      for (int col=0; col < widths.length; col++) {
        row[col] = String.valueOf(objects[col]);
        widths[col] = Math.max(widths[col], row[col].length());
      }
      rows.add(row);
    }

    /**
     * Render the table to a stream 
     * @param out PrintStream for output
     */
    public void printToStream(PrintStream out) {
      if (isEmpty()) return;

      StringBuilder fmt = new StringBuilder();      
      for (int i=0; i < widths.length; i++) {
        if (fmt.length() != 0) fmt.append("  ");
        if (rightAlign[i]) {
          fmt.append("%"+widths[i]+"s");
        } else if (i != widths.length-1) {
          fmt.append("%-"+widths[i]+"s");
        } else {
          // prevent trailing spaces if the final column is left-aligned
          fmt.append("%s");
        }
      }

      for (Object[] row : rows) {
        out.println(String.format(fmt.toString(), row));
      }
    }
    
    /**
     * Number of rows excluding header 
     * @return rows
     */
    public int size() {
      return rows.size() - (hasHeader ? 1 : 0);
    }

    /**
     * Does table have any rows 
     * @return boolean
     */
    public boolean isEmpty() {
      return size() == 0;
    }
  }
}