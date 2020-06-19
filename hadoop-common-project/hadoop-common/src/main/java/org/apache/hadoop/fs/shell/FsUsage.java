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
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystemUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * Base class for commands related to viewing filesystem usage,
 * such as du and df.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class FsUsage extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Df.class, "-df");
    factory.addClass(Du.class, "-du");
    factory.addClass(Dus.class, "-dus");
  }

  private boolean humanReadable = false;
  private TableBuilder usagesTable;

  protected String formatSize(long size) {
    return humanReadable
        ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
        : String.valueOf(size);
  }

  public TableBuilder getUsagesTable() {
    return usagesTable;
  }

  public void setUsagesTable(TableBuilder usagesTable) {
    this.usagesTable = usagesTable;
  }

  public void setHumanReadable(boolean humanReadable) {
    this.humanReadable = humanReadable;
  }

  /** Show the size of a partition in the filesystem */
  public static class Df extends FsUsage {
    public static final String NAME = "df";
    public static final String USAGE = "[-h] [<path> ...]";
    public static final String DESCRIPTION =
      "Shows the capacity, free and used space of the filesystem. "+
      "If the filesystem has multiple partitions, and no path to a " +
      "particular partition is specified, then the status of the root " +
      "partitions will be shown.\n" +
      "-h: Formats the sizes of files in a human-readable fashion " +
      "rather than a number of bytes.";
    
    @Override
    protected void processOptions(LinkedList<String> args)
    throws IOException {
      CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "h");
      cf.parse(args);
      setHumanReadable(cf.getOpt("h"));
      if (args.isEmpty()) args.add(Path.SEPARATOR);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      setUsagesTable(new TableBuilder(
          "Filesystem", "Size", "Used", "Available", "Use%", "Mounted on"));
      getUsagesTable().setRightAlign(1, 2, 3, 4);

      super.processArguments(args);
      if (!getUsagesTable().isEmpty()) {
        getUsagesTable().printToStream(out);
      }
    }

    /**
     * Add a new row to the usages table for the given FileSystem URI.
     *
     * @param uri - FileSystem URI
     * @param fsStatus - FileSystem status
     * @param mountedOnPath - FileSystem mounted on path
     */
    private void addToUsagesTable(URI uri, FsStatus fsStatus,
        String mountedOnPath) {
      long size = fsStatus.getCapacity();
      long used = fsStatus.getUsed();
      long free = fsStatus.getRemaining();
      getUsagesTable().addRow(
          uri,
          formatSize(size),
          formatSize(used),
          formatSize(free),
          StringUtils.formatPercent((double) used / (double) size, 0),
          mountedOnPath
      );
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (ViewFileSystemUtil.isViewFileSystem(item.fs)
          || ViewFileSystemUtil.isViewFileSystemOverloadScheme(item.fs)) {
        ViewFileSystem viewFileSystem = (ViewFileSystem) item.fs;
        Map<ViewFileSystem.MountPoint, FsStatus>  fsStatusMap =
            ViewFileSystemUtil.getStatus(viewFileSystem, item.path);

        for (Map.Entry<ViewFileSystem.MountPoint, FsStatus> entry :
            fsStatusMap.entrySet()) {
          ViewFileSystem.MountPoint viewFsMountPoint = entry.getKey();
          FsStatus fsStatus = entry.getValue();

          // Add the viewfs mount point status to report
          URI[] mountPointFileSystemURIs =
              viewFsMountPoint.getTargetFileSystemURIs();
          // Since LinkMerge is not supported yet, we
          // should ideally see mountPointFileSystemURIs
          // array with only one element.
          addToUsagesTable(mountPointFileSystemURIs[0],
              fsStatus, viewFsMountPoint.getMountedOnPath().toString());
        }
      } else {
        // Hide the columns specific to ViewFileSystem
        getUsagesTable().setColumnHide(5, true);
        FsStatus fsStatus = item.fs.getStatus(item.path);
        addToUsagesTable(item.fs.getUri(), fsStatus, "/");
      }
    }

  }

  /** show disk usage */
  public static class Du extends FsUsage {
    public static final String NAME = "du";
    public static final String USAGE = "[-s] [-h] [-v] [-x] <path> ...";
    public static final String DESCRIPTION =
        "Show the amount of space, in bytes, used by the files that match " +
            "the specified file pattern. The following flags are optional:\n" +
            "-s: Rather than showing the size of each individual file that" +
            " matches the pattern, shows the total (summary) size.\n" +
            "-h: Formats the sizes of files in a human-readable fashion" +
            " rather than a number of bytes.\n" +
            "-v: option displays a header line.\n" +
            "-x: Excludes snapshots from being counted.\n\n" +
            "Note that, even without the -s option, this only shows size " +
            "summaries one level deep into a directory.\n\n" +
            "The output is in the form \n" +
            "\tsize\tdisk space consumed\tname(full path)\n";

    protected boolean summary = false;
    private boolean showHeaderLine = false;
    private boolean excludeSnapshots = false;
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "h", "s", "v", "x");
      cf.parse(args);
      setHumanReadable(cf.getOpt("h"));
      summary = cf.getOpt("s");
      showHeaderLine = cf.getOpt("v");
      excludeSnapshots = cf.getOpt("x");
      if (args.isEmpty()) args.add(Path.CUR_DIR);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
        throws IOException {
      if (showHeaderLine) {
        setUsagesTable(new TableBuilder("SIZE",
            "DISK_SPACE_CONSUMED_WITH_ALL_REPLICAS", "FULL_PATH_NAME"));
      } else {
        setUsagesTable(new TableBuilder(3));
      }
      super.processArguments(args);
      if (!getUsagesTable().isEmpty()) {
        getUsagesTable().printToStream(out);
      }
    }

    @Override
    protected void processPathArgument(PathData item) throws IOException {
      // go one level deep on dirs from cmdline unless in summary mode
      if (!summary && item.stat.isDirectory()) {
        recursePath(item);
      } else {
        super.processPathArgument(item);
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      ContentSummary contentSummary = item.fs.getContentSummary(item.path);
      long length = contentSummary.getLength();
      long spaceConsumed = contentSummary.getSpaceConsumed();
      if (excludeSnapshots) {
        length -= contentSummary.getSnapshotLength();
        spaceConsumed -= contentSummary.getSnapshotSpaceConsumed();
      }
      getUsagesTable().addRow(formatSize(length),
          formatSize(spaceConsumed), item);
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
    private boolean[] hide;
    
    /**
     * Create a table w/o headers
     * @param columns number of columns
     */
    public TableBuilder(int columns) {
      rows = new ArrayList<String[]>();
      widths = new int[columns];
      rightAlign = new boolean[columns];
      hide = new boolean[columns];
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
     * Hide the given column index
     */
    public void setColumnHide(int columnIndex, boolean hideCol) {
      hide[columnIndex] = hideCol;
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
     * Render the table to a stream.
     * @param out PrintStream for output
     */
    public void printToStream(PrintStream out) {
      if (isEmpty()) return;

      StringBuilder fmt = new StringBuilder();      
      for (int i=0; i < widths.length; i++) {
        if (hide[i]) {
          continue;
        }
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