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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Unix touch like commands
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class TouchCommands extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Touchz.class, "-touchz");
    factory.addClass(Touch.class, "-touch");
  }

  /**
   * (Re)create zero-length file at the specified path.
   * This will be replaced by a more UNIX-like touch when files may be
   * modified.
   */
  public static class Touchz extends TouchCommands {
    public static final String NAME = "touchz";
    public static final String USAGE = "<path> ...";
    public static final String DESCRIPTION =
      "Creates a file of zero length " +
      "at <path> with current time as the timestamp of that <path>. " +
      "An error is returned if the file exists with non-zero length\n";

    @Override
    protected void processOptions(LinkedList<String> args) {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE);
      cf.parse(args);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        // TODO: handle this
        throw new PathIsDirectoryException(item.toString());
      }
      if (item.stat.getLen() != 0) {
        throw new PathIOException(item.toString(), "Not a zero-length file");
      }
      touchz(item);
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      if (!item.parentExists()) {
        throw new PathNotFoundException(item.toString())
            .withFullyQualifiedPath(item.path.toUri().toString());
      }
      touchz(item);
    }

    private void touchz(PathData item) throws IOException {
      item.fs.create(item.path).close();
    }
  }

  /**
   * A UNIX like touch command.
   */
  public static class Touch extends TouchCommands {
    private static final String OPTION_CHANGE_ONLY_MODIFICATION_TIME = "m";
    private static final String OPTION_CHANGE_ONLY_ACCESS_TIME = "a";
    private static final String OPTION_USE_TIMESTAMP = "t";
    private static final String OPTION_DO_NOT_CREATE_FILE = "c";

    public static final String NAME = "touch";
    public static final String USAGE = "[-" + OPTION_CHANGE_ONLY_ACCESS_TIME
        + "] [-" + OPTION_CHANGE_ONLY_MODIFICATION_TIME + "] [-"
        + OPTION_USE_TIMESTAMP + " TIMESTAMP (yyyyMMdd:HHmmss) ] "
        + "[-" + OPTION_DO_NOT_CREATE_FILE + "] <path> ...";
    public static final String DESCRIPTION =
        "Updates the access and modification times of the file specified by the"
            + " <path> to the current time. If the file does not exist, then a zero"
            + " length file is created at <path> with current time as the timestamp"
            + " of that <path>.\n"
            + "-" + OPTION_CHANGE_ONLY_ACCESS_TIME
            + " Change only the access time \n" + "-"
            + OPTION_CHANGE_ONLY_MODIFICATION_TIME
            + " Change only the modification time \n" + "-"
            + OPTION_USE_TIMESTAMP + " TIMESTAMP"
            + " Use specified timestamp instead of current time\n"
            + " TIMESTAMP format yyyyMMdd:HHmmss\n"
            + "-" + OPTION_DO_NOT_CREATE_FILE + " Do not create any files";

    private boolean changeModTime = false;
    private boolean changeAccessTime = false;
    private boolean doNotCreate = false;
    private String timestamp;
    private final SimpleDateFormat dateFormat =
        new SimpleDateFormat("yyyyMMdd:HHmmss");

    @InterfaceAudience.Private
    @VisibleForTesting
    public DateFormat getDateFormat() {
      return dateFormat;
    }

    @Override
    protected void processOptions(LinkedList<String> args) {
      this.timestamp =
          StringUtils.popOptionWithArgument("-" + OPTION_USE_TIMESTAMP, args);

      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE,
          OPTION_USE_TIMESTAMP, OPTION_CHANGE_ONLY_ACCESS_TIME,
          OPTION_CHANGE_ONLY_MODIFICATION_TIME, OPTION_DO_NOT_CREATE_FILE);
      cf.parse(args);
      this.changeModTime = cf.getOpt(OPTION_CHANGE_ONLY_MODIFICATION_TIME);
      this.changeAccessTime = cf.getOpt(OPTION_CHANGE_ONLY_ACCESS_TIME);
      this.doNotCreate = cf.getOpt(OPTION_DO_NOT_CREATE_FILE);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new PathIsDirectoryException(item.toString());
      }
      touch(item);
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      if (!item.parentExists()) {
        throw new PathNotFoundException(item.toString())
            .withFullyQualifiedPath(item.path.toUri().toString());
      }
      touch(item);
    }

    private void touch(PathData item) throws IOException {
      if (!item.fs.exists(item.path)) {
        if (doNotCreate) {
          return;
        }
        item.fs.create(item.path).close();
        if (timestamp != null) {
          // update the time only if user specified a timestamp using -t option.
          updateTime(item);
        }
      } else {
        updateTime(item);
      }
    }

    private void updateTime(PathData item) throws IOException {
      long time = System.currentTimeMillis();
      if (timestamp != null) {
        try {
          time = dateFormat.parse(timestamp).getTime();
        } catch (ParseException e) {
          throw new IllegalArgumentException(
              "Unable to parse the specified timestamp "+ timestamp
              + ". The expected format is " + dateFormat.toPattern(), e);
        }
      }
      if (changeModTime ^ changeAccessTime) {
        long atime = changeModTime ? -1 : time;
        long mtime = changeAccessTime ? -1 : time;
        item.fs.setTimes(item.path, mtime, atime);
      } else {
        item.fs.setTimes(item.path, time, time);
      }
    }
  }
}
