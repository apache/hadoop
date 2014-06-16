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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.ChmodParser;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.Shell;

/**
 * This class is the home for file permissions related commands.
 * Moved to this separate class since FsShell is getting too large.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsShellPermissions extends FsCommand {

  static Log LOG = FsShell.LOG;
  
  /**
   * Register the permission related commands with the factory
   * @param factory the command factory
   */
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Chmod.class, "-chmod");
    factory.addClass(Chown.class, "-chown");
    factory.addClass(Chgrp.class, "-chgrp");
  }

  /**
   * The pattern is almost as flexible as mode allowed by chmod shell command.
   * The main restriction is that we recognize only rwxXt. To reduce errors we
   * also enforce octal mode specifications of either 3 digits without a sticky
   * bit setting or four digits with a sticky bit setting.
   */
  public static class Chmod extends FsShellPermissions {
    public static final String NAME = "chmod";
    public static final String USAGE = "[-R] <MODE[,MODE]... | OCTALMODE> PATH...";
    public static final String DESCRIPTION =
      "Changes permissions of a file. " +
      "This works similar to the shell's chmod command with a few exceptions.\n" +
      "-R: modifies the files recursively. This is the only option" +
      " currently supported.\n" +
      "<MODE>: Mode is the same as mode used for the shell's command. " +
      "The only letters recognized are 'rwxXt', e.g. +t,a+r,g-w,+rwx,o=r.\n" +
      "<OCTALMODE>: Mode specifed in 3 or 4 digits. If 4 digits, the first " +
      "may be 1 or 0 to turn the sticky bit on or off, respectively.  Unlike " +
      "the shell command, it is not possible to specify only part of the " +
      "mode, e.g. 754 is same as u=rwx,g=rx,o=r.\n\n" +
      "If none of 'augo' is specified, 'a' is assumed and unlike the " +
      "shell command, no umask is applied.";

    protected ChmodParser pp;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "R", null);
      cf.parse(args);
      setRecursive(cf.getOpt("R"));

      String modeStr = args.removeFirst();
      try {
        pp = new ChmodParser(modeStr);
      } catch (IllegalArgumentException iea) {
        // TODO: remove "chmod : " so it's not doubled up in output, but it's
        // here for backwards compatibility...
        throw new IllegalArgumentException(
            "chmod : mode '" + modeStr + "' does not match the expected pattern.");      
      }
    }
    
    @Override
    protected void processPath(PathData item) throws IOException {
      short newperms = pp.applyNewPermission(item.stat);
      if (item.stat.getPermission().toShort() != newperms) {
        try {
          item.fs.setPermission(item.path, new FsPermission(newperms));
        } catch (IOException e) {
          LOG.debug("Error changing permissions of " + item, e);
          throw new IOException(
              "changing permissions of '" + item + "': " + e.getMessage());
        }
      }
    }    
  }
  
  // used by chown/chgrp
  static private String allowedChars = Shell.WINDOWS ? "[-_./@a-zA-Z0-9 ]" :
    "[-_./@a-zA-Z0-9]";

  /**
   * Used to change owner and/or group of files 
   */
  public static class Chown extends FsShellPermissions {
    public static final String NAME = "chown";
    public static final String USAGE = "[-R] [OWNER][:[GROUP]] PATH...";
    public static final String DESCRIPTION =
      "Changes owner and group of a file. " +
      "This is similar to the shell's chown command with a few exceptions.\n" +
      "-R: modifies the files recursively. This is the only option " +
      "currently supported.\n\n" +
      "If only the owner or group is specified, then only the owner or " +
      "group is modified. " +
      "The owner and group names may only consist of digits, alphabet, "+
      "and any of " + allowedChars + ". The names are case sensitive.\n\n" +
      "WARNING: Avoid using '.' to separate user name and group though " +
      "Linux allows it. If user names have dots in them and you are " +
      "using local file system, you might see surprising results since " +
      "the shell command 'chown' is used for local files.";

    ///allows only "allowedChars" above in names for owner and group
    static private final Pattern chownPattern = Pattern.compile(
        "^\\s*(" + allowedChars + "+)?([:](" + allowedChars + "*))?\\s*$");

    protected String owner = null;
    protected String group = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "R");
      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      parseOwnerGroup(args.removeFirst());
    }
    
    /**
     * Parse the first argument into an owner and group
     * @param ownerStr string describing new ownership
     */
    protected void parseOwnerGroup(String ownerStr) {
      Matcher matcher = chownPattern.matcher(ownerStr);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "'" + ownerStr + "' does not match expected pattern for [owner][:group].");
      }
      owner = matcher.group(1);
      group = matcher.group(3);
      if (group != null && group.length() == 0) {
        group = null;
      }
      if (owner == null && group == null) {
        throw new IllegalArgumentException(
            "'" + ownerStr + "' does not specify owner or group.");
      }    
    }
    
    @Override
    protected void processPath(PathData item) throws IOException {
      //Should we do case insensitive match?
      String newOwner = (owner == null || owner.equals(item.stat.getOwner())) ?
                        null : owner;
      String newGroup = (group == null || group.equals(item.stat.getGroup())) ?
                        null : group;

      if (newOwner != null || newGroup != null) {
        try {
          item.fs.setOwner(item.path, newOwner, newGroup);
        } catch (IOException e) {
          LOG.debug("Error changing ownership of " + item, e);
          throw new IOException(
              "changing ownership of '" + item + "': " + e.getMessage());
        }
      }
    }
  }

  /**
   * Used to change group of files 
   */
  public static class Chgrp extends Chown {
    public static final String NAME = "chgrp";
    public static final String USAGE = "[-R] GROUP PATH...";
    public static final String DESCRIPTION =
      "This is equivalent to -chown ... :GROUP ...";

    static private final Pattern chgrpPattern = 
      Pattern.compile("^\\s*(" + allowedChars + "+)\\s*$");

    @Override
    protected void parseOwnerGroup(String groupStr) {
      Matcher matcher = chgrpPattern.matcher(groupStr);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "'" + groupStr + "' does not match expected pattern for group");
      }
      owner = null;
      group = matcher.group(1);
    }
  }
}
