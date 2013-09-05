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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper for the Unix stat(1) command. Used to workaround the lack of 
 * lstat(2) in Java 6.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Stat extends Shell {

  private final Path original;
  private final Path qualified;
  private final Path path;
  private final long blockSize;
  private final boolean dereference;

  private FileStatus stat;

  public Stat(Path path, long blockSize, boolean deref, FileSystem fs)
      throws IOException {
    super(0L, true);
    // Original path
    this.original = path;
    // Qualify the original and strip out URI fragment via toUri().getPath()
    Path stripped = new Path(
        original.makeQualified(fs.getUri(), fs.getWorkingDirectory())
        .toUri().getPath());
    // Re-qualify the bare stripped path and store it
    this.qualified = 
        stripped.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    // Strip back down to a plain path
    this.path = new Path(qualified.toUri().getPath());
    this.blockSize = blockSize;
    this.dereference = deref;
    // LANG = C setting
    Map<String, String> env = new HashMap<String, String>();
    env.put("LANG", "C");
    setEnvironment(env);
  }

  public FileStatus getFileStatus() throws IOException {
    run();
    return stat;
  }

  /**
   * Whether Stat is supported on the current platform
   * @return
   */
  public static boolean isAvailable() {
    if (Shell.LINUX || Shell.FREEBSD || Shell.MAC) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  FileStatus getFileStatusForTesting() {
    return stat;
  }

  @Override
  protected String[] getExecString() {
    String derefFlag = "-";
    if (dereference) {
      derefFlag = "-L";
    }
    if (Shell.LINUX) {
      return new String[] {
          "stat", derefFlag + "c", "%s,%F,%Y,%X,%a,%U,%G,%N", path.toString() };
    } else if (Shell.FREEBSD || Shell.MAC) {
      return new String[] {
          "stat", derefFlag + "f", "%z,%HT,%m,%a,%Op,%Su,%Sg,`link' -> `%Y'",
          path.toString() };
    } else {
      throw new UnsupportedOperationException(
          "stat is not supported on this platform");
    }
  }

  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {
    // Reset stat
    stat = null;

    String line = lines.readLine();
    if (line == null) {
      throw new IOException("Unable to stat path: " + original);
    }
    if (line.endsWith("No such file or directory") ||
        line.endsWith("Not a directory")) {
      throw new FileNotFoundException("File " + original + " does not exist");
    }
    if (line.endsWith("Too many levels of symbolic links")) {
      throw new IOException("Possible cyclic loop while following symbolic" +
          " link " + original);
    }
    // 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,`link' -> `target'
    StringTokenizer tokens = new StringTokenizer(line, ",");
    try {
      long length = Long.parseLong(tokens.nextToken());
      boolean isDir = tokens.nextToken().equalsIgnoreCase("directory") ? true
          : false;
      // Convert from seconds to milliseconds
      long modTime = Long.parseLong(tokens.nextToken())*1000;
      long accessTime = Long.parseLong(tokens.nextToken())*1000;
      String octalPerms = tokens.nextToken();
      // FreeBSD has extra digits beyond 4, truncate them
      if (octalPerms.length() > 4) {
        int len = octalPerms.length();
        octalPerms = octalPerms.substring(len-4, len);
      }
      FsPermission perms = new FsPermission(Short.parseShort(octalPerms, 8));
      String owner = tokens.nextToken();
      String group = tokens.nextToken();
      String symStr = tokens.nextToken();
      // 'notalink'
      // 'link' -> `target'
      // '' -> ''
      Path symlink = null;
      StringTokenizer symTokens = new StringTokenizer(symStr, "`");
      symTokens.nextToken();
      try {
        String target = symTokens.nextToken();
        target = target.substring(0, target.length()-1);
        if (!target.isEmpty()) {
          symlink = new Path(target);
        }
      } catch (NoSuchElementException e) {
        // null if not a symlink
      }
      // Set stat
      stat = new FileStatus(length, isDir, 1, blockSize, modTime, accessTime,
          perms, owner, group, symlink, qualified);
    } catch (NumberFormatException e) {
      throw new IOException("Unexpected stat output: " + line, e);
    } catch (NoSuchElementException e) {
      throw new IOException("Unexpected stat output: " + line, e);
    }
  }
}
