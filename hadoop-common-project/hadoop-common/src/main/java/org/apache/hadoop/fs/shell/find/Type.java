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
package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -type expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class Type extends BaseExpression {
  /**
   * Registers this expression with the specified factory.
   */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Type.class, "-type");
  }

  private static final String[] USAGE = {"-type filetype"};
  private static final String[] HELP =
      {"Evaluates to true if the file type matches that specified.",
          "The following file type values are supported:",
          "'d' (directory), 'l' (symbolic link), 'f' (regular file)."};

  private static final FileType DIRECTORY;
  private static final FileType SYMBOLIC_LINK;
  private static final FileType REGULAR_FILE;
  private static final Map<String, FileType> FILE_TYPES;

  static {
    DIRECTORY = new FileType("d") {
      public boolean matches(FileStatus stat) {
        return stat.isDirectory();
      }
    };
    SYMBOLIC_LINK = new FileType("l") {
      public boolean matches(FileStatus stat) {
        return stat.isSymlink();
      }
    };
    REGULAR_FILE = new FileType("f") {
      public boolean matches(FileStatus stat) {
        return stat.isFile();
      }
    };

    HashMap<String, FileType> map = new HashMap<>();
    map.put(DIRECTORY.getCode(), DIRECTORY);
    map.put(SYMBOLIC_LINK.getCode(), SYMBOLIC_LINK);
    map.put(REGULAR_FILE.getCode(), REGULAR_FILE);
    FILE_TYPES = Collections.unmodifiableMap(map);
  }

  Type() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  private static abstract class FileType {
    private final String code;

    private FileType(String code) {
      this.code = code;
    }

    public String getCode() {
      return this.code;
    }

    public abstract boolean matches(FileStatus stat);
  }

  private FileType fileType = null;

  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  @Override
  public void prepare() throws IOException {
    String arg = getArgument(1);
    if (FILE_TYPES.containsKey(arg)) {
      this.fileType = FILE_TYPES.get(arg);
    } else {
      throw new IOException("Invalid file type: " + arg);
    }
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    if (this.fileType.matches(getFileStatus(item, depth))) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
}
