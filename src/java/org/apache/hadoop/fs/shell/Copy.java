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
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/** Various commands for copy files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class Copy extends FsCommand {  
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Merge.class, "-getmerge");
  }

  /** merge multiple files together */
  public static class Merge extends Copy {
    public static final String NAME = "MergeToLocal";    
    public static final String USAGE = "<src> <localdst> [addnl]";
    public static final String DESCRIPTION =
      "Get all the files in the directories that\n" +
      "match the source file pattern and merge and sort them to only\n" +
      "one file on local fs. <src> is kept.\n";

    protected PathData dst = null;
    protected String delimiter = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(null, 2, 3);
      cf.parse(args);

      // TODO: this really should be a -nl option
      if ((args.size() > 2) && Boolean.parseBoolean(args.removeLast())) {
        delimiter = "\n";
      } else {
        delimiter = null;
      }
      
      Path path = new Path(args.removeLast());
      dst = new PathData(path.getFileSystem(getConf()), path);
    }

    @Override
    protected void processPath(PathData src) throws IOException {
      FileUtil.copyMerge(src.fs, src.path,
          dst.fs, dst.path, false, getConf(), delimiter);
    }
  }
}
