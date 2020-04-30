package org.apache.hadoop.fs.shell;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Concat the given files.
 */
@InterfaceAudience.Private @InterfaceStability.Unstable public class Concat
    extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Concat.class, "-concat");
  }

  public static final String NAME = "concat";
  public static final String USAGE = "<target path> <src path> <src path> ...";
  public static final String DESCRIPTION =
      "Concatenate existing source files into the target file.";
  private static FileSystem tstFs; // test only.

  @Override
  protected void processArguments(LinkedList<PathData> args)
      throws IOException {
    if (args.size() < 1) {
      throw new IOException("Target path not specified.");
    }
    if (args.size() < 3) {
      throw new IOException("The number of source paths is less than 2.");
    }
    PathData target = args.removeFirst();
    LinkedList<PathData> srcList = args;
    if (!target.exists || !target.stat.isFile()) {
      throw new IOException(String.format("Target path %s does not exist or is"
              + " not file.", target.path));
    }
    Path[] srcArray = new Path[srcList.size()];
    for (int i = 0; i < args.size(); i++) {
      PathData src = srcList.get(i);
      if (!src.exists || !src.stat.isFile()) {
        throw new IOException(
            String.format("%s does not exist or is not file.", src.path));
      }
      srcArray[i] = src.path;
    }
    FileSystem fs = target.fs;
    if (tstFs != null) {
      fs = tstFs;
    }
    fs.concat(target.path, srcArray);
  }

  @VisibleForTesting
  static void setTstFs(FileSystem fs) {
    tstFs = fs;
  }
}
