package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.FsPermission;

public class RenameAtomicityUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RenameAtomicityUtils.class);

  private final AzureBlobFileSystem azureBlobFileSystem;
  private final Path srcPath;
  private final Path dstPath;
  private final TracingContext tracingContext;
  private final List<BlobProperty> blobPropertyList;

  private static final int MAX_RENAME_PENDING_FILE_SIZE = 10000000;
  private static final int FORMATTING_BUFFER = 10000;

  public static final String SUFFIX = "-RenamePending.json";

  RenameAtomicityUtils(final AzureBlobFileSystem azureBlobFileSystem,
  final Path srcPath, final Path dstPath, final TracingContext tracingContext,
      List<BlobProperty> blobPropertyList) {
    this.azureBlobFileSystem = azureBlobFileSystem;
    this.srcPath = srcPath;
    this.dstPath = dstPath;
    this.tracingContext = tracingContext;
    this.blobPropertyList = blobPropertyList;
  }


  /**
   * Write to disk the information needed to redo folder rename,
   * in JSON format. The file name will be
   * {@code wasb://<sourceFolderPrefix>/folderName-RenamePending.json}
   * The file format will be:
   * <pre>{@code
   * {
   *   FormatVersion: "1.0",
   *   OperationTime: "<YYYY-MM-DD HH:MM:SS.MMM>",
   *   OldFolderName: "<key>",
   *   NewFolderName: "<key>",
   *   FileList: [ <string> , <string> , ... ]
   * }
   *
   * Here's a sample:
   * {
   *  FormatVersion: "1.0",
   *  OperationUTCTime: "2014-07-01 23:50:35.572",
   *  OldFolderName: "user/ehans/folderToRename",
   *  NewFolderName: "user/ehans/renamedFolder",
   *  FileList: [
   *    "innerFile",
   *    "innerFile2"
   *  ]
   * } }</pre>
   * @throws IOException Thrown when fail to write file.
   */
  public void writeFile() throws IOException {
    Path path = getRenamePendingFilePath();
    LOG.debug("Preparing to write atomic rename state to {}", path.toString());
    OutputStream output = null;

    String contents = makeRenamePendingFileContents();

    // Write file.
    try {
      output = azureBlobFileSystem.create(path, false);
      output.write(contents.getBytes(Charset.forName("UTF-8")));
    } catch (IOException e) {
      throw new IOException("Unable to write RenamePending file for folder rename from "
          + srcPath.toUri().getPath() + " to " + dstPath.toUri().getPath(), e);
    }
  }

  /**
   * Return the contents of the JSON file to represent the operations
   * to be performed for a folder rename.
   *
   * @return JSON string which represents the operation.
   */
  private String makeRenamePendingFileContents() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    String time = sdf.format(new Date());

    // Make file list string
    StringBuilder builder = new StringBuilder();
    builder.append("[\n");
    for (int i = 0; i != blobPropertyList.size(); i++) {
      if (i > 0) {
        builder.append(",\n");
      }
      builder.append("    ");
      String noPrefix = StringUtils.removeStart(blobPropertyList.get(i).getPath().toUri().getPath(), srcPath.toUri().getPath() + "/");

      // Quote string file names, escaping any possible " characters or other
      // necessary characters in the name.
      builder.append(quote(noPrefix));
      if (builder.length() >=
          MAX_RENAME_PENDING_FILE_SIZE - FORMATTING_BUFFER) {

        // Give up now to avoid using too much memory.
        LOG.error("Internal error: Exceeded maximum rename pending file size of {} bytes.",
            MAX_RENAME_PENDING_FILE_SIZE);

        // return some bad JSON with an error message to make it human readable
        return "exceeded maximum rename pending file size";
      }
    }
    builder.append("\n  ]");
    String fileList = builder.toString();

    // Make file contents as a string. Again, quote file names, escaping
    // characters as appropriate.
    String contents = "{\n"
        + "  FormatVersion: \"1.0\",\n"
        + "  OperationUTCTime: \"" + time + "\",\n"
        + "  OldFolderName: " + quote(srcPath.toUri().getPath()) + ",\n"
        + "  NewFolderName: " + quote(dstPath.toUri().getPath()) + ",\n"
        + "  FileList: " + fileList + "\n"
        + "}\n";

    return contents;
  }

  /**
   * This is an exact copy of org.codehaus.jettison.json.JSONObject.quote
   * method.
   *
   * Produce a string in double quotes with backslash sequences in all the
   * right places. A backslash will be inserted within </, allowing JSON
   * text to be delivered in HTML. In JSON text, a string cannot contain a
   * control character or an unescaped quote or backslash.
   * @param string A String
   * @return  A String correctly formatted for insertion in a JSON text.
   */
  private String quote(String string) {
    if (string == null || string.length() == 0) {
      return "\"\"";
    }

    char c = 0;
    int  i;
    int  len = string.length();
    StringBuilder sb = new StringBuilder(len + 4);
    String t;

    sb.append('"');
    for (i = 0; i < len; i += 1) {
      c = string.charAt(i);
      switch (c) {
      case '\\':
      case '"':
        sb.append('\\');
        sb.append(c);
        break;
      case '/':
        sb.append('\\');
        sb.append(c);
        break;
      case '\b':
        sb.append("\\b");
        break;
      case '\t':
        sb.append("\\t");
        break;
      case '\n':
        sb.append("\\n");
        break;
      case '\f':
        sb.append("\\f");
        break;
      case '\r':
        sb.append("\\r");
        break;
      default:
        if (c < ' ') {
          t = "000" + Integer.toHexString(c);
          sb.append("\\u" + t.substring(t.length() - 4));
        } else {
          sb.append(c);
        }
      }
    }
    sb.append('"');
    return sb.toString();
  }

  /** Clean up after execution of rename.
   * @throws IOException Thrown when fail to clean up.
   * */
  public void cleanup() throws IOException {

      // Remove RenamePending file
      azureBlobFileSystem.delete(getRenamePendingFilePath(), false);

      // Freeing source folder lease is not necessary since the source
      // folder file was deleted.
  }

  private Path getRenamePendingFilePath() {
    String fileName = srcPath.toUri().getPath() + SUFFIX;
    Path fileNamePath = new Path(fileName);
    return fileNamePath;
  }
}
