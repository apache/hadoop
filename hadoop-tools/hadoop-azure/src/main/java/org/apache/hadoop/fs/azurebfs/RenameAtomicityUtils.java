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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * For a directory enabled for atomic-rename, before rename starts, a
 * file with -RenamePending.json suffix is created. In this file, the states required
 * for the rename are given. This file is created by {@link #writeFile()} method.
 * This is important in case the JVM process crashes during rename, the atomicity
 * will be maintained, when the job calls {@link AzureBlobFileSystem#listStatus(Path)}
 * or {@link AzureBlobFileSystem#getFileStatus(Path)}. On these API calls to filesystem,
 * it will be checked if there is any RenamePending JSON file. If yes, the rename
 * would be resumed as per the file.
 */
public class RenameAtomicityUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      RenameAtomicityUtils.class);

  private final AzureBlobFileSystem azureBlobFileSystem;
  private Path srcPath;
  private Path dstPath;
  private TracingContext tracingContext;
  private List<BlobProperty> blobPropertyList;

  private static final int MAX_RENAME_PENDING_FILE_SIZE = 10000000;
  private static final int FORMATTING_BUFFER = 10000;

  public static final String SUFFIX = "-RenamePending.json";

  private static final ObjectReader READER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
      .readerFor(JsonNode.class);

  RenameAtomicityUtils(final AzureBlobFileSystem azureBlobFileSystem,
      final Path srcPath,
      final Path dstPath,
      final TracingContext tracingContext,
      List<BlobProperty> blobPropertyList) throws IOException {
    this.azureBlobFileSystem = azureBlobFileSystem;
    this.srcPath = srcPath;
    this.dstPath = dstPath;
    this.tracingContext = tracingContext;
    this.blobPropertyList = blobPropertyList;

    writeFile();
  }

  RenameAtomicityUtils(final AzureBlobFileSystem azureBlobFileSystem,
      final Path path, final RedoRenameInvocation redoRenameInvocation)
      throws IOException {
    this.azureBlobFileSystem = azureBlobFileSystem;
    final RenamePendingFileInfo renamePendingFileInfo = readFile(path);
    if (renamePendingFileInfo != null) {
      redoRenameInvocation.redo(renamePendingFileInfo.destination,
          renamePendingFileInfo.srcList,
          renamePendingFileInfo.destinationSuffix);
    }
  }

  private RenamePendingFileInfo readFile(final Path redoFile)
      throws IOException {
    Path f = redoFile;
    FSDataInputStream input = azureBlobFileSystem.open(f);
    byte[] bytes = new byte[MAX_RENAME_PENDING_FILE_SIZE];
    int l = input.read(bytes);
    if (l <= 0) {
      // Jira HADOOP-12678 -Handle empty rename pending metadata file during
      // atomic rename in redo path. If during renamepending file is created
      // but not written yet, then this means that rename operation
      // has not started yet. So we should delete rename pending metadata file.
      LOG.error("Deleting empty rename pending file "
          + redoFile + " -- no data available");
      deleteRenamePendingFile(azureBlobFileSystem, redoFile);
      return null;
    }
    if (l == MAX_RENAME_PENDING_FILE_SIZE) {
      throw new IOException(
          "Error reading pending rename file contents -- "
              + "maximum file size exceeded");
    }
    String contents = new String(bytes, 0, l, StandardCharsets.UTF_8);

    // parse the JSON
    JsonNode json = null;
    boolean committed;
    try {
      json = READER.readValue(contents);
      committed = true;
    } catch (JsonMappingException e) {

      // The -RedoPending.json file is corrupted, so we assume it was
      // not completely written
      // and the redo operation did not commit.
      committed = false;
    } catch (JsonParseException e) {
      committed = false;
    }

    if (!committed) {
      LOG.error("Deleting corruped rename pending file {} \n {}",
          redoFile, contents);

      // delete the -RenamePending.json file
      deleteRenamePendingFile(azureBlobFileSystem, redoFile);
      return null;
    }

    // initialize this object's fields
    JsonNode oldFolderName = json.get("OldFolderName");
    JsonNode newFolderName = json.get("NewFolderName");
    if (oldFolderName != null && StringUtils.isNotEmpty(
        oldFolderName.textValue())
        && newFolderName != null && StringUtils.isNotEmpty(
        newFolderName.textValue())) {
      RenamePendingFileInfo renamePendingFileInfo = new RenamePendingFileInfo();
      renamePendingFileInfo.destination = new Path(newFolderName.textValue());
      String srcDir = oldFolderName.textValue() + "/";
      List<Path> srcPaths = new ArrayList<>();
      List<String> destinationSuffix = new ArrayList<>();
      JsonNode fileList = json.get("FileList");
      for (int i = 0; i < fileList.size(); i++) {
        destinationSuffix.add(fileList.get(i).textValue());
        srcPaths.add(new Path(srcDir + fileList.get(i).textValue()));
      }
      renamePendingFileInfo.srcList = srcPaths;
      renamePendingFileInfo.destinationSuffix = destinationSuffix;
      return renamePendingFileInfo;
    }
    return null;
  }

  private void deleteRenamePendingFile(FileSystem fs, Path redoFile)
      throws IOException {
    try {
      fs.delete(redoFile, false);
    } catch (IOException e) {
      // If the rename metadata was not found then somebody probably
      // raced with us and finished the delete first
      if (e instanceof FileNotFoundException) {
        LOG.warn("rename pending file " + redoFile + " is already deleted");
      } else {
        throw e;
      }
    }
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
  private void writeFile() throws IOException {
    Path path = getRenamePendingFilePath();
    LOG.debug("Preparing to write atomic rename state to {}", path.toString());
    OutputStream output = null;

    String contents = makeRenamePendingFileContents();

    // Write file.
    try {
      output = azureBlobFileSystem.create(path, false);
      output.write(contents.getBytes(Charset.forName("UTF-8")));
      output.flush();
      output.close();
    } catch (IOException e) {
      if (e instanceof FileNotFoundException) {
        /*
         * In case listStatus done on directory before any content could be written,
         * that particular thread running on some worker-node of the cluster would
         * delete the RenamePending JSON file.
         * ref: https://issues.apache.org/jira/browse/HADOOP-12678.
         * To recover from parallel delete, we will give it a second try.
         */
        output = azureBlobFileSystem.create(path, false);
        output.write(contents.getBytes(Charset.forName("UTF-8")));
        output.flush();
        output.close();
        return;
      }
      throw new IOException(
          "Unable to write RenamePending file for folder rename from "
              + srcPath.toUri().getPath() + " to " + dstPath.toUri().getPath(),
          e);
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
      final String noPrefix;
      /*
       * The marker file for the source directory has the same path on non-HNS.
       * For the other files, the fileList has to save the filePath relative to the
       * source directory.
       */
      if (!blobPropertyList.get(i)
          .getPath()
          .toUri()
          .getPath()
          .equals(srcPath.toUri().getPath())) {
        noPrefix = StringUtils.removeStart(
            blobPropertyList.get(i).getPath().toUri().getPath(),
            srcPath.toUri().getPath() + "/");
      } else {
        noPrefix = "";
      }

      // Quote string file names, escaping any possible " characters or other
      // necessary characters in the name.
      builder.append(quote(noPrefix));
      if (builder.length() >=
          MAX_RENAME_PENDING_FILE_SIZE - FORMATTING_BUFFER) {

        // Give up now to avoid using too much memory.
        LOG.error(
            "Internal error: Exceeded maximum rename pending file size of {} bytes.",
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
   * @return A String correctly formatted for insertion in a JSON text.
   */
  private String quote(String string) {
    if (string == null || string.length() == 0) {
      return "\"\"";
    }

    char c = 0;
    int i;
    int len = string.length();
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

  public void cleanup(Path redoFile) throws IOException {
    azureBlobFileSystem.delete(redoFile, false);
  }

  private Path getRenamePendingFilePath() {
    String fileName = srcPath.toUri().getPath() + SUFFIX;
    Path fileNamePath = new Path(fileName);
    return fileNamePath;
  }

  private static class RenamePendingFileInfo {
    public Path destination;
    public List<Path> srcList;
    /**
     * Relative paths from the destination path.
     */
    public List<String> destinationSuffix;
  }

  static interface RedoRenameInvocation {
    void redo(Path destination, List<Path> sourcePaths,
        final List<String> destinationSuffix) throws
        AzureBlobFileSystemException;
  }
}
