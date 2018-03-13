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

package org.apache.hadoop.fs.azure;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemMetricsSystem;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.fs.azure.security.RemoteWasbDelegationTokenManager;
import org.apache.hadoop.fs.azure.security.WasbDelegationTokenManager;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azure.NativeAzureFileSystemHelper.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.StorageException;

/**
 * A {@link FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>. This implementation is
 * blob-based and stores files on Azure in their native form so they can be read
 * by other Azure tools.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NativeAzureFileSystem extends FileSystem {
  private static final int USER_WX_PERMISION = 0300;
  private static final String USER_HOME_DIR_PREFIX_DEFAULT = "/user";
  /**
   * A description of a folder rename operation, including the source and
   * destination keys, and descriptions of the files in the source folder.
   */

  public static class FolderRenamePending {
    private SelfRenewingLease folderLease;
    private String srcKey;
    private String dstKey;
    private FileMetadata[] fileMetadata = null;    // descriptions of source files
    private ArrayList<String> fileStrings = null;
    private NativeAzureFileSystem fs;
    private static final int MAX_RENAME_PENDING_FILE_SIZE = 10000000;
    private static final int FORMATTING_BUFFER = 10000;
    private boolean committed;
    public static final String SUFFIX = "-RenamePending.json";
    private static final ObjectReader READER = new ObjectMapper()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .readerFor(JsonNode.class);

    // Prepare in-memory information needed to do or redo a folder rename.
    public FolderRenamePending(String srcKey, String dstKey, SelfRenewingLease lease,
        NativeAzureFileSystem fs) throws IOException {
      this.srcKey = srcKey;
      this.dstKey = dstKey;
      this.folderLease = lease;
      this.fs = fs;
      ArrayList<FileMetadata> fileMetadataList = new ArrayList<FileMetadata>();

      // List all the files in the folder.
      long start = Time.monotonicNow();
      String priorLastKey = null;
      do {
        PartialListing listing = fs.getStoreInterface().listAll(srcKey, AZURE_LIST_ALL,
          AZURE_UNBOUNDED_DEPTH, priorLastKey);
        for(FileMetadata file : listing.getFiles()) {
          fileMetadataList.add(file);
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);
      fileMetadata = fileMetadataList.toArray(new FileMetadata[fileMetadataList.size()]);
      long end = Time.monotonicNow();
      LOG.debug("Time taken to list {} blobs for rename operation is: {} ms", fileMetadata.length, (end - start));

      this.committed = true;
    }

    // Prepare in-memory information needed to do or redo folder rename from
    // a -RenamePending.json file read from storage. This constructor is to use during
    // redo processing.
    public FolderRenamePending(Path redoFile, NativeAzureFileSystem fs)
        throws IllegalArgumentException, IOException {

      this.fs = fs;

      // open redo file
      Path f = redoFile;
      FSDataInputStream input = fs.open(f);
      byte[] bytes = new byte[MAX_RENAME_PENDING_FILE_SIZE];
      int l = input.read(bytes);
      if (l <= 0) {
        // Jira HADOOP-12678 -Handle empty rename pending metadata file during
        // atomic rename in redo path. If during renamepending file is created
        // but not written yet, then this means that rename operation
        // has not started yet. So we should delete rename pending metadata file.
        LOG.error("Deleting empty rename pending file "
            + redoFile + " -- no data available");
        deleteRenamePendingFile(fs, redoFile);
        return;
      }
      if (l == MAX_RENAME_PENDING_FILE_SIZE) {
        throw new IOException(
            "Error reading pending rename file contents -- "
                + "maximum file size exceeded");
      }
      String contents = new String(bytes, 0, l, Charset.forName("UTF-8"));

      // parse the JSON
      JsonNode json = null;
      try {
        json = READER.readValue(contents);
        this.committed = true;
      } catch (JsonMappingException e) {

        // The -RedoPending.json file is corrupted, so we assume it was
        // not completely written
        // and the redo operation did not commit.
        this.committed = false;
      } catch (JsonParseException e) {
        this.committed = false;
      } catch (IOException e) {
        this.committed = false;
      }
      
      if (!this.committed) {
        LOG.error("Deleting corruped rename pending file {} \n {}",
            redoFile, contents);

        // delete the -RenamePending.json file
        deleteRenamePendingFile(fs, redoFile);
        return;
      }

      // initialize this object's fields
      ArrayList<String> fileStrList = new ArrayList<String>();
      JsonNode oldFolderName = json.get("OldFolderName");
      JsonNode newFolderName = json.get("NewFolderName");
      if (oldFolderName == null || newFolderName == null) {
        this.committed = false;
      } else {
        this.srcKey = oldFolderName.textValue();
        this.dstKey = newFolderName.textValue();
        if (this.srcKey == null || this.dstKey == null) {
          this.committed = false;
        } else {
          JsonNode fileList = json.get("FileList");
          if (fileList == null) {
            this.committed = false;
          } else {
            for (int i = 0; i < fileList.size(); i++) {
              fileStrList.add(fileList.get(i).textValue());
            }
          }
        }
      }
      this.fileStrings = fileStrList;
    }

    public FileMetadata[] getFiles() {
      return fileMetadata;
    }

    public SelfRenewingLease getFolderLease() {
      return folderLease;
    }

    /**
     * Deletes rename pending metadata file
     * @param fs -- the file system
     * @param redoFile - rename pending metadata file path
     * @throws IOException - If deletion fails
     */
    @VisibleForTesting
    void deleteRenamePendingFile(FileSystem fs, Path redoFile)
        throws IOException {
      try {
        fs.delete(redoFile, false);
      } catch (IOException e) {
        // If the rename metadata was not found then somebody probably
        // raced with us and finished the delete first
        Throwable t = e.getCause();
        if (t != null && t instanceof StorageException
            && "BlobNotFound".equals(((StorageException) t).getErrorCode())) {
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
     * @param fs file system on which a file is written.
     * @throws IOException Thrown when fail to write file.
     */
    public void writeFile(NativeAzureFileSystem fs) throws IOException {
      Path path = getRenamePendingFilePath();
      LOG.debug("Preparing to write atomic rename state to {}", path.toString());
      OutputStream output = null;

      String contents = makeRenamePendingFileContents();

      // Write file.
      try {
        output = fs.createInternal(path, FsPermission.getFileDefault(), false, null);
        output.write(contents.getBytes(Charset.forName("UTF-8")));
      } catch (IOException e) {
        throw new IOException("Unable to write RenamePending file for folder rename from "
            + srcKey + " to " + dstKey, e);
      } finally {
        NativeAzureFileSystemHelper.cleanup(LOG, output);
      }
    }

    /**
     * Return the contents of the JSON file to represent the operations
     * to be performed for a folder rename.
     *
     * @return JSON string which represents the operation.
     */
    public String makeRenamePendingFileContents() {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      String time = sdf.format(new Date());

      // Make file list string
      StringBuilder builder = new StringBuilder();
      builder.append("[\n");
      for (int i = 0; i != fileMetadata.length; i++) {
        if (i > 0) {
          builder.append(",\n");
        }
        builder.append("    ");
        String noPrefix = StringUtils.removeStart(fileMetadata[i].getKey(), srcKey + "/");

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
          + "  OldFolderName: " + quote(srcKey) + ",\n"
          + "  NewFolderName: " + quote(dstKey) + ",\n"
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

    public String getSrcKey() {
      return srcKey;
    }

    public String getDstKey() {
      return dstKey;
    }

    public FileMetadata getSourceMetadata() throws IOException {
      return fs.getStoreInterface().retrieveMetadata(srcKey);
    }

    /**
     * Execute a folder rename. This is the execution path followed
     * when everything is working normally. See redo() for the alternate
     * execution path for the case where we're recovering from a folder rename
     * failure.
     * @throws IOException Thrown when fail to renaming.
     */
    public void execute() throws IOException {

      AzureFileSystemThreadTask task = new AzureFileSystemThreadTask() {
        @Override
        public boolean execute(FileMetadata file) throws IOException{
          renameFile(file);
          return true;
        }
      };

      AzureFileSystemThreadPoolExecutor executor = this.fs.getThreadPoolExecutor(this.fs.renameThreadCount,
          "AzureBlobRenameThread", "Rename", getSrcKey(), AZURE_RENAME_THREADS);

      executor.executeParallel(this.getFiles(), task);

      // Rename the source folder 0-byte root file itself.
      FileMetadata srcMetadata2 = this.getSourceMetadata();
      if (srcMetadata2.getBlobMaterialization() ==
          BlobMaterialization.Explicit) {

        // It already has a lease on it from the "prepare" phase so there's no
        // need to get one now. Pass in existing lease to allow file delete.
        fs.getStoreInterface().rename(this.getSrcKey(), this.getDstKey(),
            false, folderLease);
      }

      // Update the last-modified time of the parent folders of both source and
      // destination.
      fs.updateParentFolderLastModifiedTime(srcKey);
      fs.updateParentFolderLastModifiedTime(dstKey);
    }

    // Rename a single file
    @VisibleForTesting
    void renameFile(FileMetadata file) throws IOException{
      // Rename all materialized entries under the folder to point to the
      // final destination.
      if (file.getBlobMaterialization() == BlobMaterialization.Explicit) {
        String srcName = file.getKey();
        String suffix  = srcName.substring((this.getSrcKey()).length());
        String dstName = this.getDstKey() + suffix;

        // Rename gets exclusive access (via a lease) for files
        // designated for atomic rename.
        // The main use case is for HBase write-ahead log (WAL) and data
        // folder processing correctness.  See the rename code for details.
        boolean acquireLease = this.fs.getStoreInterface().isAtomicRenameKey(srcName);
        this.fs.getStoreInterface().rename(srcName, dstName, acquireLease, null);
      }
    }

    /** Clean up after execution of rename.
     * @throws IOException Thrown when fail to clean up.
     * */
    public void cleanup() throws IOException {

      if (fs.getStoreInterface().isAtomicRenameKey(srcKey)) {

        // Remove RenamePending file
        fs.delete(getRenamePendingFilePath(), false);

        // Freeing source folder lease is not necessary since the source
        // folder file was deleted.
      }
    }

    private Path getRenamePendingFilePath() {
      String fileName = srcKey + SUFFIX;
      Path fileNamePath = keyToPath(fileName);
      Path path = fs.makeAbsolute(fileNamePath);
      return path;
    }

    /**
     * Recover from a folder rename failure by redoing the intended work,
     * as recorded in the -RenamePending.json file.
     * 
     * @throws IOException Thrown when fail to redo.
     */
    public void redo() throws IOException {

      if (!committed) {

        // Nothing to do. The -RedoPending.json file should have already been
        // deleted.
        return;
      }

      // Try to get a lease on source folder to block concurrent access to it.
      // It may fail if the folder is already gone. We don't check if the
      // source exists explicitly because that could recursively trigger redo
      // and give an infinite recursion.
      SelfRenewingLease lease = null;
      boolean sourceFolderGone = false;
      try {
        lease = fs.leaseSourceFolder(srcKey);
      } catch (AzureException e) {

        // If the source folder was not found then somebody probably
        // raced with us and finished the rename first, or the
        // first rename failed right before deleting the rename pending
        // file.
        String errorCode = "";
        try {
          StorageException se = (StorageException) e.getCause();
          errorCode = se.getErrorCode();
        } catch (Exception e2) {
          ; // do nothing -- could not get errorCode
        }
        if (errorCode.equals("BlobNotFound")) {
          sourceFolderGone = true;
        } else {
          throw new IOException(
              "Unexpected error when trying to lease source folder name during "
              + "folder rename redo",
              e);
        }
      }

      if (!sourceFolderGone) {
        // Make sure the target folder exists.
        Path dst = fullPath(dstKey);
        if (!fs.existsInternal(dst)) {
          fs.mkdirs(dst);
        }

        // For each file inside the folder to be renamed,
        // make sure it has been renamed.
        for(String fileName : fileStrings) {
          finishSingleFileRename(fileName);
        }

        // Remove the source folder. Don't check explicitly if it exists,
        // to avoid triggering redo recursively.
        try {
          // Rename the source folder 0-byte root file
          // as destination folder 0-byte root file.
          FileMetadata srcMetaData = this.getSourceMetadata();
          if (srcMetaData.getBlobMaterialization() == BlobMaterialization.Explicit) {
            // We already have a lease. So let's just rename the source blob
            // as destination blob under same lease.
            fs.getStoreInterface().rename(this.getSrcKey(), this.getDstKey(), false, lease);
          }

          // Now we can safely delete the source folder.
          fs.getStoreInterface().delete(srcKey, lease);
        } catch (Exception e) {
          LOG.info("Unable to delete source folder during folder rename redo. "
              + "If the source folder is already gone, this is not an error "
              + "condition. Continuing with redo.", e);
        }

        // Update the last-modified time of the parent folders of both source
        // and destination.
        fs.updateParentFolderLastModifiedTime(srcKey);
        fs.updateParentFolderLastModifiedTime(dstKey);
      }

      // Remove the -RenamePending.json file.
      fs.delete(getRenamePendingFilePath(), false);
    }

    // See if the source file is still there, and if it is, rename it.
    private void finishSingleFileRename(String fileName)
        throws IOException {
      Path srcFile = fullPath(srcKey, fileName);
      Path dstFile = fullPath(dstKey, fileName);
      String srcName = fs.pathToKey(srcFile);
      String dstName = fs.pathToKey(dstFile);
      boolean srcExists = fs.getStoreInterface().explicitFileExists(srcName);
      boolean dstExists = fs.getStoreInterface().explicitFileExists(dstName);
      if(srcExists) {
        // Rename gets exclusive access (via a lease) for HBase write-ahead log
        // (WAL) file processing correctness.  See the rename code for details.
        fs.getStoreInterface().rename(srcName, dstName, true, null);
      } else if (!srcExists && dstExists) {
        // The rename already finished, so do nothing.
        ;
      } else {
        // HADOOP-14512
        LOG.warn(
          "Attempting to complete rename of file " + srcKey + "/" + fileName
          + " during folder rename redo, and file was not found in source "
          + "or destination " + dstKey + "/" + fileName + ". "
          + "This must mean the rename of this file has already completed");
      }
    }

    // Return an absolute path for the specific fileName within the folder
    // specified by folderKey.
    private Path fullPath(String folderKey, String fileName) {
      return new Path(new Path(fs.getUri()), "/" + folderKey + "/" + fileName);
    }

    private Path fullPath(String fileKey) {
      return new Path(new Path(fs.getUri()), "/" + fileKey);
    }
  }

  private static final String TRAILING_PERIOD_PLACEHOLDER = "[[.]]";
  private static final Pattern TRAILING_PERIOD_PLACEHOLDER_PATTERN =
      Pattern.compile("\\[\\[\\.\\]\\](?=$|/)");
  private static final Pattern TRAILING_PERIOD_PATTERN = Pattern.compile("\\.(?=$|/)");

  @Override
  public String getScheme() {
    return "wasb";
  }


  /**
   * <p>
   * A {@link FileSystem} for reading and writing files stored on <a
   * href="http://store.azure.com/">Windows Azure</a>. This implementation is
   * blob-based and stores files on Azure in their native form so they can be read
   * by other Azure tools. This implementation uses HTTPS for secure network communication.
   * </p>
   */
  public static class Secure extends NativeAzureFileSystem {
    @Override
    public String getScheme() {
      return "wasbs";
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(NativeAzureFileSystem.class);

  static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  /**
   * The time span in seconds before which we consider a temp blob to be
   * dangling (not being actively uploaded to) and up for reclamation.
   * 
   * So e.g. if this is 60, then any temporary blobs more than a minute old
   * would be considered dangling.
   */
  static final String AZURE_TEMP_EXPIRY_PROPERTY_NAME = "fs.azure.fsck.temp.expiry.seconds";
  private static final int AZURE_TEMP_EXPIRY_DEFAULT = 3600;
  static final String PATH_DELIMITER = Path.SEPARATOR;
  static final String AZURE_TEMP_FOLDER = "_$azuretmpfolder$";

  private static final int AZURE_LIST_ALL = -1;
  private static final int AZURE_UNBOUNDED_DEPTH = -1;

  private static final long MAX_AZURE_BLOCK_SIZE = 512 * 1024 * 1024L;

  /**
   * The configuration property that determines which group owns files created
   * in WASB.
   */
  private static final String AZURE_DEFAULT_GROUP_PROPERTY_NAME = "fs.azure.permissions.supergroup";
  /**
   * The default value for fs.azure.permissions.supergroup. Chosen as the same
   * default as DFS.
   */
  static final String AZURE_DEFAULT_GROUP_DEFAULT = "supergroup";

  /**
   * Configuration property used to specify list of users that can perform
   * chown operation when authorization is enabled in WASB.
   */
  public static final String AZURE_CHOWN_USERLIST_PROPERTY_NAME =
      "fs.azure.chown.allowed.userlist";

  static final String AZURE_CHOWN_USERLIST_PROPERTY_DEFAULT_VALUE = "*";

  /**
   * Configuration property used to specify list of daemon users that can
   * perform chmod operation when authorization is enabled in WASB.
   */
  public static final String AZURE_DAEMON_USERLIST_PROPERTY_NAME =
      "fs.azure.daemon.userlist";

  static final String AZURE_DAEMON_USERLIST_PROPERTY_DEFAULT_VALUE = "*";

  /**
   * Configuration property used to specify list of users that can perform
   * chmod operation when authorization is enabled in WASB.
   */
  public static final String AZURE_CHMOD_USERLIST_PROPERTY_NAME =
          "fs.azure.chmod.allowed.userlist";

  static final String AZURE_CHMOD_USERLIST_PROPERTY_DEFAULT_VALUE = "*";

  static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME =
      "fs.azure.block.location.impersonatedhost";
  private static final String AZURE_BLOCK_LOCATION_HOST_DEFAULT =
      "localhost";
  static final String AZURE_RINGBUFFER_CAPACITY_PROPERTY_NAME =
      "fs.azure.ring.buffer.capacity";
  static final String AZURE_OUTPUT_STREAM_BUFFER_SIZE_PROPERTY_NAME =
      "fs.azure.output.stream.buffer.size";

  public static final String SKIP_AZURE_METRICS_PROPERTY_NAME = "fs.azure.skip.metrics";

  /*
   * Property to enable Append API.
   */
  public static final String APPEND_SUPPORT_ENABLE_PROPERTY_NAME = "fs.azure.enable.append.support";

  /**
   * The configuration property to set number of threads to be used for rename operation.
   */
  public static final String AZURE_RENAME_THREADS = "fs.azure.rename.threads";

  /**
   * The default number of threads to be used for rename operation.
   */
  public static final int DEFAULT_AZURE_RENAME_THREADS = 0;

  /**
   * The configuration property to set number of threads to be used for delete operation.
   */
  public static final String AZURE_DELETE_THREADS = "fs.azure.delete.threads";

  /**
   * The default number of threads to be used for delete operation.
   */
  public static final int DEFAULT_AZURE_DELETE_THREADS = 0;

  /**
   * The number of threads to be used for delete operation after reading user configuration.
   */
  private int deleteThreadCount = 0;

  /**
   * The number of threads to be used for rename operation after reading user configuration.
   */
  private int renameThreadCount = 0;

  private class NativeAzureFsInputStream extends FSInputStream {
    private InputStream in;
    private final String key;
    private long pos = 0;
    private boolean closed = false;
    private boolean isPageBlob;

    // File length, valid only for streams over block blobs.
    private long fileLength;

    NativeAzureFsInputStream(InputStream in, String key, long fileLength) {
      this.in = in;
      this.key = key;
      this.isPageBlob = store.isPageBlobKey(key);
      this.fileLength = fileLength;
    }

    /**
     * Return the size of the remaining available bytes
     * if the size is less than or equal to {@link Integer#MAX_VALUE},
     * otherwise, return {@link Integer#MAX_VALUE}.
     *
     * This is to match the behavior of DFSInputStream.available(),
     * which some clients may rely on (HBase write-ahead log reading in
     * particular).
     */
    @Override
    public synchronized int available() throws IOException {
      if (isPageBlob) {
        return in.available();
      } else {
        if (closed) {
          throw new IOException("Stream closed");
        }
        final long remaining = this.fileLength - pos;
        return remaining <= Integer.MAX_VALUE ?
            (int) remaining : Integer.MAX_VALUE;
      }
    }

    /*
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an integer in the range 0 to 255. If no byte is available
     * because the end of the stream has been reached, the value -1 is returned.
     * This method blocks until input data is available, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @returns int An integer corresponding to the byte read.
     */
    @Override
    public synchronized int read() throws FileNotFoundException, IOException {
      try {
        int result = 0;
        result = in.read();
        if (result != -1) {
          pos++;
          if (statistics != null) {
            statistics.incrementBytesRead(1);
          }
        }
      // Return to the caller with the result.
      //
        return result;
      } catch(EOFException e) {
        return -1;
      } catch(IOException e) {

        Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(e);

        if (innerException instanceof StorageException) {

          LOG.error("Encountered Storage Exception for read on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e, ((StorageException) innerException).getErrorCode());

          if (NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {
            throw new FileNotFoundException(String.format("%s is not found", key));
          }
        }

       throw e;
      }
    }

    /*
     * Reads up to len bytes of data from the input stream into an array of
     * bytes. An attempt is made to read as many as len bytes, but a smaller
     * number may be read. The number of bytes actually read is returned as an
     * integer. This method blocks until input data is available, end of file is
     * detected, or an exception is thrown. If len is zero, then no bytes are
     * read and 0 is returned; otherwise, there is an attempt to read at least
     * one byte. If no byte is available because the stream is at end of file,
     * the value -1 is returned; otherwise, at least one byte is read and stored
     * into b.
     *
     * @param b -- the buffer into which data is read
     *
     * @param off -- the start offset in the array b at which data is written
     *
     * @param len -- the maximum number of bytes read
     *
     * @ returns int The total number of byes read into the buffer, or -1 if
     * there is no more data because the end of stream is reached.
     */
    @Override
    public synchronized int read(byte[] b, int off, int len) throws FileNotFoundException, IOException {
      try {
        int result = 0;
        result = in.read(b, off, len);
        if (result > 0) {
          pos += result;
        }

        if (null != statistics && result > 0) {
          statistics.incrementBytesRead(result);
        }

        // Return to the caller with the result.
        return result;
      } catch(IOException e) {

        Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(e);

        if (innerException instanceof StorageException) {

          LOG.error("Encountered Storage Exception for read on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e, ((StorageException) innerException).getErrorCode());

          if (NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {
            throw new FileNotFoundException(String.format("%s is not found", key));
          }
        }

       throw e;
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (!closed) {
        closed = true;
        IOUtils.closeStream(in);
        in = null;
      }
    }

    @Override
    public synchronized void seek(long pos) throws FileNotFoundException, EOFException, IOException {
      try {
        checkNotClosed();
        if (pos < 0) {
          throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (this.pos > pos) {
          if (in instanceof Seekable) {
            ((Seekable) in).seek(pos);
            this.pos = pos;
          } else {
            IOUtils.closeStream(in);
            in = store.retrieve(key);
            this.pos = in.skip(pos);
          }
        } else {
          this.pos += in.skip(pos - this.pos);
        }
        LOG.debug("Seek to position {}. Bytes skipped {}", pos,
          this.pos);
      } catch(IOException e) {

        Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(e);

        if (innerException instanceof StorageException
            && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {
          throw new FileNotFoundException(String.format("%s is not found", key));
        }

        throw e;
      } catch(IndexOutOfBoundsException e) {
        throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
      }
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }


    /*
     * Helper method to check if a stream is closed.
     */
    private void checkNotClosed() throws IOException {
      if (closed) {
        throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      }
    }
  }

  /**
   * Azure output stream; wraps an inner stream of different types.
   */
  public class NativeAzureFsOutputStream extends OutputStream
      implements Syncable, StreamCapabilities {
    private String key;
    private String keyEncoded;
    private OutputStream out;

    public NativeAzureFsOutputStream(OutputStream out, String aKey,
        String anEncodedKey) throws IOException {
      // Check input arguments. The output stream should be non-null and the
      // keys
      // should be valid strings.
      if (null == out) {
        throw new IllegalArgumentException(
            "Illegal argument: the output stream is null.");
      }

      if (null == aKey || 0 == aKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the key string is null or empty");
      }

      if (null == anEncodedKey || 0 == anEncodedKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the encoded key string is null or empty");
      }

      // Initialize the member variables with the incoming parameters.
      this.out = out;

      setKey(aKey);
      setEncodedKey(anEncodedKey);
    }

    /**
     * Get a reference to the wrapped output stream.
     *
     * @return the underlying output stream
     */
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public OutputStream getOutStream() {
      return out;
    }

    @Override  // Syncable
    public void hflush() throws IOException {
      if (out instanceof Syncable) {
        ((Syncable) out).hflush();
      } else {
        flush();
      }
    }

    @Override  // Syncable
    public void hsync() throws IOException {
      if (out instanceof Syncable) {
        ((Syncable) out).hsync();
      } else {
        flush();
      }
    }

    /**
     * Propagate probe of stream capabilities to nested stream
     * (if supported), else return false.
     * @param capability string to query the stream support for.
     * @return true if the nested stream supports the specific capability.
     */
    @Override // StreamCapability
    public boolean hasCapability(String capability) {
      if (out instanceof StreamCapabilities) {
        return ((StreamCapabilities) out).hasCapability(capability);
      }
      return false;
    }

    @Override
    public synchronized void close() throws IOException {
      if (out != null) {
        // Close the output stream and decode the key for the output stream
        // before returning to the caller.
        //
        out.close();
        try {
          restoreKey();
        } finally {
          out = null;
        }
      }
    }

    /**
     * Writes the specified byte to this output stream. The general contract for
     * write is that one byte is written to the output stream. The byte to be
     * written is the eight low-order bits of the argument b. The 24 high-order
     * bits of b are ignored.
     * 
     * @param b
     *          32-bit integer of block of 4 bytes
     */
    @Override
    public void write(int b) throws IOException {
      try {
        out.write(b);
      } catch(IOException e) {
        if (e.getCause() instanceof StorageException) {
          StorageException storageExcp  = (StorageException) e.getCause();
          LOG.error("Encountered Storage Exception for write on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e.getMessage(), storageExcp.getErrorCode());
        }
        throw e;
      }
    }

    /**
     * Writes b.length bytes from the specified byte array to this output
     * stream. The general contract for write(b) is that it should have exactly
     * the same effect as the call write(b, 0, b.length).
     * 
     * @param b
     *          Block of bytes to be written to the output stream.
     */
    @Override
    public void write(byte[] b) throws IOException {
      try {
        out.write(b);
      } catch(IOException e) {
        if (e.getCause() instanceof StorageException) {
          StorageException storageExcp  = (StorageException) e.getCause();
          LOG.error("Encountered Storage Exception for write on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e.getMessage(), storageExcp.getErrorCode());
        }
        throw e;
      }
    }

    /**
     * Writes <code>len</code> from the specified byte array starting at offset
     * <code>off</code> to the output stream. The general contract for write(b,
     * off, len) is that some of the bytes in the array <code>b</code>
     * are written to the output stream in order; element <code>b[off]</code>
     * is the first byte written and <code>b[off+len-1]</code> is the last
     * byte written by this operation.
     * 
     * @param b
     *          Byte array to be written.
     * @param off
     *          Write this offset in stream.
     * @param len
     *          Number of bytes to be written.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        out.write(b, off, len);
      } catch(IOException e) {
        if (e.getCause() instanceof StorageException) {
          StorageException storageExcp  = (StorageException) e.getCause();
          LOG.error("Encountered Storage Exception for write on Blob : {}"
              + " Exception details: {} Error Code : {}",
              key, e.getMessage(), storageExcp.getErrorCode());
        }
        throw e;
      }
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getKey() {
      return key;
    }

    /**
     * Set the blob name.
     * 
     * @param key
     *          Blob name.
     */
    public void setKey(String key) {
      this.key = key;
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getEncodedKey() {
      return keyEncoded;
    }

    /**
     * Set the blob name.
     * 
     * @param anEncodedKey
     *          Blob name.
     */
    public void setEncodedKey(String anEncodedKey) {
      this.keyEncoded = anEncodedKey;
    }

    /**
     * Restore the original key name from the m_key member variable. Note: The
     * output file stream is created with an encoded blob store key to guarantee
     * load balancing on the front end of the Azure storage partition servers.
     * The create also includes the name of the original key value which is
     * stored in the m_key member variable. This method should only be called
     * when the stream is closed.
     */
    private void restoreKey() throws IOException {
      store.rename(getEncodedKey(), getKey());
    }
  }

  private URI uri;
  private NativeFileSystemStore store;
  private AzureNativeFileSystemStore actualStore;
  private Path workingDir;
  private long blockSize = MAX_AZURE_BLOCK_SIZE;
  private AzureFileSystemInstrumentation instrumentation;
  private String metricsSourceName;
  private boolean isClosed = false;
  private static boolean suppressRetryPolicy = false;
  // A counter to create unique (within-process) names for my metrics sources.
  private static AtomicInteger metricsSourceNameCounter = new AtomicInteger();
  private boolean appendSupportEnabled = false;
  private DelegationTokenAuthenticatedURL authURL;
  private DelegationTokenAuthenticatedURL.Token authToken = new DelegationTokenAuthenticatedURL.Token();
  private String credServiceUrl;
  private List<String> chownAllowedUsers;
  private List<String> chmodAllowedUsers;
  private List<String> daemonUsers;
  /**
   * Configuration key to enable authorization support in WASB.
   */
  public static final String KEY_AZURE_AUTHORIZATION =
      "fs.azure.authorization";

  /**
   * Default value for the authorization support in WASB.
   */
  private static final boolean DEFAULT_AZURE_AUTHORIZATION = false;

  /**
   * Flag controlling authorization support in WASB.
   */
  private boolean azureAuthorization = false;

  /**
   * Flag controlling Kerberos support in WASB.
   */
  private boolean kerberosSupportEnabled = false;

  /**
   * Authorizer to use when authorization support is enabled in
   * WASB.
   */
  private WasbAuthorizerInterface authorizer = null;

  private UserGroupInformation ugi;

  private WasbDelegationTokenManager wasbDelegationTokenManager;

  public NativeAzureFileSystem() {
    // set store in initialize()
  }

  public NativeAzureFileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Suppress the default retry policy for the Storage, useful in unit tests to
   * test negative cases without waiting forever.
   */
  @VisibleForTesting
  static void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
   * Undo the effect of suppressRetryPolicy.
   */
  @VisibleForTesting
  static void resumeRetryPolicy() {
    suppressRetryPolicy = false;
  }

  /**
   * Creates a new metrics source name that's unique within this process.
   * @return metric source name
   */
  @VisibleForTesting
  public static String newMetricsSourceName() {
    int number = metricsSourceNameCounter.incrementAndGet();
    final String baseName = "AzureFileSystemMetrics";
    if (number == 1) { // No need for a suffix for the first one
      return baseName;
    } else {
      return baseName + number;
    }
  }

  /**
   * Checks if the given URI scheme is a scheme that's affiliated with the Azure
   * File System.
   *
   * @param scheme
   *          The URI scheme.
   * @return true iff it's an Azure File System URI scheme.
   */
  private static boolean isWasbScheme(String scheme) {
    // The valid schemes are: asv (old name), asvs (old name over HTTPS),
    // wasb (new name), wasbs (new name over HTTPS).
    return scheme != null
        && (scheme.equalsIgnoreCase("asv") || scheme.equalsIgnoreCase("asvs")
            || scheme.equalsIgnoreCase("wasb") || scheme
              .equalsIgnoreCase("wasbs"));
  }

  /**
   * Puts in the authority of the default file system if it is a WASB file
   * system and the given URI's authority is null.
   *
   * @return The URI with reconstructed authority if necessary and possible.
   */
  private static URI reconstructAuthorityIfNeeded(URI uri, Configuration conf) {
    if (null == uri.getAuthority()) {
      // If WASB is the default file system, get the authority from there
      URI defaultUri = FileSystem.getDefaultUri(conf);
      if (defaultUri != null && isWasbScheme(defaultUri.getScheme())) {
        try {
          // Reconstruct the URI with the authority from the default URI.
          return new URI(uri.getScheme(), defaultUri.getAuthority(),
              uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
          // This should never happen.
          throw new Error("Bad URI construction", e);
        }
      }
    }
    return uri;
  }

  @Override
  protected void checkPath(Path path) {
    // Make sure to reconstruct the path's authority if needed
    super.checkPath(new Path(reconstructAuthorityIfNeeded(path.toUri(),
        getConf())));
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException, IllegalArgumentException {
    // Check authority for the URI to guarantee that it is non-null.
    uri = reconstructAuthorityIfNeeded(uri, conf);
    if (null == uri.getAuthority()) {
      final String errMsg = String
          .format("Cannot initialize WASB file system, URI authority not recognized.");
      throw new IllegalArgumentException(errMsg);
    }
    super.initialize(uri, conf);

    if (store == null) {
      store = createDefaultStore(conf);
    }

    instrumentation = new AzureFileSystemInstrumentation(conf);
    if(!conf.getBoolean(SKIP_AZURE_METRICS_PROPERTY_NAME, false)) {
      // Make sure the metrics system is available before interacting with Azure
      AzureFileSystemMetricsSystem.fileSystemStarted();
      metricsSourceName = newMetricsSourceName();
      String sourceDesc = "Azure Storage Volume File System metrics";
      AzureFileSystemMetricsSystem.registerSource(metricsSourceName, sourceDesc,
        instrumentation);
    }

    store.initialize(uri, conf, instrumentation);
    setConf(conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", UserGroupInformation.getCurrentUser()
        .getShortUserName()).makeQualified(getUri(), getWorkingDirectory());
    this.blockSize = conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME,
        MAX_AZURE_BLOCK_SIZE);

    this.appendSupportEnabled = conf.getBoolean(APPEND_SUPPORT_ENABLE_PROPERTY_NAME, false);
    LOG.debug("NativeAzureFileSystem. Initializing.");
    LOG.debug("  blockSize  = {}",
        conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME, MAX_AZURE_BLOCK_SIZE));

    // Initialize thread counts from user configuration
    deleteThreadCount = conf.getInt(AZURE_DELETE_THREADS, DEFAULT_AZURE_DELETE_THREADS);
    renameThreadCount = conf.getInt(AZURE_RENAME_THREADS, DEFAULT_AZURE_RENAME_THREADS);

    boolean useSecureMode = conf.getBoolean(AzureNativeFileSystemStore.KEY_USE_SECURE_MODE,
        AzureNativeFileSystemStore.DEFAULT_USE_SECURE_MODE);

    this.azureAuthorization = useSecureMode &&
        conf.getBoolean(KEY_AZURE_AUTHORIZATION, DEFAULT_AZURE_AUTHORIZATION);
    this.kerberosSupportEnabled =
        conf.getBoolean(Constants.AZURE_KERBEROS_SUPPORT_PROPERTY_NAME, false);

    if (this.azureAuthorization) {

      this.authorizer =
          new RemoteWasbAuthorizerImpl();
      authorizer.init(conf);

      this.chmodAllowedUsers =
          Arrays.asList(conf.getTrimmedStrings(
              AZURE_CHMOD_USERLIST_PROPERTY_NAME,
                  AZURE_CHMOD_USERLIST_PROPERTY_DEFAULT_VALUE));
      this.chownAllowedUsers =
          Arrays.asList(conf.getTrimmedStrings(
              AZURE_CHOWN_USERLIST_PROPERTY_NAME,
                  AZURE_CHOWN_USERLIST_PROPERTY_DEFAULT_VALUE));
      this.daemonUsers =
          Arrays.asList(conf.getTrimmedStrings(
              AZURE_DAEMON_USERLIST_PROPERTY_NAME,
                  AZURE_DAEMON_USERLIST_PROPERTY_DEFAULT_VALUE));
    }

    if (UserGroupInformation.isSecurityEnabled() && kerberosSupportEnabled) {
      this.wasbDelegationTokenManager = new RemoteWasbDelegationTokenManager(conf);
    }
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(
        USER_HOME_DIR_PREFIX_DEFAULT + "/" + this.ugi.getShortUserName()));
  }

  @VisibleForTesting
  public void updateWasbAuthorizer(WasbAuthorizerInterface authorizer) {
    this.authorizer = authorizer;
  }

  private NativeFileSystemStore createDefaultStore(Configuration conf) {
    actualStore = new AzureNativeFileSystemStore();

    if (suppressRetryPolicy) {
      actualStore.suppressRetryPolicy();
    }
    return actualStore;
  }

  /**
   * Azure Storage doesn't allow the blob names to end in a period,
   * so encode this here to work around that limitation.
   */
  private static String encodeTrailingPeriod(String toEncode) {
    Matcher matcher = TRAILING_PERIOD_PATTERN.matcher(toEncode);
    return matcher.replaceAll(TRAILING_PERIOD_PLACEHOLDER);
  }

  /**
   * Reverse the encoding done by encodeTrailingPeriod().
   */
  private static String decodeTrailingPeriod(String toDecode) {
    Matcher matcher = TRAILING_PERIOD_PLACEHOLDER_PATTERN.matcher(toDecode);
    return matcher.replaceAll(".");
  }

  /**
   * Convert the path to a key. By convention, any leading or trailing slash is
   * removed, except for the special case of a single slash.
   * @param path path converted to a key
   * @return key string
   */
  @VisibleForTesting
  public String pathToKey(Path path) {
    // Convert the path to a URI to parse the scheme, the authority, and the
    // path from the path object.
    URI tmpUri = path.toUri();
    String pathUri = tmpUri.getPath();

    // The scheme and authority is valid. If the path does not exist add a "/"
    // separator to list the root of the container.
    Path newPath = path;
    if ("".equals(pathUri)) {
      newPath = new Path(tmpUri.toString() + Path.SEPARATOR);
    }

    // Verify path is absolute if the path refers to a windows drive scheme.
    if (!newPath.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }

    String key = null;
    key = newPath.toUri().getPath();
    key = removeTrailingSlash(key);
    key = encodeTrailingPeriod(key);
    if (key.length() == 1) {
      return key;
    } else {
      return key.substring(1); // remove initial slash
    }
  }

  // Remove any trailing slash except for the case of a single slash.
  private static String removeTrailingSlash(String key) {
    if (key.length() == 0 || key.length() == 1) {
      return key;
    }
    if (key.charAt(key.length() - 1) == '/') {
      return key.substring(0, key.length() - 1);
    } else {
      return key;
    }
  }

  private static Path keyToPath(String key) {
    if (key.equals("/")) {
      return new Path("/"); // container
    }
    return new Path("/" + decodeTrailingPeriod(key));
  }

  /**
   * Get the absolute version of the path (fully qualified).
   * This is public for testing purposes.
   *
   * @param path path to be absolute path.
   * @return fully qualified path
   */
  @VisibleForTesting
  public Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * For unit test purposes, retrieves the AzureNativeFileSystemStore store
   * backing this file system.
   *
   * @return The store object.
   */
  @VisibleForTesting
  public AzureNativeFileSystemStore getStore() {
    return actualStore;
  }

  NativeFileSystemStore getStoreInterface() {
    return store;
  }

  /**
   * @param requestingAccessForPath - The path to the ancestor/parent/subtree/file that needs to be
   *                                checked before granting access to originalPath
   * @param accessType - The type of access READ/WRITE being requested
   * @param operation - A string describing the operation being performed ("delete", "create" etc.).
   * @param originalPath - The originalPath that was being accessed
   */
  private void performAuthCheck(Path requestingAccessForPath, WasbAuthorizationOperations accessType,
      String operation, Path originalPath) throws WasbAuthorizationException, IOException {

    if (azureAuthorization && this.authorizer != null) {

      requestingAccessForPath = requestingAccessForPath.makeQualified(getUri(), getWorkingDirectory());
      originalPath = originalPath.makeQualified(getUri(), getWorkingDirectory());

      String owner = getOwnerForPath(requestingAccessForPath);

      if (!this.authorizer.authorize(requestingAccessForPath.toString(), accessType.toString(), owner)) {
        throw new WasbAuthorizationException(operation
            + " operation for Path : " + originalPath.toString() + " not allowed");
      }

   }
  }

  /**
   * Gets the metrics source for this file system.
   * This is mainly here for unit testing purposes.
   *
   * @return the metrics source.
   */
  public AzureFileSystemInstrumentation getInstrumentation() {
    return instrumentation;
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {

    if (!appendSupportEnabled) {
      throw new UnsupportedOperationException("Append Support not enabled");
    }

    LOG.debug("Opening file: {} for append", f);

    Path absolutePath = makeAbsolute(f);

    performAuthCheck(absolutePath, WasbAuthorizationOperations.WRITE, "append", absolutePath);

    String key = pathToKey(absolutePath);
    FileMetadata meta = null;
    try {
      meta = store.retrieveMetadata(key);
    } catch(Exception ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("%s is not found", key));
      } else {
        throw ex;
      }
    }

    if (meta == null) {
      throw new FileNotFoundException(f.toString());
    }

    if (meta.isDir()) {
      throw new FileNotFoundException(f.toString()
          + " is a directory not a file.");
    }

    if (store.isPageBlobKey(key)) {
      throw new IOException("Append not supported for Page Blobs");
    }

    DataOutputStream appendStream = null;

    try {
      appendStream = store.retrieveAppendStream(key, bufferSize);
    } catch (Exception ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {
        throw new FileNotFoundException(String.format("%s is not found", key));
      } else {
        throw ex;
      }
    }

    return new FSDataOutputStream(appendStream, statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, true,
        bufferSize, replication, blockSize, progress,
        (SelfRenewingLease) null);
  }

  /**
   * Get a self-renewing lease on the specified file.
   * @param path path whose lease to be renewed.
   * @return Lease
   * @throws AzureException when not being able to acquire a lease on the path
   */
  public SelfRenewingLease acquireLease(Path path) throws AzureException {
    String fullKey = pathToKey(makeAbsolute(path));
    return getStore().acquireLease(fullKey);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    Path parent = f.getParent();

    // Get exclusive access to folder if this is a directory designated
    // for atomic rename. The primary use case of for HBase write-ahead
    // log file management.
    SelfRenewingLease lease = null;
    if (store.isAtomicRenameKey(pathToKey(f))) {
      try {
        lease = acquireLease(parent);
      } catch (AzureException e) {

        String errorCode = "";
        try {
          StorageException e2 = (StorageException) e.getCause();
          errorCode = e2.getErrorCode();
        } catch (Exception e3) {
          // do nothing if cast fails
        }
        if (errorCode.equals("BlobNotFound")) {
          throw new FileNotFoundException("Cannot create file " +
              f.getName() + " because parent folder does not exist.");
        }

        LOG.warn("Got unexpected exception trying to get lease on {} . {}",
          pathToKey(parent), e.getMessage());
        throw e;
      }
    }

    // See if the parent folder exists. If not, throw error.
    // The exists() check will push any pending rename operation forward,
    // if there is one, and return false.
    //
    // At this point, we have exclusive access to the source folder
    // via the lease, so we will not conflict with an active folder
    // rename operation.
    //
    // In the secure case, the call to exists will happen in the context
    // of the user that initiated the operation. In this case, we should
    // do the auth-check against ranger for the path.
    if (!exists(parent)) {
      try {

        // This'll let the keep-alive thread exit as soon as it wakes up.
        lease.free();
      } catch (Exception e) {
        LOG.warn("Unable to free lease because: {}", e.getMessage());
      }
      throw new FileNotFoundException("Cannot create file " +
          f.getName() + " because parent folder does not exist.");
    }

    // Create file inside folder.
    FSDataOutputStream out = null;
    try {
      out = create(f, permission, overwrite, false,
          bufferSize, replication, blockSize, progress, lease);
    } finally {
      // Release exclusive access to folder.
      try {
        if (lease != null) {
          lease.free();
        }
      } catch (Exception e) {
        NativeAzureFileSystemHelper.cleanup(LOG, out);
        String msg = "Unable to free lease on " + parent.toUri();
        LOG.error(msg);
        throw new IOException(msg, e);
      }
    }
    return out;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    // Check if file should be appended or overwritten. Assume that the file
    // is overwritten on if the CREATE and OVERWRITE create flags are set. Note
    // that any other combinations of create flags will result in an open new or
    // open with append.
    final EnumSet<CreateFlag> createflags =
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
    boolean overwrite = flags.containsAll(createflags);

    // Delegate the create non-recursive call.
    return this.createNonRecursive(f, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return this.createNonRecursive(f, FsPermission.getFileDefault(),
        overwrite, bufferSize, replication, blockSize, progress);
  }


  /**
   * Create an Azure blob and return an output stream to use
   * to write data to it.
   *
   * @param f
   * @param permission
   * @param overwrite
   * @param createParent
   * @param bufferSize
   * @param replication
   * @param blockSize
   * @param progress
   * @param parentFolderLease Lease on parent folder (or null if
   * no lease).
   * @return
   * @throws IOException
   */
  private FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, boolean createParent, int bufferSize,
      short replication, long blockSize, Progressable progress,
      SelfRenewingLease parentFolderLease)
          throws FileAlreadyExistsException, IOException {

    LOG.debug("Creating file: {}", f.toString());

    if (containsColon(f)) {
      throw new IOException("Cannot create file " + f
          + " through WASB that has colons in the name");
    }

    Path absolutePath = makeAbsolute(f);
    Path ancestor = getAncestor(absolutePath);

    performAuthCheck(ancestor, WasbAuthorizationOperations.WRITE, "create", absolutePath);

    return createInternal(f, permission, overwrite, parentFolderLease);
  }


  /**
   * This is the version of the create call that is meant for internal usage.
   * This version is not public facing and does not perform authorization checks.
   * It is used by the public facing create call and by FolderRenamePending to
   * create the internal -RenamePending.json file.
   * @param f the path to a file to be created.
   * @param permission for the newly created file.
   * @param overwrite specifies if the file should be overwritten.
   * @param parentFolderLease lease on the parent folder.
   * @return the output stream used to write data into the newly created file .
   * @throws IOException if an IO error occurs while attempting to delete the
   * path.
   *
   */
  protected FSDataOutputStream createInternal(Path f, FsPermission permission,
                                    boolean overwrite,
                                    SelfRenewingLease parentFolderLease)
      throws FileAlreadyExistsException, IOException {

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    FileMetadata existingMetadata = store.retrieveMetadata(key);
    if (existingMetadata != null) {
      if (existingMetadata.isDir()) {
        throw new FileAlreadyExistsException("Cannot create file " + f
            + "; already exists as a directory.");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException("File already exists:" + f);
      }
      else {
        performAuthCheck(absolutePath, WasbAuthorizationOperations.WRITE, "create", absolutePath);
      }
    }

    Path parentFolder = absolutePath.getParent();
    if (parentFolder != null && parentFolder.getParent() != null) { // skip root
      // Update the parent folder last modified time if the parent folder
      // already exists.
      String parentKey = pathToKey(parentFolder);
      FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
      if (parentMetadata != null && parentMetadata.isDir() &&
        parentMetadata.getBlobMaterialization() == BlobMaterialization.Explicit) {
        if (parentFolderLease != null) {
          store.updateFolderLastModifiedTime(parentKey, parentFolderLease);
        } else {
          updateParentFolderLastModifiedTime(key);
        }
      } else {
        // Make sure that the parent folder exists.
        // Create it using inherited permissions from the first existing directory going up the path
        Path firstExisting = parentFolder.getParent();
        FileMetadata metadata = store.retrieveMetadata(pathToKey(firstExisting));
        while(metadata == null) {
          // Guaranteed to terminate properly because we will eventually hit root, which will return non-null metadata
          firstExisting = firstExisting.getParent();
          metadata = store.retrieveMetadata(pathToKey(firstExisting));
        }
        mkdirs(parentFolder, metadata.getPermissionStatus().getPermission(), true);
      }
    }

    // Mask the permission first (with the default permission mask as well).
    FsPermission masked = applyUMask(permission, UMaskApplyMode.NewFile);
    PermissionStatus permissionStatus = createPermissionStatus(masked);

    OutputStream bufOutStream;
    if (store.isPageBlobKey(key)) {
      // Store page blobs directly in-place without renames.
      bufOutStream = store.storefile(key, permissionStatus, key);
    } else {
      // This is a block blob, so open the output blob stream based on the
      // encoded key.
      //
      String keyEncoded = encodeKey(key);


      // First create a blob at the real key, pointing back to the temporary file
      // This accomplishes a few things:
      // 1. Makes sure we can create a file there.
      // 2. Makes it visible to other concurrent threads/processes/nodes what
      // we're
      // doing.
      // 3. Makes it easier to restore/cleanup data in the event of us crashing.
      store.storeEmptyLinkFile(key, keyEncoded, permissionStatus);

      // The key is encoded to point to a common container at the storage server.
      // This reduces the number of splits on the server side when load balancing.
      // Ingress to Azure storage can take advantage of earlier splits. We remove
      // the root path to the key and prefix a random GUID to the tail (or leaf
      // filename) of the key. Keys are thus broadly and randomly distributed over
      // a single container to ease load balancing on the storage server. When the
      // blob is committed it is renamed to its earlier key. Uncommitted blocks
      // are not cleaned up and we leave it to Azure storage to garbage collect
      // these
      // blocks.
      bufOutStream = new NativeAzureFsOutputStream(store.storefile(
          keyEncoded, permissionStatus, key), key, keyEncoded);
    }
    // Construct the data output stream from the buffered output stream.
    FSDataOutputStream fsOut = new FSDataOutputStream(bufOutStream, statistics);


    // Increment the counter
    instrumentation.fileCreated();

    // Return data output stream to caller.
    return fsOut;
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return delete(f, recursive, false);
  }

  /**
   * Delete file or folder with authorization checks. Most of the code
   * is duplicate of the actual delete implementation and will be merged
   * once the performance and funcional aspects are guaranteed not to
   * regress existing delete semantics.
   */
  private boolean deleteWithAuthEnabled(Path f, boolean recursive,
      boolean skipParentFolderLastModifiedTimeUpdate) throws IOException {

    LOG.debug("Deleting file: {}", f);

    Path absolutePath = makeAbsolute(f);
    Path parentPath = absolutePath.getParent();

    // If delete is issued for 'root', parentPath will be null
    // In that case, we perform auth check for root itself before
    // proceeding for deleting contents under root.
    if (parentPath != null) {
      performAuthCheck(parentPath, WasbAuthorizationOperations.WRITE, "delete", absolutePath);
    } else {
      performAuthCheck(absolutePath, WasbAuthorizationOperations.WRITE, "delete", absolutePath);
    }

    String key = pathToKey(absolutePath);

    // Capture the metadata for the path.
    FileMetadata metaFile = null;
    try {
      metaFile = store.retrieveMetadata(key);
    } catch (IOException e) {

      Throwable innerException = checkForAzureStorageException(e);

      if (innerException instanceof StorageException
          && isFileNotFoundException((StorageException) innerException)) {

        return false;
      }
      throw e;
    }

    if (null == metaFile) {
      // The path to be deleted does not exist.
      return false;
    }

    FileMetadata parentMetadata = null;
    String parentKey = null;
    if (parentPath != null) {
      parentKey = pathToKey(parentPath);

      try {
        parentMetadata = store.retrieveMetadata(parentKey);
      } catch (IOException e) {
         Throwable innerException = checkForAzureStorageException(e);
         if (innerException instanceof StorageException) {
           // Invalid State.
           // A FileNotFoundException is not thrown here as the API returns false
           // if the file not present. But not retrieving metadata here is an
           // unrecoverable state and can only happen if there is a race condition
           // hence throwing a IOException
           if (isFileNotFoundException((StorageException) innerException)) {
             throw new IOException("File " + f + " has a parent directory "
               + parentPath + " whose metadata cannot be retrieved. Can't resolve");
           }
         }
         throw e;
      }

      // Same case as unable to retrieve metadata
      if (parentMetadata == null) {
          throw new IOException("File " + f + " has a parent directory "
              + parentPath + " whose metadata cannot be retrieved. Can't resolve");
      }

      if (!parentMetadata.isDir()) {
         // Invalid state: the parent path is actually a file. Throw.
         throw new AzureException("File " + f + " has a parent directory "
             + parentPath + " which is also a file. Can't resolve.");
      }
    }

    // The path exists, determine if it is a folder containing objects,
    // an empty folder, or a simple file and take the appropriate actions.
    if (!metaFile.isDir()) {
      // The path specifies a file. We need to check the parent path
      // to make sure it's a proper materialized directory before we
      // delete the file. Otherwise we may get into a situation where
      // the file we were deleting was the last one in an implicit directory
      // (e.g. the blob store only contains the blob a/b and there's no
      // corresponding directory blob a) and that would implicitly delete
      // the directory as well, which is not correct.

      if (parentPath != null && parentPath.getParent() != null) {// Not root

        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          LOG.debug("Found an implicit parent directory while trying to"
              + " delete the file {}. Creating the directory blob for"
              + " it in {}.", f, parentKey);

          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        } else {
          if (!skipParentFolderLastModifiedTimeUpdate) {
            updateParentFolderLastModifiedTime(key);
          }
        }
      }

      // check if the file can be deleted based on sticky bit check
      // This check will be performed only when authorization is enabled
      if (isStickyBitCheckViolated(metaFile, parentMetadata)) {
        throw new WasbAuthorizationException(String.format("%s has sticky bit set. "
          + "File %s cannot be deleted.", parentPath, f));
      }

      try {
        if (store.delete(key)) {
          instrumentation.fileDeleted();
        } else {
          return false;
        }
      } catch(IOException e) {

        Throwable innerException = checkForAzureStorageException(e);

        if (innerException instanceof StorageException
            && isFileNotFoundException((StorageException) innerException)) {
          return false;
        }

       throw e;
      }
    } else {
      // The path specifies a folder. Recursively delete all entries under the
      // folder.
      LOG.debug("Directory Delete encountered: {}", f);
      if (parentPath != null && parentPath.getParent() != null) {

        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          LOG.debug("Found an implicit parent directory while trying to"
                  + " delete the directory {}. Creating the directory blob for"
                  + " it in {}. ", f, parentKey);

          store.storeEmptyFolder(parentKey,
                  createPermissionStatus(FsPermission.getDefault()));
        }
      }

      // check if the folder can be deleted based on sticky bit check on parent
      // This check will be performed only when authorization is enabled.
      if (!metaFile.getKey().equals("/")
          && isStickyBitCheckViolated(metaFile, parentMetadata)) {

        throw new WasbAuthorizationException(String.format("%s has sticky bit set. "
          + "File %s cannot be deleted.", parentPath, f));
      }

      // Iterate through folder contents and get the list of files
      // and folders that can be deleted. We might encounter IOException
      // while listing blobs. In such cases, we return false.
      ArrayList<FileMetadata> fileMetadataList = new ArrayList<>();
      boolean isPartialDelete = false;

      // Start time for list operation
      long start = Time.monotonicNow();

      try {
        // Get list of files/folders that can be deleted
        // based on authorization checks and stickybit checks
        isPartialDelete = getFolderContentsToDelete(metaFile, fileMetadataList);
      } catch (IOException e) {
        Throwable innerException = checkForAzureStorageException(e);

        if (innerException instanceof StorageException
            && isFileNotFoundException((StorageException) innerException)) {
            return false;
        }
        throw e;
      }

      long end = Time.monotonicNow();
      LOG.debug("Time taken to list {} blobs for delete operation: {} ms",
        fileMetadataList.size(), (end - start));

      // Here contents holds the list of metadata of the files and folders that can be deleted
      // under the path that is requested for delete(excluding itself).
      final FileMetadata[] contents = fileMetadataList.toArray(new FileMetadata[fileMetadataList.size()]);

      if (contents.length > 0 && !recursive) {
          // The folder is non-empty and recursive delete was not specified.
          // Throw an exception indicating that a non-recursive delete was
          // specified for a non-empty folder.
          throw new IOException("Non-recursive delete of non-empty directory "
              + f);
      }

      // Delete all files / folders in current directory stored as list in 'contents'.
      AzureFileSystemThreadTask task = new AzureFileSystemThreadTask() {
        @Override
        public boolean execute(FileMetadata file) throws IOException{
          if (!deleteFile(file.getKey(), file.isDir())) {
            LOG.warn("Attempt to delete non-existent {} {}",
                file.isDir() ? "directory" : "file",
                file.getKey());
          }
          return true;
        }
      };

      AzureFileSystemThreadPoolExecutor executor = getThreadPoolExecutor(this.deleteThreadCount,
          "AzureBlobDeleteThread", "Delete", key, AZURE_DELETE_THREADS);

      if (!executor.executeParallel(contents, task)) {
        LOG.error("Failed to delete files / subfolders in blob {}", key);
        return false;
      }

      if (metaFile.getKey().equals("/")) {
        LOG.error("Cannot delete root directory {}", f);
        return false;
      }

      // Delete the current directory if all underlying contents are deleted
      if (isPartialDelete || (store.retrieveMetadata(metaFile.getKey()) != null
          && !deleteFile(metaFile.getKey(), metaFile.isDir()))) {
        LOG.error("Failed delete directory : {}", f);
        return false;
      }

      // Update parent directory last modified time
      Path parent = absolutePath.getParent();
      if (parent != null && parent.getParent() != null) { // not root
        if (!skipParentFolderLastModifiedTimeUpdate) {
          updateParentFolderLastModifiedTime(key);
        }
      }
    }

    // File or directory was successfully deleted.
    LOG.debug("Delete Successful for : {}", f);
    return true;
  }

  private boolean deleteWithoutAuth(Path f, boolean recursive,
      boolean skipParentFolderLastModifiedTimeUpdate) throws IOException {

    LOG.debug("Deleting file: {}", f);

    Path absolutePath = makeAbsolute(f);
    Path parentPath = absolutePath.getParent();

    String key = pathToKey(absolutePath);

    // Capture the metadata for the path.
    //
    FileMetadata metaFile = null;
    try {
      metaFile = store.retrieveMetadata(key);
    } catch (IOException e) {

      Throwable innerException = checkForAzureStorageException(e);

      if (innerException instanceof StorageException
          && isFileNotFoundException((StorageException) innerException)) {

        return false;
      }
      throw e;
    }

    if (null == metaFile) {
      // The path to be deleted does not exist.
      return false;
    }

    // The path exists, determine if it is a folder containing objects,
    // an empty folder, or a simple file and take the appropriate actions.
    if (!metaFile.isDir()) {
      // The path specifies a file. We need to check the parent path
      // to make sure it's a proper materialized directory before we
      // delete the file. Otherwise we may get into a situation where
      // the file we were deleting was the last one in an implicit directory
      // (e.g. the blob store only contains the blob a/b and there's no
      // corresponding directory blob a) and that would implicitly delete
      // the directory as well, which is not correct.

      if (parentPath.getParent() != null) {// Not root
        String parentKey = pathToKey(parentPath);

        FileMetadata parentMetadata = null;
        try {
          parentMetadata = store.retrieveMetadata(parentKey);
        } catch (IOException e) {

          Throwable innerException = checkForAzureStorageException(e);

          if (innerException instanceof StorageException) {
            // Invalid State.
            // A FileNotFoundException is not thrown here as the API returns false
            // if the file not present. But not retrieving metadata here is an
            // unrecoverable state and can only happen if there is a race condition
            // hence throwing a IOException
            if (isFileNotFoundException((StorageException) innerException)) {
              throw new IOException("File " + f + " has a parent directory "
                  + parentPath + " whose metadata cannot be retrieved. Can't resolve");
            }
          }
          throw e;
        }

        // Invalid State.
        // A FileNotFoundException is not thrown here as the API returns false
        // if the file not present. But not retrieving metadata here is an
        // unrecoverable state and can only happen if there is a race condition
        // hence throwing a IOException
        if (parentMetadata == null) {
          throw new IOException("File " + f + " has a parent directory "
              + parentPath + " whose metadata cannot be retrieved. Can't resolve");
        }

        if (!parentMetadata.isDir()) {
          // Invalid state: the parent path is actually a file. Throw.
          throw new AzureException("File " + f + " has a parent directory "
              + parentPath + " which is also a file. Can't resolve.");
        }

        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          LOG.debug("Found an implicit parent directory while trying to"
              + " delete the file {}. Creating the directory blob for"
              + " it in {}.", f, parentKey);

          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        } else {
          if (!skipParentFolderLastModifiedTimeUpdate) {
            updateParentFolderLastModifiedTime(key);
          }
        }
      }

      try {
        if (store.delete(key)) {
          instrumentation.fileDeleted();
        } else {
          return false;
        }
      } catch(IOException e) {

        Throwable innerException = checkForAzureStorageException(e);

        if (innerException instanceof StorageException
            && isFileNotFoundException((StorageException) innerException)) {
          return false;
        }

       throw e;
      }
    } else {
      // The path specifies a folder. Recursively delete all entries under the
      // folder.
      LOG.debug("Directory Delete encountered: {}", f);
      if (parentPath.getParent() != null) {
        String parentKey = pathToKey(parentPath);
        FileMetadata parentMetadata = null;

        try {
          parentMetadata = store.retrieveMetadata(parentKey);
        } catch (IOException e) {

          Throwable innerException = checkForAzureStorageException(e);

          if (innerException instanceof StorageException) {
            // Invalid State.
            // A FileNotFoundException is not thrown here as the API returns false
            // if the file not present. But not retrieving metadata here is an
            // unrecoverable state and can only happen if there is a race condition
            // hence throwing a IOException
            if (isFileNotFoundException((StorageException) innerException)) {
              throw new IOException("File " + f + " has a parent directory "
                  + parentPath + " whose metadata cannot be retrieved. Can't resolve");
            }
          }
          throw e;
        }

        // Invalid State.
        // A FileNotFoundException is not thrown here as the API returns false
        // if the file not present. But not retrieving metadata here is an
        // unrecoverable state and can only happen if there is a race condition
        // hence throwing a IOException
        if (parentMetadata == null) {
          throw new IOException("File " + f + " has a parent directory "
              + parentPath + " whose metadata cannot be retrieved. Can't resolve");
        }

        if (parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          LOG.debug("Found an implicit parent directory while trying to"
              + " delete the directory {}. Creating the directory blob for"
              + " it in {}. ", f, parentKey);

          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        }
      }

      // List all the blobs in the current folder.
      String priorLastKey = null;

      // Start time for list operation
      long start = Time.monotonicNow();
      ArrayList<FileMetadata> fileMetadataList = new ArrayList<FileMetadata>();

      // List all the files in the folder with AZURE_UNBOUNDED_DEPTH depth.
      do {
        try {
          PartialListing listing = store.listAll(key, AZURE_LIST_ALL,
            AZURE_UNBOUNDED_DEPTH, priorLastKey);
          for(FileMetadata file : listing.getFiles()) {
            fileMetadataList.add(file);
          }
          priorLastKey = listing.getPriorLastKey();
        } catch (IOException e) {
          Throwable innerException = checkForAzureStorageException(e);

          if (innerException instanceof StorageException
              && isFileNotFoundException((StorageException) innerException)) {
            return false;
          }

          throw e;
        }
      } while (priorLastKey != null);

      long end = Time.monotonicNow();
      LOG.debug("Time taken to list {} blobs for delete operation: {} ms", fileMetadataList.size(), (end - start));

      final FileMetadata[] contents = fileMetadataList.toArray(new FileMetadata[fileMetadataList.size()]);

      if (contents.length > 0) {
        if (!recursive) {
          // The folder is non-empty and recursive delete was not specified.
          // Throw an exception indicating that a non-recursive delete was
          // specified for a non-empty folder.
          throw new IOException("Non-recursive delete of non-empty directory "+ f);
        }
      }

      // Delete all files / folders in current directory stored as list in 'contents'.
      AzureFileSystemThreadTask task = new AzureFileSystemThreadTask() {
        @Override
        public boolean execute(FileMetadata file) throws IOException{
          if (!deleteFile(file.getKey(), file.isDir())) {
            LOG.warn("Attempt to delete non-existent {} {}",
                file.isDir() ? "directory" : "file",
                file.getKey());
          }
          return true;
        }
      };

      AzureFileSystemThreadPoolExecutor executor = getThreadPoolExecutor(this.deleteThreadCount,
          "AzureBlobDeleteThread", "Delete", key, AZURE_DELETE_THREADS);

      if (!executor.executeParallel(contents, task)) {
        LOG.error("Failed to delete files / subfolders in blob {}", key);
        return false;
      }

      // Delete the current directory
      if (store.retrieveMetadata(metaFile.getKey()) != null
          && !deleteFile(metaFile.getKey(), metaFile.isDir())) {
        LOG.error("Failed delete directory : {}", f);
        return false;
      }

      // Update parent directory last modified time
      Path parent = absolutePath.getParent();
      if (parent != null && parent.getParent() != null) { // not root
        if (!skipParentFolderLastModifiedTimeUpdate) {
          updateParentFolderLastModifiedTime(key);
        }
      }
    }

    // File or directory was successfully deleted.
    LOG.debug("Delete Successful for : {}", f);
    return true;
  }

  /**
   * Delete the specified file or folder. The parameter
   * skipParentFolderLastModifiedTimeUpdate
   * is used in the case of atomic folder rename redo. In that case, there is
   * a lease on the parent folder, so (without reworking the code) modifying
   * the parent folder update time will fail because of a conflict with the
   * lease. Since we are going to delete the folder soon anyway so accurate
   * modified time is not necessary, it's easier to just skip
   * the modified time update.
   *
   * @param f file path to be deleted.
   * @param recursive specify deleting recursively or not.
   * @param skipParentFolderLastModifiedTimeUpdate If true, don't update the folder last
   * modified time.
   * @return true if and only if the file is deleted
   * @throws IOException Thrown when fail to delete file or directory.
   */
  public boolean delete(Path f, boolean recursive,
      boolean skipParentFolderLastModifiedTimeUpdate) throws IOException {

    if (this.azureAuthorization) {
      return deleteWithAuthEnabled(f, recursive,
        skipParentFolderLastModifiedTimeUpdate);
    } else {
      return deleteWithoutAuth(f, recursive,
        skipParentFolderLastModifiedTimeUpdate);
    }
  }

  public AzureFileSystemThreadPoolExecutor getThreadPoolExecutor(int threadCount,
      String threadNamePrefix, String operation, String key, String config) {
    return new AzureFileSystemThreadPoolExecutor(threadCount, threadNamePrefix, operation, key, config);
  }

  /**
   * Gets list of contents that can be deleted based on authorization check calls
   * performed on the sub-tree for the folderToDelete.
   *
   * @param folderToDelete - metadata of the folder whose delete is requested.
   * @param finalList - list of metadata of all files/folders that can be deleted .
   *
   * @return 'true' only if all the contents of the folderToDelete can be deleted
   * @throws IOException Thrown when current user cannot be retrieved.
   */
  private boolean getFolderContentsToDelete(FileMetadata folderToDelete,
      ArrayList<FileMetadata> finalList) throws IOException {

    final int maxListingDepth = 1;
    Stack<FileMetadata> foldersToProcess = new Stack<FileMetadata>();
    HashMap<String, FileMetadata> folderContentsMap = new HashMap<String, FileMetadata>();

    boolean isPartialDelete = false;

    Path pathToDelete = makeAbsolute(keyToPath(folderToDelete.getKey()));
    foldersToProcess.push(folderToDelete);

    while (!foldersToProcess.empty()) {

      FileMetadata currentFolder = foldersToProcess.pop();
      Path currentPath = makeAbsolute(keyToPath(currentFolder.getKey()));
      boolean canDeleteChildren = true;

      // If authorization is enabled, check for 'write' permission on current folder
      // This check maps to subfolders 'write' check for deleting contents recursively.
      try {
        performAuthCheck(currentPath, WasbAuthorizationOperations.WRITE, "delete", pathToDelete);
      } catch (WasbAuthorizationException we) {
        LOG.debug("Authorization check failed for {}", currentPath);
        // We cannot delete the children of currentFolder since 'write' check on parent failed
        canDeleteChildren = false;
      }

      if (canDeleteChildren) {

        // get immediate children list
        ArrayList<FileMetadata> fileMetadataList = getChildrenMetadata(currentFolder.getKey(),
            maxListingDepth);

        // Process children of currentFolder and add them to list of contents
        // that can be deleted. We Perform stickybit check on every file and
        // folder under currentFolder in case stickybit is set on currentFolder.
        for (FileMetadata childItem : fileMetadataList) {
          if (isStickyBitCheckViolated(childItem, currentFolder, false)) {
            // Stickybit check failed for the childItem that is being processed.
            // This file/folder cannot be deleted and neither can the parent paths be deleted.
            // Remove parent paths from list of contents that can be deleted.
            canDeleteChildren = false;
            Path filePath = makeAbsolute(keyToPath(childItem.getKey()));
            LOG.error("User does not have permissions to delete {}. "
              + "Parent directory has sticky bit set.", filePath);
          } else {
            // push the child directories to the stack to process their contents
            if (childItem.isDir()) {
              foldersToProcess.push(childItem);
            }
            // Add items to list of contents that can be deleted.
            folderContentsMap.put(childItem.getKey(), childItem);
          }
        }

      } else {
        // Cannot delete children since parent permission check has not passed and
        // if there are files/folders under currentFolder they will not be deleted.
        LOG.error("Authorization check failed. Files or folders under {} "
          + "will not be processed for deletion.", currentPath);
      }

      if (!canDeleteChildren) {
        // We reach here if
        // 1. cannot delete children since 'write' check on parent failed or
        // 2. One of the files under the current folder cannot be deleted due to stickybit check.
        // In this case we remove all the parent paths from the list of contents
        // that can be deleted till we reach the original path of delete request
        String pathToRemove = currentFolder.getKey();
        while (!pathToRemove.equals(folderToDelete.getKey())) {
          if (folderContentsMap.containsKey(pathToRemove)) {
            LOG.debug("Cannot delete {} since some of its contents "
              + "cannot be deleted", pathToRemove);
            folderContentsMap.remove(pathToRemove);
          }
          Path parentPath = keyToPath(pathToRemove).getParent();
          pathToRemove = pathToKey(parentPath);
        }
        // Since one or more files/folders cannot be deleted return value should indicate
        // partial delete, so that the delete on the path requested by user is not performed
        isPartialDelete = true;
      }
    }

    // final list of contents that can be deleted
    for (HashMap.Entry<String, FileMetadata> entry : folderContentsMap.entrySet()) {
      finalList.add(entry.getValue());
    }

    return isPartialDelete;
  }

  private ArrayList<FileMetadata> getChildrenMetadata(String key, int maxListingDepth)
    throws IOException {

    String priorLastKey = null;
    ArrayList<FileMetadata> fileMetadataList = new ArrayList<FileMetadata>();
    do {
       PartialListing listing = store.listAll(key, AZURE_LIST_ALL,
         maxListingDepth, priorLastKey);
       for (FileMetadata file : listing.getFiles()) {
         fileMetadataList.add(file);
       }
       priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);

    return fileMetadataList;
  }

  private boolean isStickyBitCheckViolated(FileMetadata metaData,
    FileMetadata parentMetadata, boolean throwOnException) throws IOException {
      try {
        return isStickyBitCheckViolated(metaData, parentMetadata);
      } catch (FileNotFoundException ex) {
        if (throwOnException) {
          throw ex;
        } else {
          LOG.debug("Encountered FileNotFoundException while performing "
            + "stickybit check operation for {}", metaData.getKey());
          // swallow exception and return that stickyBit check has been violated
          return true;
        }
      }
  }

  /**
   * Checks if the Current user is not permitted access to a file/folder when
   * sticky bit is set on parent path. Only the owner of parent path
   * and owner of the file/folder itself are permitted to perform certain
   * operations on file/folder based on sticky bit check. Sticky bit check will
   * be performed only when authorization is enabled.
   *
   * @param metaData - metadata of the file/folder whose parent has sticky bit set.
   * @param parentMetadata - metadata of the parent.
   *
   * @return true if Current user violates stickybit check
   * @throws IOException Thrown when current user cannot be retrieved.
   */
   private boolean isStickyBitCheckViolated(FileMetadata metaData,
    FileMetadata parentMetadata) throws IOException {

    // In case stickybit check should not be performed,
    // return value should indicate stickybit check is not violated.
    if (!this.azureAuthorization) {
      return false;
    }

    // This should never happen when the sticky bit check is invoked.
    if (parentMetadata == null) {
      throw new FileNotFoundException(
        String.format("Parent metadata for '%s' not found!", metaData.getKey()));
    }

    // stickybit is not set on parent and hence cannot be violated
    if (!parentMetadata.getPermissionStatus().getPermission().getStickyBit()) {
      return false;
    }

    String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    String parentDirectoryOwner = parentMetadata.getPermissionStatus().getUserName();
    String currentFileOwner = metaData.getPermissionStatus().getUserName();

    // Files/Folders with no owner set will not pass stickybit check
    if ((parentDirectoryOwner.equalsIgnoreCase(currentUser))
      || currentFileOwner.equalsIgnoreCase(currentUser)) {

      return false;
    }
    return true;
  }

  /**
   * Delete the specified file or directory and increment metrics.
   * If the file or directory does not exist, the operation returns false.
   * @param path the path to a file or directory.
   * @param isDir true if the path is a directory; otherwise false.
   * @return true if delete is successful; otherwise false.
   * @throws IOException if an IO error occurs while attempting to delete the
   * path.
   *
   */
  @VisibleForTesting
  boolean deleteFile(String path, boolean isDir) throws IOException {
    if (!store.delete(path)) {
      return false;
    }

    if (isDir) {
      instrumentation.directoryDeleted();
    } else {
      instrumentation.fileDeleted();
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws FileNotFoundException, IOException {

    LOG.debug("Getting the file status for {}", f.toString());
    return getFileStatusInternal(f);
  }

  /**
   * Checks if a given path exists in the filesystem.
   * Calls getFileStatusInternal and has the same costs
   * as the public facing exists call.
   * This internal version of the exists call does not perform
   * authorization checks, and is used internally by various filesystem
   * operations that need to check if the parent/ancestor/path exist.
   * The idea is to avoid having to configure authorization policies for
   * these internal calls.
   * @param f the path to a file or directory.
   * @return true if path exists; otherwise false.
   * @throws IOException if an IO error occurs while attempting to check
   * for existence of the path.
   *
   */
  protected boolean existsInternal(Path f) throws IOException {
    try {
      this.getFileStatusInternal(f);
      return true;
    } catch (FileNotFoundException fnfe) {
      return false;
    }
  }

  /**
   * Inner implementation of {@link #getFileStatus(Path)}.
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException Other failure
   */
  private FileStatus getFileStatusInternal(Path f) throws FileNotFoundException, IOException {

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.length() == 0) { // root always exists
      return newDirectory(null, absolutePath);
    }

    // The path is either a folder or a file. Retrieve metadata to
    // determine if it is a directory or file.
    FileMetadata meta = null;
    try {
      meta = store.retrieveMetadata(key);
    } catch(Exception ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

          throw new FileNotFoundException(String.format("%s is not found", key));
       }

      throw ex;
    }

    if (meta != null) {
      if (meta.isDir()) {
        // The path is a folder with files in it.
        //

        LOG.debug("Path {} is a folder.", f.toString());

        // If a rename operation for the folder was pending, redo it.
        // Then the file does not exist, so signal that.
        if (conditionalRedoFolderRename(f)) {
          throw new FileNotFoundException(
              absolutePath + ": No such file or directory.");
        }

        // Return reference to the directory object.
        return newDirectory(meta, absolutePath);
      }

      // The path is a file.
      LOG.debug("Found the path: {} as a file.", f.toString());

      // Return with reference to a file object.
      return newFile(meta, absolutePath);
    }

    // File not found. Throw exception no such file or directory.
    //
    throw new FileNotFoundException(
        absolutePath + ": No such file or directory.");
  }

  // Return true if there is a rename pending and we redo it, otherwise false.
  private boolean conditionalRedoFolderRename(Path f) throws IOException {

    // Can't rename /, so return immediately in that case.
    if (f.getName().equals("")) {
      return false;
    }

    // Check if there is a -RenamePending.json file for this folder, and if so,
    // redo the rename.
    Path absoluteRenamePendingFile = renamePendingFilePath(f);
    if (existsInternal(absoluteRenamePendingFile)) {
      FolderRenamePending pending =
          new FolderRenamePending(absoluteRenamePendingFile, this);
      pending.redo();
      return true;
    } else {
      return false;
    }
  }

  // Return the path name that would be used for rename of folder with path f.
  private Path renamePendingFilePath(Path f) {
    Path absPath = makeAbsolute(f);
    String key = pathToKey(absPath);
    key += "-RenamePending.json";
    return keyToPath(key);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Retrieve the status of a given path if it is a file, or of all the
   * contained files if it is a directory.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {

    LOG.debug("Listing status for {}", f.toString());

    Path absolutePath = makeAbsolute(f);

    performAuthCheck(absolutePath, WasbAuthorizationOperations.READ, "liststatus", absolutePath);

    String key = pathToKey(absolutePath);
    Set<FileStatus> status = new TreeSet<FileStatus>();
    FileMetadata meta = null;
    try {
      meta = store.retrieveMetadata(key);
    } catch (IOException ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("%s is not found", f));
      }

      throw ex;
    }

    if (meta != null) {
      if (!meta.isDir()) {

        LOG.debug("Found path as a file");

        return new FileStatus[] { newFile(meta, absolutePath) };
      }

      String partialKey = null;
      PartialListing listing = null;

      try {
        listing  = store.list(key, AZURE_LIST_ALL, 1, partialKey);
      } catch (IOException ex) {

        Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

        if (innerException instanceof StorageException
            && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

            throw new FileNotFoundException(String.format("%s is not found", key));
        }

        throw ex;
      }
      // NOTE: We don't check for Null condition as the Store API should return
      // an empty list if there are not listing.

      // For any -RenamePending.json files in the listing,
      // push the rename forward.
      boolean renamed = conditionalRedoFolderRenames(listing);

      // If any renames were redone, get another listing,
      // since the current one may have changed due to the redo.
      if (renamed) {
       listing = null;
       try {
         listing = store.list(key, AZURE_LIST_ALL, 1, partialKey);
       } catch (IOException ex) {
         Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

         if (innerException instanceof StorageException
             && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

           throw new FileNotFoundException(String.format("%s is not found", key));
         }

         throw ex;
       }
      }

      // NOTE: We don't check for Null condition as the Store API should return
      // and empty list if there are not listing.

      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());

        // Test whether the metadata represents a file or directory and
        // add the appropriate metadata object.
        //
        // Note: There was a very old bug here where directories were added
        // to the status set as files flattening out recursive listings
        // using "-lsr" down the file system hierarchy.
        if (fileMetadata.isDir()) {
          // Make sure we hide the temp upload folder
          if (fileMetadata.getKey().equals(AZURE_TEMP_FOLDER)) {
            // Don't expose that.
            continue;
          }
          status.add(newDirectory(fileMetadata, subpath));
        } else {
          status.add(newFile(fileMetadata, subpath));
        }
      }

      LOG.debug("Found path as a directory with {}"
          + " files in it.", status.size());

    } else {
      // There is no metadata found for the path.
      LOG.debug("Did not find any metadata for path: {}", key);

      throw new FileNotFoundException("File" + f + " does not exist.");
    }

    return status.toArray(new FileStatus[0]);
  }

  // Redo any folder renames needed if there are rename pending files in the
  // directory listing. Return true if one or more redo operations were done.
  private boolean conditionalRedoFolderRenames(PartialListing listing)
      throws IllegalArgumentException, IOException {
    boolean renamed = false;
    for (FileMetadata fileMetadata : listing.getFiles()) {
      Path subpath = keyToPath(fileMetadata.getKey());
      if (isRenamePendingFile(subpath)) {
        FolderRenamePending pending =
            new FolderRenamePending(subpath, this);
        pending.redo();
        renamed = true;
      }
    }
    return renamed;
  }

  // True if this is a folder rename pending file, else false.
  private boolean isRenamePendingFile(Path path) {
    return path.toString().endsWith(FolderRenamePending.SUFFIX);
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus (
        meta.getLength(),
        false,
        1,
        blockSize,
        meta.getLastModified(),
        0,
        meta.getPermissionStatus().getPermission(),
        meta.getPermissionStatus().getUserName(),
        meta.getPermissionStatus().getGroupName(),
        path.makeQualified(getUri(), getWorkingDirectory()));
  }

  private FileStatus newDirectory(FileMetadata meta, Path path) {
    return new FileStatus (
        0,
        true,
        1,
        blockSize,
        meta == null ? 0 : meta.getLastModified(),
        0,
        meta == null ? FsPermission.getDefault() : meta.getPermissionStatus().getPermission(),
        meta == null ? "" : meta.getPermissionStatus().getUserName(),
        meta == null ? "" : meta.getPermissionStatus().getGroupName(),
        path.makeQualified(getUri(), getWorkingDirectory()));
  }

  private static enum UMaskApplyMode {
    NewFile,
    NewDirectory,
    NewDirectoryNoUmask,
    ChangeExistingFile,
    ChangeExistingDirectory,
  }

  /**
   * Applies the applicable UMASK's on the given permission.
   *
   * @param permission
   *          The permission to mask.
   * @param applyMode
   *          Whether to also apply the default umask.
   * @return The masked persmission.
   */
  private FsPermission applyUMask(final FsPermission permission,
      final UMaskApplyMode applyMode) {
    FsPermission newPermission = new FsPermission(permission);
    // Apply the default umask - this applies for new files or directories.
    if (applyMode == UMaskApplyMode.NewFile
        || applyMode == UMaskApplyMode.NewDirectory) {
      newPermission = newPermission
          .applyUMask(FsPermission.getUMask(getConf()));
    }
    return newPermission;
  }

  /**
   * Creates the PermissionStatus object to use for the given permission, based
   * on the current user in context.
   *
   * @param permission
   *          The permission for the file.
   * @return The permission status object to use.
   * @throws IOException
   *           If login fails in getCurrentUser
   */
  @VisibleForTesting
  PermissionStatus createPermissionStatus(FsPermission permission)
    throws IOException {
    // Create the permission status for this file based on current user
    return new PermissionStatus(
        UserGroupInformation.getCurrentUser().getShortUserName(),
        getConf().get(AZURE_DEFAULT_GROUP_PROPERTY_NAME,
            AZURE_DEFAULT_GROUP_DEFAULT),
        permission);
  }

  private Path getAncestor(Path f) throws IOException {

    for (Path current = f, parent = current.getParent();
         parent != null; // Stop when you get to the root
         current = parent, parent = current.getParent()) {

      String currentKey = pathToKey(current);
      FileMetadata currentMetadata = store.retrieveMetadata(currentKey);
      if (currentMetadata != null && currentMetadata.isDir()) {
        Path ancestor = keyToPath(currentMetadata.getKey());
        LOG.debug("Found ancestor {}, for path: {}", ancestor.toString(), f.toString());
        return ancestor;
      }
    }

    return new Path("/");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return mkdirs(f, permission, false);
  }

  public boolean mkdirs(Path f, FsPermission permission, boolean noUmask) throws IOException {

    LOG.debug("Creating directory: {}", f.toString());

    if (containsColon(f)) {
      throw new IOException("Cannot create directory " + f
          + " through WASB that has colons in the name");
    }

    Path absolutePath = makeAbsolute(f);
    Path ancestor = getAncestor(absolutePath);

    if (absolutePath.equals(ancestor)) {
      return true;
    }

    performAuthCheck(ancestor, WasbAuthorizationOperations.WRITE, "mkdirs", absolutePath);

    PermissionStatus permissionStatus = null;
    if(noUmask) {
      // ensure owner still has wx permissions at the minimum
      permissionStatus = createPermissionStatus(
          applyUMask(FsPermission.createImmutable((short) (permission.toShort() | USER_WX_PERMISION)),
              UMaskApplyMode.NewDirectoryNoUmask));
    } else {
      permissionStatus = createPermissionStatus(
          applyUMask(permission, UMaskApplyMode.NewDirectory));
    }


    ArrayList<String> keysToCreateAsFolder = new ArrayList<String>();
    // Check that there is no file in the parent chain of the given path.
    for (Path current = absolutePath, parent = current.getParent();
        parent != null; // Stop when you get to the root
        current = parent, parent = current.getParent()) {
      String currentKey = pathToKey(current);
      FileMetadata currentMetadata = store.retrieveMetadata(currentKey);
      if (currentMetadata != null && !currentMetadata.isDir()) {
        throw new FileAlreadyExistsException("Cannot create directory " + f + " because "
            + current + " is an existing file.");
      } else if (currentMetadata == null) {
        keysToCreateAsFolder.add(currentKey);
      }
    }

    for (String currentKey : keysToCreateAsFolder) {
      store.storeEmptyFolder(currentKey, permissionStatus);
    }

    instrumentation.directoryCreated();

    // otherwise throws exception
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws FileNotFoundException, IOException {

    LOG.debug("Opening file: {}", f.toString());

    Path absolutePath = makeAbsolute(f);

    performAuthCheck(absolutePath, WasbAuthorizationOperations.READ, "read", absolutePath);

    String key = pathToKey(absolutePath);
    FileMetadata meta = null;
    try {
      meta = store.retrieveMetadata(key);
    } catch(Exception ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("%s is not found", key));
      }

      throw ex;
    }

    if (meta == null) {
      throw new FileNotFoundException(f.toString());
    }
    if (meta.isDir()) {
      throw new FileNotFoundException(f.toString()
          + " is a directory not a file.");
    }

    InputStream inputStream;
    try {
      inputStream = store.retrieve(key);
    } catch(Exception ex) {
      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("%s is not found", key));
      }

      throw ex;
    }

    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeAzureFsInputStream(inputStream, key, meta.getLength()), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws FileNotFoundException, IOException {

    FolderRenamePending renamePending = null;

    LOG.debug("Moving {} to {}", src, dst);

    if (containsColon(dst)) {
      throw new IOException("Cannot rename to file " + dst
          + " through WASB that has colons in the name");
    }

    Path absoluteSrcPath = makeAbsolute(src);
    Path srcParentFolder = absoluteSrcPath.getParent();

    if (srcParentFolder == null) {
      // Cannot rename root of file system
      return false;
    }

    String srcKey = pathToKey(absoluteSrcPath);

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    performAuthCheck(srcParentFolder, WasbAuthorizationOperations.WRITE, "rename",
        absoluteSrcPath);

    if (this.azureAuthorization) {
      try {
        performStickyBitCheckForRenameOperation(absoluteSrcPath, srcParentFolder);
      } catch (FileNotFoundException ex) {
        return false;
      } catch (IOException ex) {
        Throwable innerException = checkForAzureStorageException(ex);
        if (innerException instanceof StorageException
          && isFileNotFoundException((StorageException) innerException)) {
          LOG.debug("Encountered FileNotFound Exception when performing sticky bit check "
            + "on {}. Failing rename", srcKey);
          return false;
        }
        throw ex;
      }
    }

    // Figure out the final destination
    Path absoluteDstPath = makeAbsolute(dst);
    Path dstParentFolder = absoluteDstPath.getParent();

    String dstKey = pathToKey(absoluteDstPath);
    FileMetadata dstMetadata = null;
    try {
      dstMetadata = store.retrieveMetadata(dstKey);
    } catch (IOException ex) {

      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      // A BlobNotFound storage exception in only thrown from retrieveMetadata API when
      // there is a race condition. If there is another thread which deletes the destination
      // file or folder, then this thread calling rename should be able to continue with
      // rename gracefully. Hence the StorageException is swallowed here.
      if (innerException instanceof StorageException) {
        if (NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {
          LOG.debug("BlobNotFound exception encountered for Destination key : {}. "
              + "Swallowing the exception to handle race condition gracefully", dstKey);
        }
      } else {
        throw ex;
      }
    }

    if (dstMetadata != null && dstMetadata.isDir()) {
      // It's an existing directory.
      performAuthCheck(absoluteDstPath, WasbAuthorizationOperations.WRITE, "rename",
          absoluteDstPath);

      dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      LOG.debug("Destination {} "
          + " is a directory, adjusted the destination to be {}", dst, dstKey);
    } else if (dstMetadata != null) {
      // Attempting to overwrite a file using rename()
      LOG.debug("Destination {}"
          + " is an already existing file, failing the rename.", dst);
      return false;
    } else {
      // Check that the parent directory exists.
      FileMetadata parentOfDestMetadata = null;
      try {
        parentOfDestMetadata = store.retrieveMetadata(pathToKey(absoluteDstPath.getParent()));
      } catch (IOException ex) {

        Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

        if (innerException instanceof StorageException
            && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

          LOG.debug("Parent of destination {} doesn't exists. Failing rename", dst);
          return false;
        }

        throw ex;
      }

      if (parentOfDestMetadata == null) {
        LOG.debug("Parent of the destination {}"
            + " doesn't exist, failing the rename.", dst);
        return false;
      } else if (!parentOfDestMetadata.isDir()) {
        LOG.debug("Parent of the destination {}"
            + " is a file, failing the rename.", dst);
        return false;
      } else {
        performAuthCheck(dstParentFolder, WasbAuthorizationOperations.WRITE,
          "rename", absoluteDstPath);
      }
    }
    FileMetadata srcMetadata = null;
    try {
      srcMetadata = store.retrieveMetadata(srcKey);
    } catch (IOException ex) {
      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        LOG.debug("Source {} doesn't exists. Failing rename", src);
        return false;
      }

      throw ex;
    }

    if (srcMetadata == null) {
      // Source doesn't exist
      LOG.debug("Source {} doesn't exist, failing the rename.", src);
      return false;
    } else if (!srcMetadata.isDir()) {
      LOG.debug("Source {} found as a file, renaming.", src);
      try {
        // HADOOP-15086 - file rename must ensure that the destination does
        // not exist.  The fix is targeted to this call only to avoid
        // regressions.  Other call sites are attempting to rename temporary
        // files, redo a failed rename operation, or rename a directory
        // recursively; for these cases the destination may exist.
        store.rename(srcKey, dstKey, false, null,
            false);
      } catch(IOException ex) {
        Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

        if (innerException instanceof StorageException) {
          if (NativeAzureFileSystemHelper.isFileNotFoundException(
              (StorageException) innerException)) {
            LOG.debug("BlobNotFoundException encountered. Failing rename", src);
            return false;
          }
          if (NativeAzureFileSystemHelper.isBlobAlreadyExistsConflict(
              (StorageException) innerException)) {
            LOG.debug("Destination BlobAlreadyExists. Failing rename", src);
            return false;
          }
        }

        throw ex;
      }
    } else {

      // Prepare for, execute and clean up after of all files in folder, and
      // the root file, and update the last modified time of the source and
      // target parent folders. The operation can be redone if it fails part
      // way through, by applying the "Rename Pending" file.

      // The following code (internally) only does atomic rename preparation
      // and lease management for page blob folders, limiting the scope of the
      // operation to HBase log file folders, where atomic rename is required.
      // In the future, we could generalize it easily to all folders.
      renamePending = prepareAtomicFolderRename(srcKey, dstKey);
      renamePending.execute();

      LOG.debug("Renamed {} to {} successfully.", src, dst);
      renamePending.cleanup();
      return true;
    }

    // Update the last-modified time of the parent folders of both source
    // and destination.
    updateParentFolderLastModifiedTime(srcKey);
    updateParentFolderLastModifiedTime(dstKey);

    LOG.debug("Renamed {} to {} successfully.", src, dst);
    return true;
  }

  /**
   * Update the last-modified time of the parent folder of the file
   * identified by key.
   * @param key
   * @throws IOException
   */
  private void updateParentFolderLastModifiedTime(String key)
      throws IOException {
    Path parent = makeAbsolute(keyToPath(key)).getParent();
    if (parent != null && parent.getParent() != null) { // not root
      String parentKey = pathToKey(parent);

      // ensure the parent is a materialized folder
      FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
      // The metadata could be null if the implicit folder only contains a
      // single file. In this case, the parent folder no longer exists if the
      // file is renamed; so we can safely ignore the null pointer case.
      if (parentMetadata != null) {
        if (parentMetadata.isDir()
            && parentMetadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        }

        if (store.isAtomicRenameKey(parentKey)) {
          SelfRenewingLease lease = null;
          try {
            lease = leaseSourceFolder(parentKey);
            store.updateFolderLastModifiedTime(parentKey, lease);
          } catch (AzureException e) {
            String errorCode = "";
            try {
              StorageException e2 = (StorageException) e.getCause();
              errorCode = e2.getErrorCode();
            } catch (Exception e3) {
              // do nothing if cast fails
            }
            if (errorCode.equals("BlobNotFound")) {
              throw new FileNotFoundException("Folder does not exist: " + parentKey);
            }
            LOG.warn("Got unexpected exception trying to get lease on {}. {}",
                parentKey, e.getMessage());
            throw e;
          } finally {
            try {
              if (lease != null) {
                lease.free();
              }
            } catch (Exception e) {
              LOG.error("Unable to free lease on {}", parentKey, e);
            }
          }
        } else {
          store.updateFolderLastModifiedTime(parentKey, null);
        }
      }
    }
  }

  /**
   * If the source is a page blob folder,
   * prepare to rename this folder atomically. This means to get exclusive
   * access to the source folder, and record the actions to be performed for
   * this rename in a "Rename Pending" file. This code was designed to
   * meet the needs of HBase, which requires atomic rename of write-ahead log
   * (WAL) folders for correctness.
   *
   * Before calling this method, the caller must ensure that the source is a
   * folder.
   *
   * For non-page-blob directories, prepare the in-memory information needed,
   * but don't take the lease or write the redo file. This is done to limit the
   * scope of atomic folder rename to HBase, at least at the time of writing
   * this code.
   *
   * @param srcKey Source folder name.
   * @param dstKey Destination folder name.
   * @throws IOException
   */
  @VisibleForTesting
  FolderRenamePending prepareAtomicFolderRename(
      String srcKey, String dstKey) throws IOException {

    if (store.isAtomicRenameKey(srcKey)) {

      // Block unwanted concurrent access to source folder.
      SelfRenewingLease lease = leaseSourceFolder(srcKey);

      // Prepare in-memory information needed to do or redo a folder rename.
      FolderRenamePending renamePending =
          new FolderRenamePending(srcKey, dstKey, lease, this);

      // Save it to persistent storage to help recover if the operation fails.
      renamePending.writeFile(this);
      return renamePending;
    } else {
      FolderRenamePending renamePending =
          new FolderRenamePending(srcKey, dstKey, null, this);
      return renamePending;
    }
  }

  /**
   * Get a self-renewing Azure blob lease on the source folder zero-byte file.
   */
  private SelfRenewingLease leaseSourceFolder(String srcKey)
      throws AzureException {
    return store.acquireLease(srcKey);
  }

  /**
   * Performs sticky bit check on source folder for rename operation.
   *
   * @param srcPath - path which is to be renamed.
   * @param srcParentPath - parent to srcPath to check for stickybit check.
   * @throws FileNotFoundException if srcPath or srcParentPath do not exist.
   * @throws WasbAuthorizationException if stickybit check is violated.
   * @throws IOException when retrieving metadata operation fails.
   */
  private void performStickyBitCheckForRenameOperation(Path srcPath,
      Path srcParentPath)
    throws FileNotFoundException, WasbAuthorizationException, IOException {

    String srcKey = pathToKey(srcPath);
    FileMetadata srcMetadata = null;
    srcMetadata = store.retrieveMetadata(srcKey);
    if (srcMetadata == null) {
      LOG.debug("Source {} doesn't exist. Failing rename.", srcPath);
      throw new FileNotFoundException(
        String.format("%s does not exist.", srcPath));
    }

    String parentkey = pathToKey(srcParentPath);
    FileMetadata parentMetadata = store.retrieveMetadata(parentkey);
    if (parentMetadata == null) {
      LOG.debug("Path {} doesn't exist, failing rename.", srcParentPath);
      throw new FileNotFoundException(
        String.format("%s does not exist.", parentkey));
    }

    if (isStickyBitCheckViolated(srcMetadata, parentMetadata)) {
      throw new WasbAuthorizationException(
        String.format("Rename operation for %s is not permitted."
        + " Details : Stickybit check failed.", srcPath));
    }
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file. For WASB we'll just lie and give
   * fake hosts to make sure we get many splits in MR jobs.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) throws IOException {
    if (file == null) {
      return null;
    }

    if ((start < 0) || (len < 0)) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];
    }
    final String blobLocationHost = getConf().get(
        AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME,
        AZURE_BLOCK_LOCATION_HOST_DEFAULT);
    final String[] name = { blobLocationHost };
    final String[] host = { blobLocationHost };
    long blockSize = file.getBlockSize();
    if (blockSize <= 0) {
      throw new IllegalArgumentException(
          "The block size for the given file is not a positive number: "
              + blockSize);
    }
    int numberOfLocations = (int) (len / blockSize)
        + ((len % blockSize == 0) ? 0 : 1);
    BlockLocation[] locations = new BlockLocation[numberOfLocations];
    for (int i = 0; i < locations.length; i++) {
      long currentOffset = start + (i * blockSize);
      long currentLength = Math.min(blockSize, start + len - currentOffset);
      locations[i] = new BlockLocation(name, host, currentOffset, currentLength);
    }
    return locations;
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws FileNotFoundException, IOException {
    Path absolutePath = makeAbsolute(p);

    String key = pathToKey(absolutePath);
    FileMetadata metadata = null;
    try {
      metadata = store.retrieveMetadata(key);
    } catch (IOException ex) {
      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("File %s doesn't exists.", p));
      }

      throw ex;
    }

    if (metadata == null) {
      throw new FileNotFoundException("File doesn't exist: " + p);
    }

    // If authorization is enabled, check if the user is
    // part of chmod allowed list or a daemon user or owner of the file/folder
    if (azureAuthorization) {
      UserGroupInformation currentUgi = UserGroupInformation.getCurrentUser();

      // Check if the user is part of chown allowed list or a daemon user.
      if (!isAllowedUser(currentUgi.getShortUserName(), chmodAllowedUsers)
          && !isAllowedUser(currentUgi.getShortUserName(), daemonUsers)) {

        //Check if the user is the owner of the file.
        String owner = metadata.getPermissionStatus().getUserName();
        if (!currentUgi.getShortUserName().equals(owner)) {
          throw new WasbAuthorizationException(
              String.format("user '%s' does not have the privilege to "
                  + "change the permission of files/folders.",
                      currentUgi.getShortUserName()));
        }
      }
    }

    permission = applyUMask(permission,
        metadata.isDir() ? UMaskApplyMode.ChangeExistingDirectory
            : UMaskApplyMode.ChangeExistingFile);
    if (metadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
      // It's an implicit folder, need to materialize it.
      store.storeEmptyFolder(key, createPermissionStatus(permission));
    } else if (!metadata.getPermissionStatus().getPermission().
        equals(permission)) {
      store.changePermissionStatus(key, new PermissionStatus(
          metadata.getPermissionStatus().getUserName(),
          metadata.getPermissionStatus().getGroupName(),
          permission));
    }
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    Path absolutePath = makeAbsolute(p);

    String key = pathToKey(absolutePath);
    FileMetadata metadata = null;

    try {
      metadata = store.retrieveMetadata(key);
    } catch (IOException ex) {
      Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);

      if (innerException instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException)) {

        throw new FileNotFoundException(String.format("File %s doesn't exists.", p));
      }

      throw ex;
    }

    if (metadata == null) {
      throw new FileNotFoundException("File doesn't exist: " + p);
    }

    /* If authorization is enabled, check if the user has privileges
    *  to change the ownership of file/folder
    */
    if (this.azureAuthorization && username != null) {
      UserGroupInformation currentUgi = UserGroupInformation.getCurrentUser();

      if (!isAllowedUser(currentUgi.getShortUserName(),
          chownAllowedUsers)) {
          throw new WasbAuthorizationException(
            String.format("user '%s' does not have the privilege to change "
                + "the ownership of files/folders.",
                    currentUgi.getShortUserName()));
      }
    }

    PermissionStatus newPermissionStatus = new PermissionStatus(
        username == null ?
            metadata.getPermissionStatus().getUserName() : username,
        groupname == null ?
            metadata.getPermissionStatus().getGroupName() : groupname,
        metadata.getPermissionStatus().getPermission());
    if (metadata.getBlobMaterialization() == BlobMaterialization.Implicit) {
      // It's an implicit folder, need to materialize it.
      store.storeEmptyFolder(key, newPermissionStatus);
    } else {
      store.changePermissionStatus(key, newPermissionStatus);
    }
  }

  /**
   * Is the user allowed?
   * <ol>
   *   <li>No user: false</li>
   *   <li>Empty list: false</li>
   *   <li>List == ["*"]: true</li>
   *   <li>Otherwise: is the user in the list?</li>
   * </ol>
   * @param username user to check; may be null
   * @param userList list of users; may be null or empty
   * @return
   * @throws IllegalArgumentException if the userList is invalid.
   */
  private boolean isAllowedUser(String username, List<String> userList) {

    if (null == userList || userList.isEmpty()) {
      return false;
    }

    boolean shouldSkipUserCheck = userList.size() == 1
        && userList.get(0).equals("*");

    // skip the check if the allowed users config value is set as '*'
    if (!shouldSkipUserCheck) {
      Preconditions.checkArgument(!userList.contains("*"),
        "User list must contain either '*' or a list of user names,"
            + " but not both.");
      return userList.contains(username);
    }
    return true;
  }

  @Override
  public synchronized void close() throws IOException {
    if (isClosed) {
      return;
    }

    // Call the base close() to close any resources there.
    super.close();
    // Close the store to close any resources there - e.g. the bandwidth
    // updater thread would be stopped at this time.
    store.close();
    // Notify the metrics system that this file system is closed, which may
    // trigger one final metrics push to get the accurate final file system
    // metrics out.

    long startTime = System.currentTimeMillis();

    if(!getConf().getBoolean(SKIP_AZURE_METRICS_PROPERTY_NAME, false)) {
      AzureFileSystemMetricsSystem.unregisterSource(metricsSourceName);
      AzureFileSystemMetricsSystem.fileSystemClosed();
    }

    LOG.debug("Submitting metrics when file system closed took {} ms.",
        (System.currentTimeMillis() - startTime));
    isClosed = true;
  }

  /**
   * Get a delegation token from remote service endpoint if
   * 'fs.azure.enable.kerberos.support' is set to 'true'.
   * @param renewer the account name that is allowed to renew the token.
   * @return delegation token
   * @throws IOException thrown when getting the current user.
   */
  @Override
  public synchronized Token<?> getDelegationToken(final String renewer) throws IOException {
    if (kerberosSupportEnabled) {
      return wasbDelegationTokenManager.getDelegationToken(renewer);
    } else {
      return super.getDelegationToken(renewer);
    }
  }

  @Override
  public void access(Path path, FsAction mode) throws IOException {
    if (azureAuthorization && authorizer != null) {
      try {
        // Required to check the existence of the path.
        getFileStatus(path);
        switch (mode) {
        case READ:
        case READ_EXECUTE:
          performAuthCheck(path, WasbAuthorizationOperations.READ, "access", path);
          break;
        case WRITE:
        case WRITE_EXECUTE:
          performAuthCheck(path, WasbAuthorizationOperations.WRITE, "access",
              path);
          break;
        case READ_WRITE:
        case ALL:
          performAuthCheck(path, WasbAuthorizationOperations.READ, "access", path);
          performAuthCheck(path, WasbAuthorizationOperations.WRITE, "access",
              path);
          break;
        case EXECUTE:
        case NONE:
        default:
          break;
        }
      } catch (WasbAuthorizationException wae){
        throw new AccessControlException(wae);
      }
    } else {
      super.access(path, mode);
    }
  }

  /**
   * A handler that defines what to do with blobs whose upload was
   * interrupted.
   */
  private abstract class DanglingFileHandler {
    abstract void handleFile(FileMetadata file, FileMetadata tempFile)
      throws IOException;
  }

  /**
   * Handler implementation for just deleting dangling files and cleaning
   * them up.
   */
  private class DanglingFileDeleter extends DanglingFileHandler {
    @Override
    void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException {

      LOG.debug("Deleting dangling file {}", file.getKey());
      // Not handling delete return type as false return essentially
      // means its a no-op for the caller
      store.delete(file.getKey());
      store.delete(tempFile.getKey());
    }
  }

  /**
   * Handler implementation for just moving dangling files to recovery
   * location (/lost+found).
   */
  private class DanglingFileRecoverer extends DanglingFileHandler {
    private final Path destination;

    DanglingFileRecoverer(Path destination) {
      this.destination = destination;
    }

    @Override
    void handleFile(FileMetadata file, FileMetadata tempFile)
        throws IOException {

      LOG.debug("Recovering {}", file.getKey());
      // Move to the final destination
      String finalDestinationKey =
          pathToKey(new Path(destination, file.getKey()));
      store.rename(tempFile.getKey(), finalDestinationKey);
      if (!finalDestinationKey.equals(file.getKey())) {
        // Delete the empty link file now that we've restored it.
        store.delete(file.getKey());
      }
    }
  }

  /**
   * Check if a path has colons in its name
   */
  private boolean containsColon(Path p) {
    return p.toUri().getPath().toString().contains(":");
  }

  /**
   * Implements recover and delete (-move and -delete) behaviors for handling
   * dangling files (blobs whose upload was interrupted).
   * 
   * @param root
   *          The root path to check from.
   * @param handler
   *          The handler that deals with dangling files.
   */
  private void handleFilesWithDanglingTempData(Path root,
      DanglingFileHandler handler) throws IOException {
    // Calculate the cut-off for when to consider a blob to be dangling.
    long cutoffForDangling = new Date().getTime()
        - getConf().getInt(AZURE_TEMP_EXPIRY_PROPERTY_NAME,
            AZURE_TEMP_EXPIRY_DEFAULT) * 1000;
    // Go over all the blobs under the given root and look for blobs to
    // recover.
    String priorLastKey = null;
    do {
      PartialListing listing = store.listAll(pathToKey(root), AZURE_LIST_ALL,
          AZURE_UNBOUNDED_DEPTH, priorLastKey);

      for (FileMetadata file : listing.getFiles()) {
        if (!file.isDir()) { // We don't recover directory blobs
          // See if this blob has a link in it (meaning it's a place-holder
          // blob for when the upload to the temp blob is complete).
          String link = store.getLinkInFileMetadata(file.getKey());
          if (link != null) {
            // It has a link, see if the temp blob it is pointing to is
            // existent and old enough to be considered dangling.
            FileMetadata linkMetadata = store.retrieveMetadata(link);
            if (linkMetadata != null
                && linkMetadata.getLastModified() >= cutoffForDangling) {
              // Found one!
              handler.handleFile(file, linkMetadata);
            }
          }
        }
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);
  }

  /**
   * Looks under the given root path for any blob that are left "dangling",
   * meaning that they are place-holder blobs that we created while we upload
   * the data to a temporary blob, but for some reason we crashed in the middle
   * of the upload and left them there. If any are found, we move them to the
   * destination given.
   * 
   * @param root
   *          The root path to consider.
   * @param destination
   *          The destination path to move any recovered files to.
   * @throws IOException Thrown when fail to recover files.
   */
  public void recoverFilesWithDanglingTempData(Path root, Path destination)
      throws IOException {

    LOG.debug("Recovering files with dangling temp data in {}", root);
    handleFilesWithDanglingTempData(root,
        new DanglingFileRecoverer(destination));
  }

  /**
   * Looks under the given root path for any blob that are left "dangling",
   * meaning that they are place-holder blobs that we created while we upload
   * the data to a temporary blob, but for some reason we crashed in the middle
   * of the upload and left them there. If any are found, we delete them.
   * 
   * @param root
   *          The root path to consider.
   * @throws IOException Thrown when fail to delete.
   */
  public void deleteFilesWithDanglingTempData(Path root) throws IOException {

    LOG.debug("Deleting files with dangling temp data in {}", root);
    handleFilesWithDanglingTempData(root, new DanglingFileDeleter());
  }

  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called.");
    close();
    super.finalize();
  }

  /**
   * Encode the key with a random prefix for load balancing in Azure storage.
   * Upload data to a random temporary file then do storage side renaming to
   * recover the original key.
   * 
   * @param aKey a key to be encoded.
   * @return Encoded version of the original key.
   */
  private static String encodeKey(String aKey) {
    // Get the tail end of the key name.
    //
    String fileName = aKey.substring(aKey.lastIndexOf(Path.SEPARATOR) + 1,
        aKey.length());

    // Construct the randomized prefix of the file name. The prefix ensures the
    // file always drops into the same folder but with a varying tail key name.
    String filePrefix = AZURE_TEMP_FOLDER + Path.SEPARATOR
        + UUID.randomUUID().toString();

    // Concatenate the randomized prefix with the tail of the key name.
    String randomizedKey = filePrefix + fileName;

    // Return to the caller with the randomized key.
    return randomizedKey;
  }

  /*
   * Helper method to retrieve owner information for a given path.
   * The method returns empty string in case the file is not found or the metadata does not contain owner information
  */
  @VisibleForTesting
  public String getOwnerForPath(Path absolutePath) throws IOException {
    String owner = "";
    FileMetadata meta = null;
    String key = pathToKey(absolutePath);
    try {

      meta = store.retrieveMetadata(key);

      if (meta != null) {
        owner = meta.getPermissionStatus().getUserName();
        LOG.debug("Retrieved '{}' as owner for path - {}", owner, absolutePath);
      } else {
        // meta will be null if file/folder doen not exist
        LOG.debug("Cannot find file/folder - '{}'. Returning owner as empty string", absolutePath);
      }
    } catch(IOException ex) {

          Throwable innerException = NativeAzureFileSystemHelper.checkForAzureStorageException(ex);
          boolean isfileNotFoundException = innerException instanceof StorageException
            && NativeAzureFileSystemHelper.isFileNotFoundException((StorageException) innerException);

          // should not throw when the exception is related to blob/container/file/folder not found
          if (!isfileNotFoundException) {
            String errorMsg = "Could not retrieve owner information for path - " + absolutePath;
            LOG.error(errorMsg);
            throw new IOException(errorMsg, ex);
          }
      }
    return owner;
  }

  /**
   * Helper method to update the chownAllowedUsers in tests.
   * @param chownAllowedUsers list of chown allowed users
   */
  @VisibleForTesting
  void updateChownAllowedUsers(List<String> chownAllowedUsers) {
    this.chownAllowedUsers = chownAllowedUsers;
  }

  /**
   * Helper method to update the chmodAllowedUsers in tests.
   * @param chmodAllowedUsers list of chmod allowed users
   */
  @VisibleForTesting
  void updateChmodAllowedUsers(List<String> chmodAllowedUsers) {
    this.chmodAllowedUsers = chmodAllowedUsers;
  }

  /**
   * Helper method to update the daemonUsers in tests.
   * @param daemonUsers list of daemon users
   */
  @VisibleForTesting
  void updateDaemonUsers(List<String> daemonUsers) {
    this.daemonUsers = daemonUsers;
  }
}
