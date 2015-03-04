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

package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * The CopyListing abstraction is responsible for how the list of
 * sources and targets is constructed, for DistCp's copy function.
 * The copy-listing should be a
 * SequenceFile&lt;Text, CopyListingFileStatus&gt;, located at the path
 * specified to buildListing(), each entry being a pair of (Source relative
 * path, source file status), all the paths being fully qualified.
 */
public abstract class CopyListing extends Configured {

  private Credentials credentials;

  /**
   * Build listing function creates the input listing that distcp uses to
   * perform the copy.
   *
   * The build listing is a sequence file that has relative path of a file in the key
   * and the file status information of the source file in the value
   *
   * For instance if the source path is /tmp/data and the traversed path is
   * /tmp/data/dir1/dir2/file1, then the sequence file would contain
   *
   * key: /dir1/dir2/file1 and value: FileStatus(/tmp/data/dir1/dir2/file1)
   *
   * File would also contain directory entries. Meaning, if /tmp/data/dir1/dir2/file1
   * is the only file under /tmp/data, the resulting sequence file would contain the
   * following entries
   *
   * key: /dir1 and value: FileStatus(/tmp/data/dir1)
   * key: /dir1/dir2 and value: FileStatus(/tmp/data/dir1/dir2)
   * key: /dir1/dir2/file1 and value: FileStatus(/tmp/data/dir1/dir2/file1)
   *
   * Cases requiring special handling:
   * If source path is a file (/tmp/file1), contents of the file will be as follows
   *
   * TARGET DOES NOT EXIST: Key-"", Value-FileStatus(/tmp/file1)
   * TARGET IS FILE       : Key-"", Value-FileStatus(/tmp/file1)
   * TARGET IS DIR        : Key-"/file1", Value-FileStatus(/tmp/file1)  
   *
   * @param pathToListFile - Output file where the listing would be stored
   * @param options - Input options to distcp
   * @throws IOException - Exception if any
   */
  public final void buildListing(Path pathToListFile,
                                 DistCpOptions options) throws IOException {
    validatePaths(options);
    doBuildListing(pathToListFile, options);
    Configuration config = getConf();

    config.set(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, pathToListFile.toString());
    config.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, getBytesToCopy());
    config.setLong(DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS, getNumberOfPaths());

    validateFinalListing(pathToListFile, options);
  }

  /**
   * Validate input and output paths
   *
   * @param options - Input options
   * @throws InvalidInputException If inputs are invalid
   * @throws IOException any Exception with FS
   */
  protected abstract void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException;

  /**
   * The interface to be implemented by sub-classes, to create the source/target file listing.
   * @param pathToListFile Path on HDFS where the listing file is written.
   * @param options Input Options for DistCp (indicating source/target paths.)
   * @throws IOException Thrown on failure to create the listing file.
   */
  protected abstract void doBuildListing(Path pathToListFile,
                                         DistCpOptions options) throws IOException;

  /**
   * Return the total bytes that distCp should copy for the source paths
   * This doesn't consider whether file is same should be skipped during copy
   *
   * @return total bytes to copy
   */
  protected abstract long getBytesToCopy();

  /**
   * Return the total number of paths to distcp, includes directories as well
   * This doesn't consider whether file/dir is already present and should be skipped during copy
   *
   * @return Total number of paths to distcp
   */
  protected abstract long getNumberOfPaths();

  /**
   * Validate the final resulting path listing.  Checks if there are duplicate
   * entries.  If preserving ACLs, checks that file system can support ACLs.
   * If preserving XAttrs, checks that file system can support XAttrs.
   *
   * @param pathToListFile - path listing build by doBuildListing
   * @param options - Input options to distcp
   * @throws IOException - Any issues while checking for duplicates and throws
   * @throws DuplicateFileException - if there are duplicates
   */
  private void validateFinalListing(Path pathToListFile, DistCpOptions options)
      throws DuplicateFileException, IOException {

    Configuration config = getConf();
    FileSystem fs = pathToListFile.getFileSystem(config);

    Path sortedList = DistCpUtils.sortListing(fs, config, pathToListFile);

    SequenceFile.Reader reader = new SequenceFile.Reader(
                          config, SequenceFile.Reader.file(sortedList));
    try {
      Text lastKey = new Text("*"); //source relative path can never hold *
      CopyListingFileStatus lastFileStatus = new CopyListingFileStatus();

      Text currentKey = new Text();
      Set<URI> aclSupportCheckFsSet = Sets.newHashSet();
      Set<URI> xAttrSupportCheckFsSet = Sets.newHashSet();
      while (reader.next(currentKey)) {
        if (currentKey.equals(lastKey)) {
          CopyListingFileStatus currentFileStatus = new CopyListingFileStatus();
          reader.getCurrentValue(currentFileStatus);
          throw new DuplicateFileException("File " + lastFileStatus.getPath() + " and " +
              currentFileStatus.getPath() + " would cause duplicates. Aborting");
        }
        reader.getCurrentValue(lastFileStatus);
        if (options.shouldPreserve(DistCpOptions.FileAttribute.ACL)) {
          FileSystem lastFs = lastFileStatus.getPath().getFileSystem(config);
          URI lastFsUri = lastFs.getUri();
          if (!aclSupportCheckFsSet.contains(lastFsUri)) {
            DistCpUtils.checkFileSystemAclSupport(lastFs);
            aclSupportCheckFsSet.add(lastFsUri);
          }
        }
        if (options.shouldPreserve(DistCpOptions.FileAttribute.XATTR)) {
          FileSystem lastFs = lastFileStatus.getPath().getFileSystem(config);
          URI lastFsUri = lastFs.getUri();
          if (!xAttrSupportCheckFsSet.contains(lastFsUri)) {
            DistCpUtils.checkFileSystemXAttrSupport(lastFs);
            xAttrSupportCheckFsSet.add(lastFsUri);
          }
        }
        lastKey.set(currentKey);
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  /**
   * Protected constructor, to initialize configuration.
   * @param configuration The input configuration,
   *                        with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached.If null
   * delegation token caching is skipped
   */
  protected CopyListing(Configuration configuration, Credentials credentials) {
    setConf(configuration);
    setCredentials(credentials);
  }

  /**
   * set Credentials store, on which FS delegatin token will be cached
   * @param credentials - Credentials object
   */
  protected void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }

  /**
   * get credentials to update the delegation tokens for accessed FS objects
   * @return Credentials object
   */
  protected Credentials getCredentials() {
    return credentials;
  }

  /**
   * Public Factory method with which the appropriate CopyListing implementation may be retrieved.
   * @param configuration The input configuration.
   * @param credentials Credentials object on which the FS delegation tokens are cached
   * @param options The input Options, to help choose the appropriate CopyListing Implementation.
   * @return An instance of the appropriate CopyListing implementation.
   * @throws java.io.IOException - Exception if any
   */
  public static CopyListing getCopyListing(Configuration configuration,
                                           Credentials credentials,
                                           DistCpOptions options)
      throws IOException {
    if (options.shouldUseDiff()) {
      return new GlobbedCopyListing(configuration, credentials);
    }
    String copyListingClassName = configuration.get(DistCpConstants.
        CONF_LABEL_COPY_LISTING_CLASS, "");
    Class<? extends CopyListing> copyListingClass;
    try {
      if (! copyListingClassName.isEmpty()) {
        copyListingClass = configuration.getClass(DistCpConstants.
            CONF_LABEL_COPY_LISTING_CLASS, GlobbedCopyListing.class,
            CopyListing.class);
      } else {
        if (options.getSourceFileListing() == null) {
            copyListingClass = GlobbedCopyListing.class;
        } else {
            copyListingClass = FileBasedCopyListing.class;
        }
      }
      copyListingClassName = copyListingClass.getName();
      Constructor<? extends CopyListing> constructor = copyListingClass.
          getDeclaredConstructor(Configuration.class, Credentials.class);
      return constructor.newInstance(configuration, credentials);
    } catch (Exception e) {
      throw new IOException("Unable to instantiate " + copyListingClassName, e);
    }
  }

  static class DuplicateFileException extends RuntimeException {
    public DuplicateFileException(String message) {
      super(message);
    }
  }

  static class InvalidInputException extends RuntimeException {
    public InvalidInputException(String message) {
      super(message);
    }
  }

  public static class AclsNotSupportedException extends RuntimeException {
    public AclsNotSupportedException(String message) {
      super(message);
    }
  }
  
  public static class XAttrsNotSupportedException extends RuntimeException {
    public XAttrsNotSupportedException(String message) {
      super(message);
    }
  }
}
