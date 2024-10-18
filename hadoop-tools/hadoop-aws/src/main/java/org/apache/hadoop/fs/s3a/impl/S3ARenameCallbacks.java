/*
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

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.impl.FileSystemRename3Action;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;

import static org.apache.hadoop.fs.FSExceptionMessages.RENAME_DEST_PARENT_NOT_DIRECTORY;
import static org.apache.hadoop.fs.s3a.Invoker.once;

/**
 * This is the S3A rename/3 support; which calls back into
 * the S3A FS for some actions, and delegates the rest to
 * a delete and a rename operation.
 * <p></p>
 * The FileStatus references returned are always {@link S3AFileStatus}
 * instances; that is relied upon in
 * {@link #executeRename(FileSystemRename3Action.ExecuteRenameParams)}.
 */
public class S3ARenameCallbacks extends AbstractStoreOperation implements
    FileSystemRename3Action.RenameCallbacks {

  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.fs.s3a.impl.S3ARenameCallbacks.class);

  /**
   * operations callbacks to hand off to delete and rename.
   */
  private final OperationCallbacks operationCallbacks;

  /**
   * Extra FS probes needed here.
   */
  private final Probes probes;

  /**
   * Number of entries in a delete page.
   */
  private final int pageSize;

  /**
   * Constructor.
   * @param storeContext store
   * @param operationCallbacks callbacks for delete and rename.
   * @param pageSize Number of entries in a delete page.
   * @param dirOperationsPurgeUploads Do directory operations purge pending uploads?
   * @param probes Extra FS probes needed here.
   */
  public S3ARenameCallbacks(
      final StoreContext storeContext,
      final OperationCallbacks operationCallbacks,
      final int pageSize,
      final boolean dirOperationsPurgeUploads, final Probes probes) {
    super(storeContext);
    this.operationCallbacks = operationCallbacks;
    this.probes = probes;
    this.pageSize = pageSize;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
    @Retries.RetryTranslated
    public S3AFileStatus getSourceStatus(final Path sourcePath)
        throws FileNotFoundException, IOException {
      return probes.stat(sourcePath, false,
          StatusProbeEnum.ALL);
    }

    @Override
    @Retries.RetryTranslated
    public S3AFileStatus getDestStatusOrNull(final Path destPath)
        throws IOException {
      try {
        return probes.stat(destPath, true,
            StatusProbeEnum.ALL);
      } catch (FileNotFoundException e) {
        return null;
      }
    }

    /**
     * Optimized probe which looks at empty dir state of the
     * directory passed in, if known.
     * {@inheritDoc}.
     */
    @Override
    @Retries.RetryTranslated
    public boolean directoryHasChildren(final FileStatus directory)
        throws IOException {
      if (directory instanceof S3AFileStatus) {
        S3AFileStatus st = (S3AFileStatus) directory;
        if (st.isEmptyDirectory() == Tristate.TRUE) {
          return false;
        }
        if (st.isEmptyDirectory() == Tristate.FALSE) {
          return true;
        }
      }
    return probes.dirHasChildren(directory.getPath());
  }
    /**
     * Optimized probe which assumes the path is a parent, so
     * checks directory status first.
     * {@inheritDoc}.
     */
    @Override
    @Retries.RetryTranslated
    public void verifyIsDirectory(final Path destParent)
        throws ParentNotDirectoryException, IOException {
      if (probes.isDir(destParent)) {
        return;
      }
      // either nothing there, or it is a file.
      // do a HEAD, raising FNFE if it is not there
      probes.stat(destParent, false, StatusProbeEnum.FILE);
      // if we get here, no exception was raised, therefore HEAD
      // succeeded, therefore there is a file at the path.
      throw new ParentNotDirectoryException(
          String.format(RENAME_DEST_PARENT_NOT_DIRECTORY, destParent));
    }

    /**
     * This is a highly optimized rename operation which
     * avoids performing any duplicate metadata probes,
     * as well as raising all failures as meaningful exceptions.
     * {@inheritDoc}.
     * @param params
     */
    @Override
    @Retries.RetryTranslated
    public void executeRename(
        final FileSystemRename3Action.ExecuteRenameParams params)
        throws PathIOException, IOException {

      final S3AFileStatus sourceStatus =
          (S3AFileStatus) params.getSourceStatus();
      final Path sourcePath = params.getSourcePath();
      final Path destPath = params.getDestPath();
      final String source = sourcePath.toString();
      final StoreContext context = getStoreContext();
      String srcKey = context.pathToKey(sourcePath);
      String dstKey = context.pathToKey(destPath);


      once("rename(" + source + ", "
              + destPath + ")", source,
          () -> {
            final S3AFileStatus destStatus
                = (S3AFileStatus) params.getDestStatus();
            if (params.isDeleteDest()) {
              // Delete the file or directory marker
              // (Which is all which can be there)
              DeleteOperation deleteOperation = new DeleteOperation(
                  context,
                  destStatus,
                  false,
                  operationCallbacks,
                  pageSize);
              deleteOperation.execute();
            }

            // Initiate the rename.
            RenameOperation renameOperation = new RenameOperation(
                context,
                sourcePath, srcKey, (S3AFileStatus) sourceStatus,
                destPath, dstKey, destStatus,
                operationCallbacks,
                pageSize);
            long bytesCopied = renameOperation.execute();
            LOG.debug("Copied {} bytes", bytesCopied);
          });
    }

  /**
   * Callback for the operation.
   */
  public interface Probes {

    S3AFileStatus stat(final Path f,
        final boolean needEmptyDirectoryFlag,
        final Set<StatusProbeEnum> probes) throws IOException;

    boolean isDir(Path path) throws IOException;

    boolean dirHasChildren(Path path) throws IOException;
  }
}
