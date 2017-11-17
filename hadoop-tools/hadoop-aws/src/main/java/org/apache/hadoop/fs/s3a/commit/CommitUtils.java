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

package org.apache.hadoop.fs.s3a.commit;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.*;
import static org.apache.hadoop.fs.s3a.commit.ValidationFailure.verify;

/**
 * Static utility methods related to S3A commitment processing, both
 * staging and magic.
 *
 * <b>Do not use in any codepath intended to be used from the S3AFS
 * except in the committers themselves.</b>
 */
public final class CommitUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(CommitUtils.class);

  private CommitUtils() {
  }

  /**
   * Verify that the path is a magic one.
   * @param fs filesystem
   * @param path path
   * @throws PathCommitException if the path isn't a magic commit path
   */
  public static void verifyIsMagicCommitPath(S3AFileSystem fs,
      Path path) throws PathCommitException {
    verifyIsMagicCommitFS(fs);
    if (!fs.isMagicCommitPath(path)) {
      throw new PathCommitException(path, E_BAD_PATH);
    }
  }

  /**
   * Verify that an S3A FS instance is a magic commit FS.
   * @param fs filesystem
   * @throws PathCommitException if the FS isn't a magic commit FS.
   */
  public static void verifyIsMagicCommitFS(S3AFileSystem fs)
      throws PathCommitException {
    if (!fs.isMagicCommitEnabled()) {
      // dump out details to console for support diagnostics
      String fsUri = fs.getUri().toString();
      LOG.error("{}: {}:\n{}", E_NORMAL_FS, fsUri, fs);
      // then fail
      throw new PathCommitException(fsUri, E_NORMAL_FS);
    }
  }

  /**
   * Verify that an FS is an S3A FS.
   * @param fs filesystem
   * @param path path to to use in exception
   * @return the typecast FS.
   * @throws PathCommitException if the FS is not an S3A FS.
   */
  public static S3AFileSystem verifyIsS3AFS(FileSystem fs, Path path)
      throws PathCommitException {
    if (!(fs instanceof S3AFileSystem)) {
      throw new PathCommitException(path, E_WRONG_FS);
    }
    return (S3AFileSystem) fs;
  }

  /**
   * Get the S3A FS of a path.
   * @param path path to examine
   * @param conf config
   * @param magicCommitRequired is magic complete required in the FS?
   * @return the filesystem
   * @throws PathCommitException output path isn't to an S3A FS instance, or
   * if {@code magicCommitRequired} is set, if doesn't support these commits.
   * @throws IOException failure to instantiate the FS
   */
  public static S3AFileSystem getS3AFileSystem(Path path,
      Configuration conf,
      boolean magicCommitRequired)
      throws PathCommitException, IOException {
    S3AFileSystem s3AFS = verifyIsS3AFS(path.getFileSystem(conf), path);
    if (magicCommitRequired) {
      verifyIsMagicCommitFS(s3AFS);
    }
    return s3AFS;
  }

  /**
   * Verify that all instances in a collection are of the given class.
   * @param it iterator
   * @param classname classname to require
   * @throws ValidationFailure on a failure
   */
  public static void validateCollectionClass(Iterable it, Class classname)
      throws ValidationFailure {
    for (Object o : it) {
      verify(o.getClass().equals(classname),
          "Collection element is not a %s: %s", classname, o.getClass());
    }
  }


}
