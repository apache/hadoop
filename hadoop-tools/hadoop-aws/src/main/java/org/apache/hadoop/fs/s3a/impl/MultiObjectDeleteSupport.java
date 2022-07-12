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

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.List;

import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.AWSS3IOException;

/**
 * Support for Multi Object Deletion.
 * This is used to be a complex piece of code as it was required to
 * update s3guard.
 * Now all that is left is the exception extraction for better
 * reporting,
 */
public final class MultiObjectDeleteSupport {

  private static final Logger LOG = LoggerFactory.getLogger(
      MultiObjectDeleteSupport.class);

  private MultiObjectDeleteSupport() {
  }

  /**
   * This is the exception exit code if access was denied on a delete.
   * {@value}.
   */
  public static final String ACCESS_DENIED = "AccessDenied";

  /**
   * A {@code MultiObjectDeleteException} is raised if one or more
   * paths listed in a bulk DELETE operation failed.
   * The top-level exception is therefore just "something wasn't deleted",
   * but doesn't include the what or the why.
   * This translation will extract an AccessDeniedException if that's one of
   * the causes, otherwise grabs the status code and uses it in the
   * returned exception.
   * @param message text for the exception
   * @param deleteException the delete exception. to translate
   * @return an IOE with more detail.
   */
  public static IOException translateDeleteException(
      final String message,
      final MultiObjectDeleteException deleteException) {
    List<MultiObjectDeleteException.DeleteError> errors
        = deleteException.getErrors();
    LOG.info("Bulk delete operation failed to delete all objects;"
            + " failure count = {}",
        errors.size());
    final StringBuilder result = new StringBuilder(
        errors.size() * 256);
    result.append(message).append(": ");
    String exitCode = "";
    for (MultiObjectDeleteException.DeleteError error :
        deleteException.getErrors()) {
      String code = error.getCode();
      String item = String.format("%s: %s%s: %s%n", code, error.getKey(),
          (error.getVersionId() != null
              ? (" (" + error.getVersionId() + ")")
              : ""),
          error.getMessage());
      LOG.info(item);
      result.append(item);
      if (exitCode == null || exitCode.isEmpty() || ACCESS_DENIED.equals(code)) {
        exitCode = code;
      }
    }
    if (ACCESS_DENIED.equals(exitCode)) {
      return (IOException) new AccessDeniedException(result.toString())
          .initCause(deleteException);
    } else {
      return new AWSS3IOException(result.toString(), deleteException);
    }
  }
}
