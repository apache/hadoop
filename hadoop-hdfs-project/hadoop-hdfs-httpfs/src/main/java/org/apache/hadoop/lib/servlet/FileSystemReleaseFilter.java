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

package org.apache.hadoop.lib.servlet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.lib.service.FileSystemAccess;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * The <code>FileSystemReleaseFilter</code> releases back to the
 * {@link FileSystemAccess} service a <code>FileSystem</code> instance.
 * <p>
 * This filter is useful in situations where a servlet request
 * is streaming out HDFS data and the corresponding filesystem
 * instance have to be closed after the streaming completes.
 */
@InterfaceAudience.Private
public abstract class FileSystemReleaseFilter implements Filter {
  private static final ThreadLocal<FileSystem> FILE_SYSTEM_TL = new ThreadLocal<FileSystem>();

  /**
   * Initializes the filter.
   * <p>
   * This implementation is a NOP.
   *
   * @param filterConfig filter configuration.
   *
   * @throws ServletException thrown if the filter could not be initialized.
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  /**
   * It delegates the incoming request to the <code>FilterChain</code>, and
   * at its completion (in a finally block) releases the filesystem instance
   * back to the {@link FileSystemAccess} service.
   *
   * @param servletRequest servlet request.
   * @param servletResponse servlet response.
   * @param filterChain filter chain.
   *
   * @throws IOException thrown if an IO error occurrs.
   * @throws ServletException thrown if a servet error occurrs.
   */
  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
    throws IOException, ServletException {
    try {
      filterChain.doFilter(servletRequest, servletResponse);
    } finally {
      FileSystem fs = FILE_SYSTEM_TL.get();
      if (fs != null) {
        FILE_SYSTEM_TL.remove();
        getFileSystemAccess().releaseFileSystem(fs);
      }
    }
  }

  /**
   * Destroys the filter.
   * <p>
   * This implementation is a NOP.
   */
  @Override
  public void destroy() {
  }

  /**
   * Static method that sets the <code>FileSystem</code> to release back to
   * the {@link FileSystemAccess} service on servlet request completion.
   *
   * @param fs fileystem instance.
   */
  public static void setFileSystem(FileSystem fs) {
    FILE_SYSTEM_TL.set(fs);
  }

  /**
   * Abstract method to be implemetned by concrete implementations of the
   * filter that return the {@link FileSystemAccess} service to which the filesystem
   * will be returned to.
   *
   * @return the FileSystemAccess service.
   */
  protected abstract FileSystemAccess getFileSystemAccess();

}
