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

package org.apache.hadoop.lib.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.lang.XException;

/**
 * Exception thrown by the {@link Server} class.
 */
@InterfaceAudience.Private
public class ServerException extends XException {

  /**
   * Error codes use by the {@link Server} class.
   */
  @InterfaceAudience.Private
  public enum ERROR implements XException.ERROR {
    S01("Dir [{0}] does not exist"),
    S02("[{0}] is not a directory"),
    S03("Could not load file from classpath [{0}], {1}"),
    S04("Service [{0}] does not implement declared interface [{1}]"),
    S05("[{0}] is not a file"),
    S06("Could not load file [{0}], {1}"),
    S07("Could not instanciate service class [{0}], {1}"),
    S08("Could not load service classes, {0}"),
    S09("Could not set service [{0}] programmatically -server shutting down-, {1}"),
    S10("Service [{0}] requires service [{1}]"),
    S11("Service [{0}] exception during status change to [{1}] -server shutting down-, {2}"),
    S12("Could not start service [{0}], {1}"),
    S13("Missing system property [{0}]"),
    S14("Could not initialize server, {0}")
    ;

    private String msg;

    /**
     * Constructor for the error code enum.
     *
     * @param msg message template.
     */
    private ERROR(String msg) {
      this.msg = msg;
    }

    /**
     * Returns the message template for the error code.
     *
     * @return the message template for the error code.
     */
    @Override
    public String getTemplate() {
      return msg;
    }
  }

  /**
   * Constructor for sub-classes.
   *
   * @param error error code for the XException.
   * @param params parameters to use when creating the error message
   * with the error code template.
   */
  protected ServerException(XException.ERROR error, Object... params) {
    super(error, params);
  }

  /**
   * Creates an server exception using the specified error code.
   * The exception message is resolved using the error code template
   * and the passed parameters.
   *
   * @param error error code for the XException.
   * @param params parameters to use when creating the error message
   * with the error code template.
   */
  public ServerException(ERROR error, Object... params) {
    super(error, params);
  }

}
