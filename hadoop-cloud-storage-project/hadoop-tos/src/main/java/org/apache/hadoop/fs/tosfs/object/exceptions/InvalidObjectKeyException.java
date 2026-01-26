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

package org.apache.hadoop.fs.tosfs.object.exceptions;

/**
 * Regarding accessing an object in directory bucket, if the object is locating under an existing
 * file in directory bucket, the {@link InvalidObjectKeyException} will be thrown. E.g. there is a
 * file object 'a/b/file' exists in directory bucket, the {@link InvalidObjectKeyException} will be
 * thrown if head object 'a/b/file/c' no matter whether 'c' exists or not.
 */
public class InvalidObjectKeyException extends RuntimeException {
  public InvalidObjectKeyException(Throwable cause) {
    super(cause);
  }
}
