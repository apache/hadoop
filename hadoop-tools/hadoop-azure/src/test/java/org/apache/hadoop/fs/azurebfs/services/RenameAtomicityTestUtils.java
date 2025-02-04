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

package org.apache.hadoop.fs.azurebfs.services;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public final class RenameAtomicityTestUtils {

  private RenameAtomicityTestUtils() {
  }

  /**
   * Creates a spied object of {@link BlobRenameHandler} and {@link RenameAtomicity}
   * and adds mocked behavior to {@link RenameAtomicity#createRenamePendingJson(Path, byte[])}.
   *
   * @param client client that would supply BlobRenameHandler and RenameAtomicity.
   * @param answer mocked behavior for {@link RenameAtomicity#createRenamePendingJson(Path, byte[])}.
   */
  public static void addCreatePathMock(AbfsBlobClient client, Answer answer) {
    Mockito.doAnswer(clientHandlerAns -> {
          BlobRenameHandler renameHandler = Mockito.spy(
              (BlobRenameHandler) clientHandlerAns.callRealMethod());
          Mockito.doAnswer(getRenameAtomicityAns -> {
                RenameAtomicity renameAtomicity = Mockito.spy(
                    (RenameAtomicity) getRenameAtomicityAns.callRealMethod());
                Mockito.doAnswer(answer)
                    .when(renameAtomicity)
                    .createRenamePendingJson(Mockito.any(
                        Path.class), Mockito.any(byte[].class));
                return renameAtomicity;
              })
              .when(renameHandler)
              .getRenameAtomicity(Mockito.any(PathInformation.class));
          return renameHandler;
        })
        .when(client)
        .getBlobRenameHandler(Mockito.anyString(), Mockito.anyString(),
            Mockito.nullable(String.class), Mockito.anyBoolean(), Mockito.any(
                TracingContext.class));
  }


  /**
   * Adds mocked behavior to {@link RenameAtomicity#readRenamePendingJson(Path, int)}.
   *
   * @param redoRenameAtomicity {@link RenameAtomicity} to be spied.
   * @param answer mocked behavior for {@link RenameAtomicity#readRenamePendingJson(Path, int)}.
   *
   * @throws AzureBlobFileSystemException server error or error from mocked behavior.
   */
  public static void addReadPathMock(RenameAtomicity redoRenameAtomicity,
      Answer answer)
      throws AzureBlobFileSystemException {
    Mockito.doAnswer(answer)
        .when(redoRenameAtomicity)
        .readRenamePendingJson(Mockito.any(Path.class), Mockito.anyInt());
  }
}
