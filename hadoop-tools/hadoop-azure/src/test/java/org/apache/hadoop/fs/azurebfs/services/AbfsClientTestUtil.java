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

import java.net.URL;

import org.mockito.Mockito;

import org.apache.hadoop.util.functional.BiFunctionRaisingIOE;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;

public final class AbfsClientTestUtil {

  private AbfsClientTestUtil() {

  }

  public static void setMockAbfsRestOperationForCopyBlobOperation(final AbfsClient spiedClient,
      final BiFunctionRaisingIOE<AbfsRestOperation, AbfsRestOperation, AbfsRestOperation> functionRaisingIOE) {

    Mockito.doAnswer(answer -> {
          final AbfsRestOperation spiedRestOp = Mockito.spy(new AbfsRestOperation(
              AbfsRestOperationType.CopyBlob,
              spiedClient,
              HTTP_METHOD_PUT,
              answer.getArgument(0),
              answer.getArgument(1)
          ));
          final AbfsRestOperation actualCallMakerOp = new AbfsRestOperation(
              AbfsRestOperationType.CopyBlob,
              spiedClient,
              HTTP_METHOD_PUT,
              answer.getArgument(0),
              answer.getArgument(1)
          );
          return functionRaisingIOE.apply(spiedRestOp, actualCallMakerOp);
        })
        .when(spiedClient)
        .getCopyBlobOperation(Mockito.any(URL.class), Mockito.anyList());
  }
}
