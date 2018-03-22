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

import com.microsoft.azure.storage.Constants.HeaderConstants;
import org.apache.hadoop.classification.InterfaceAudience;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Determines the operation type (PutBlock, PutPage, GetBlob, etc) of Azure
 * Storage operations.  This is used by the handlers of the SendingRequestEvent
 * and ResponseReceivedEvent exposed by the Azure Storage SDK to identify
 * operation types (since the type of operation is not exposed by the SDK).
 */
@InterfaceAudience.Private
final class BlobOperationDescriptor {

  private BlobOperationDescriptor() {
    // hide default constructor
  }

  /**
   * Gets the content length for the Azure Storage operation from the
   * 'x-ms-range' header, if set.
   * @param range the value of the 'x-ms-range' header.
   * @return the content length, or zero if not set.
   */
  private static long getContentLengthIfKnown(String range) {
    long contentLength = 0;
    // Format is "bytes=%d-%d"
    if (range != null && range.startsWith("bytes=")) {
      String[] offsets = range.substring("bytes=".length()).split("-");
      if (offsets.length == 2) {
        contentLength = Long.parseLong(offsets[1]) - Long.parseLong(offsets[0])
            + 1;
      }
    }
    return contentLength;
  }

  /**
   * Gets the content length for the Azure Storage operation, or returns zero if
   * unknown.
   * @param conn the connection object for the Azure Storage operation.
   * @param operationType the Azure Storage operation type.
   * @return the content length, or zero if unknown.
   */
  static long getContentLengthIfKnown(HttpURLConnection conn,
                                      OperationType operationType) {
    long contentLength = 0;
    switch (operationType) {
      case AppendBlock:
      case PutBlock:
        String lengthString = conn.getRequestProperty(
            HeaderConstants.CONTENT_LENGTH);
        contentLength = (lengthString != null)
            ? Long.parseLong(lengthString)
            : 0;
        break;
      case PutPage:
      case GetBlob:
        contentLength = BlobOperationDescriptor.getContentLengthIfKnown(
            conn.getRequestProperty("x-ms-range"));
        break;
      default:
        break;
    }
    return contentLength;
  }

  /**
   * Gets the operation type of an Azure Storage operation.
   *
   * @param conn the connection object for the Azure Storage operation.
   * @return the operation type.
   */
  static OperationType getOperationType(HttpURLConnection conn) {
    OperationType operationType = OperationType.Unknown;
    String method = conn.getRequestMethod();
    String compValue = getQueryParameter(conn.getURL(),
        "comp");

    if (method.equalsIgnoreCase("PUT")) {
      if (compValue != null) {
        switch (compValue) {
          case "metadata":
            operationType = OperationType.SetMetadata;
            break;
          case "properties":
            operationType = OperationType.SetProperties;
            break;
          case "block":
            operationType = OperationType.PutBlock;
            break;
          case "page":
            String pageWrite = conn.getRequestProperty("x-ms-page-write");
            if (pageWrite != null && pageWrite.equalsIgnoreCase(
                "UPDATE")) {
              operationType = OperationType.PutPage;
            }
            break;
          case "appendblock":
            operationType = OperationType.AppendBlock;
            break;
          case "blocklist":
            operationType = OperationType.PutBlockList;
            break;
          default:
            break;
        }
      } else {
        String blobType = conn.getRequestProperty("x-ms-blob-type");
        if (blobType != null
            && (blobType.equalsIgnoreCase("PageBlob")
            || blobType.equalsIgnoreCase("BlockBlob")
            || blobType.equalsIgnoreCase("AppendBlob"))) {
          operationType = OperationType.CreateBlob;
        } else if (blobType == null) {
          String resType = getQueryParameter(conn.getURL(),
              "restype");
          if (resType != null
              && resType.equalsIgnoreCase("container")) {
            operationType = operationType.CreateContainer;
          }
        }
      }
    } else if (method.equalsIgnoreCase("GET")) {
      if (compValue != null) {
        switch (compValue) {
          case "list":
            operationType = OperationType.ListBlobs;
            break;

          case "metadata":
            operationType = OperationType.GetMetadata;
            break;
          case "blocklist":
            operationType = OperationType.GetBlockList;
            break;
          case "pagelist":
            operationType = OperationType.GetPageList;
            break;
          default:
            break;
        }
      } else if (conn.getRequestProperty("x-ms-range") != null) {
        operationType = OperationType.GetBlob;
      }
    } else if (method.equalsIgnoreCase("HEAD")) {
      operationType = OperationType.GetProperties;
    } else if (method.equalsIgnoreCase("DELETE")) {
      String resType = getQueryParameter(conn.getURL(),
          "restype");
      if (resType != null
          && resType.equalsIgnoreCase("container")) {
        operationType = operationType.DeleteContainer;
      } else {
        operationType = OperationType.DeleteBlob;
      }
    }
    return operationType;
  }

  private static String getQueryParameter(URL url, String queryParameterName) {
    String query = (url != null) ? url.getQuery(): null;

    if (query == null) {
      return null;
    }

    String searchValue = queryParameterName + "=";

    int offset = query.indexOf(searchValue);
    String value = null;
    if (offset != -1) {
      int beginIndex = offset + searchValue.length();
      int endIndex = query.indexOf('&', beginIndex);
      value = (endIndex == -1)
          ? query.substring(beginIndex)
          : query.substring(beginIndex, endIndex);
    }
    return value;
  }

  @InterfaceAudience.Private
  enum OperationType {
    AppendBlock,
    CreateBlob,
    CreateContainer,
    DeleteBlob,
    DeleteContainer,
    GetBlob,
    GetBlockList,
    GetMetadata,
    GetPageList,
    GetProperties,
    ListBlobs,
    PutBlock,
    PutBlockList,
    PutPage,
    SetMetadata,
    SetProperties,
    Unknown
  }
}
