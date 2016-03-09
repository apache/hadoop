/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.handlers;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.headers.Header;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.interfaces.UserAuth;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.apache.hadoop.ozone.web.exceptions.ErrorTable.BAD_DIGEST;
import static org.apache.hadoop.ozone.web.exceptions.ErrorTable.INCOMPLETE_BODY;
import static org.apache.hadoop.ozone.web.exceptions.ErrorTable.INVALID_BUCKET_NAME;
import static org.apache.hadoop.ozone.web.exceptions.ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.web.exceptions.ErrorTable.SERVER_ERROR;
import static org.apache.hadoop.ozone.web.exceptions.ErrorTable.newError;

/**
 * This class abstracts way the repetitive tasks in  Key handling code.
 */
public abstract class KeyProcessTemplate {

  /**
   * This function serves as the common error handling function for all Key
   * related operations.
   *
   * @param bucket  bucket Name
   * @param key     the object name
   * @param headers Http headers
   * @param is      Input XML stream
   * @throws OzoneException
   */
  public Response handleCall(String volume, String bucket, String key,
                             Request request, HttpHeaders headers, UriInfo info,
                             InputStream is) throws OzoneException {

    String reqID = OzoneUtils.getRequestID();
    String hostName = OzoneUtils.getHostName();
    UserArgs userArgs = null;
    try {
      OzoneUtils.validate(request, headers, reqID, bucket, hostName);
      OzoneUtils.verifyBucketName(bucket);

      UserAuth auth = UserHandlerBuilder.getAuthHandler();
      userArgs = new UserArgs(reqID, hostName, request, info, headers);
      userArgs.setUserName(auth.getUser(userArgs));

      KeyArgs args = new KeyArgs(volume, bucket, key, userArgs);
      return doProcess(args, is, request, headers, info);
    } catch (IllegalArgumentException argExp) {
      OzoneException ex =
          newError(INVALID_BUCKET_NAME, reqID, bucket, hostName);
      ex.setMessage(argExp.getMessage());
      throw ex;
    } catch (IOException fsExp) {
      // TODO : Handle errors from the FileSystem , let us map to server error
      // for now.
      throw ErrorTable.newError(ErrorTable.SERVER_ERROR, userArgs, fsExp);
    } catch (NoSuchAlgorithmException algoEx) {
      OzoneException ex =
          ErrorTable.newError(SERVER_ERROR, reqID, key, hostName);
      ex.setMessage(algoEx.getMessage());
      throw ex;
    }
  }

  /**
   * Abstract function that gets implemented in the KeyHandler functions. This
   * function will just deal with the core file system related logic and will
   * rely on handleCall function for repetitive error checks
   *
   * @param args    - parsed bucket args, name, userName, ACLs etc
   * @param input   - The body as an Input Stream
   * @param request - Http request
   * @param headers - Parsed http Headers.
   * @param info    - UriInfo
   * @return Response
   * @throws IOException - From the file system operations
   */
  public abstract Response doProcess(KeyArgs args, InputStream input,
                                     Request request, HttpHeaders headers,
                                     UriInfo info)
      throws IOException, OzoneException, NoSuchAlgorithmException;

  /**
   * checks if the File Content-MD5 we wrote matches the hash we computed from
   * the stream. if it does match we delete the file and throw and exception to
   * let the user know that we have a hash mismatch
   *
   * @param args           Object Args
   * @param computedString MD5 hash value
   * @param fs             Pointer to File System so we can delete the file
   * @param contentHash    User Specified hash string
   * @throws IOException
   * @throws OzoneException
   */
  public void checkFileHashMatch(KeyArgs args, String computedString,
                                 StorageHandler fs, String contentHash)
      throws IOException, OzoneException {
    if (contentHash != null) {
      String contentString =
          new String(Base64.decodeBase64(contentHash), OzoneUtils.ENCODING)
              .trim();

      if (!contentString.equals(computedString)) {
        fs.deleteKey(args);
        OzoneException ex = ErrorTable.newError(BAD_DIGEST, args.getRequestID(),
            args.getKeyName(), args.getHostName());
        ex.setMessage(String.format("MD5 Digest mismatch. Expected %s Found " +
            "%s", contentString, computedString));
        throw ex;
      }
    }
  }

  /**
   * check if the content-length matches the actual stream length. if we find a
   * mismatch we will delete the file and throw an exception to let the user
   * know that length mismatch detected
   *
   * @param args       Object Args
   * @param fs         Pointer to File System Object, to delete the file that we
   *                   wrote
   * @param contentLen Http Content-Length Header
   * @param bytesRead  Actual Bytes we read from the stream
   * @throws IOException
   * @throws OzoneException
   */
  public void checkFileLengthMatch(KeyArgs args, StorageHandler fs,
                                   int contentLen, int bytesRead)
      throws IOException, OzoneException {
    if (bytesRead != contentLen) {
      fs.deleteKey(args);
      OzoneException ex = ErrorTable.newError(INCOMPLETE_BODY,
          args.getRequestID(), args.getKeyName(), args.getHostName());
      ex.setMessage(String.format("Body length mismatch. Expected length : %d" +
          " Found %d", contentLen, bytesRead));
      throw ex;
    }
  }

  /**
   * Returns Content Length header value if available.
   *
   * @param headers - Http Headers
   * @return - String or null
   */
  public String getContentLength(HttpHeaders headers, KeyArgs args)
      throws OzoneException {
    List<String> contentLengthList =
        headers.getRequestHeader(HttpHeaders.CONTENT_LENGTH);
    if ((contentLengthList != null) && (contentLengthList.size() > 0)) {
      return contentLengthList.get(0);
    }

    OzoneException ex = ErrorTable.newError(INVALID_REQUEST, args);
    ex.setMessage("Content-Length is a required header for putting a key.");
    throw ex;

  }

  /**
   * Returns Content MD5 value if available.
   *
   * @param headers - Http Headers
   * @return - String or null
   */
  public String getContentMD5(HttpHeaders headers, KeyArgs args) {
    List<String> contentLengthList =
        headers.getRequestHeader(Header.CONTENT_MD5);
    if ((contentLengthList != null) && (contentLengthList.size() > 0)) {
      return contentLengthList.get(0);
    }
// TODO : Should we make this compulsory ?
//    OzoneException ex = ErrorTable.newError(ErrorTable.invalidRequest, args);
//    ex.setMessage("Content-MD5 is a required header for putting a key");
//    throw ex;
    return "";
  }
}

