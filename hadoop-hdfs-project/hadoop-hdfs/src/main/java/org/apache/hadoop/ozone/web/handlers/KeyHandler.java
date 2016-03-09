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

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.interfaces.Keys;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * KeyHandler deals with basic Key Operations.
 */
public class KeyHandler implements Keys {

  /**
   * Gets the Key if it exists.
   *
   * @param volume  Storage Volume
   * @param bucket  Name of the bucket
   * @param req     Request
   * @param info    - UriInfo
   * @param headers Http Header
   * @return Response
   * @throws OzoneException
   */
  @Override
  public Response getKey(String volume, String bucket, String key,
                         Request req, UriInfo info, HttpHeaders headers)
      throws OzoneException {
    return new KeyProcessTemplate() {
      /**
       * Abstract function that gets implemented in the KeyHandler functions.
       * This function will just deal with the core file system related logic
       * and will rely on handleCall function for repetitive error checks
       *
       * @param args - parsed bucket args, name, userName, ACLs etc
       * @param input - The body as an Input Stream
       * @param request - Http request
       * @param headers - Parsed http Headers.
       * @param info - UriInfo
       *
       * @return Response
       *
       * @throws IOException - From the file system operations
       */
      @Override
      public Response doProcess(KeyArgs args, InputStream input,
                                Request request, HttpHeaders headers,
                                UriInfo info)
          throws IOException, OzoneException, NoSuchAlgorithmException {
        StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
        LengthInputStream stream = fs.newKeyReader(args);
        return OzoneUtils.getResponse(args, HTTP_OK, stream);
      }
    }.handleCall(volume, bucket, key, req, headers, info, null);
  }

  /**
   * Adds a key to an existing bucket. If the object already exists this call
   * will overwrite or add with new version number if the bucket versioning is
   * turned on.
   *
   * @param volume  Storage Volume Name
   * @param bucket  Name of the bucket
   * @param keys    Name of the Object
   * @param is      InputStream or File Data
   * @param req     Request
   * @param info    - UriInfo
   * @param headers http headers
   * @return Response
   * @throws OzoneException
   */
  @Override
  public Response putKey(String volume, String bucket, String keys,
                         InputStream is, Request req, UriInfo info,
                         HttpHeaders headers) throws OzoneException {

    return new KeyProcessTemplate() {
      /**
       * Abstract function that gets implemented in the KeyHandler functions.
       * This function will just deal with the core file system related logic
       * and will rely on handleCall function for repetitive error checks
       *
       * @param args - parsed bucket args, name, userName, ACLs etc
       * @param input - The body as an Input Stream
       * @param request - Http request
       * @param headers - Parsed http Headers.
       * @param info - UriInfo
       *
       * @return Response
       *
       * @throws IOException - From the file system operations
       */
      @Override
      public Response doProcess(KeyArgs args, InputStream input,
                                Request request, HttpHeaders headers,
                                UriInfo info)
          throws IOException, OzoneException, NoSuchAlgorithmException {
        final int eof = -1;
        StorageHandler fs = StorageHandlerBuilder.getStorageHandler();

        byte[] buffer = new byte[4 * 1024];
        String contentLenString = getContentLength(headers, args);
        String newLen = contentLenString.replaceAll("\"", "");
        int contentLen = Integer.parseInt(newLen);

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        int bytesRead = 0;
        int len = 0;
        OutputStream stream = fs.newKeyWriter(args);
        while ((bytesRead < contentLen) && (len != eof)) {
          int readSize =
              (contentLen - bytesRead > buffer.length) ? buffer.length :
                  contentLen - bytesRead;
          len = input.read(buffer, 0, readSize);
          if (len != eof) {
            stream.write(buffer, 0, len);
            md5.update(buffer, 0, len);
            bytesRead += len;

          }
        }

        checkFileLengthMatch(args, fs, contentLen, bytesRead);

        String hashString = Hex.encodeHexString(md5.digest());
// TODO : Enable hash value checking.
//          String contentHash = getContentMD5(headers, args);
//          checkFileHashMatch(args, hashString, fs, contentHash);
        args.setHash(hashString);
        args.setSize(bytesRead);
        fs.commitKey(args, stream);
        return OzoneUtils.getResponse(args, HTTP_CREATED, "");
      }
    }.handleCall(volume, bucket, keys, req, headers, info, is);
  }

  /**
   * Deletes an existing key.
   *
   * @param volume  Storage Volume Name
   * @param bucket  Name of the bucket
   * @param keys    Name of the Object
   * @param req     http Request
   * @param info    - UriInfo
   * @param headers HttpHeaders
   * @return Response
   * @throws OzoneException
   */
  @Override
  public Response deleteKey(String volume, String bucket, String keys,
                            Request req, UriInfo info, HttpHeaders headers)
      throws OzoneException {
    return new KeyProcessTemplate() {
      /**
       * Abstract function that gets implemented in the KeyHandler functions.
       * This function will just deal with the core file system related logic
       * and will rely on handleCall function for repetitive error checks
       *
       * @param args - parsed bucket args, name, userName, ACLs etc
       * @param input - The body as an Input Stream
       * @param request - Http request
       * @param headers - Parsed http Headers.
       * @param info - UriInfo
       *
       * @return Response
       *
       * @throws IOException - From the file system operations
       */
      @Override
      public Response doProcess(KeyArgs args, InputStream input,
                                Request request, HttpHeaders headers,
                                UriInfo info)
          throws IOException, OzoneException, NoSuchAlgorithmException {
        StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
        fs.deleteKey(args);
        return OzoneUtils.getResponse(args, HTTP_OK, "");
      }
    }.handleCall(volume, bucket, keys, req, headers, info, null);
  }
}
