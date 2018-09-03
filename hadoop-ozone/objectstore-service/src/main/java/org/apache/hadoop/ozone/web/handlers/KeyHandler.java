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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.ozone.OzoneRestUtils;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.interfaces.Keys;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.response.KeyInfo;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import org.apache.commons.codec.binary.Hex;

/**
 * KeyHandler deals with basic Key Operations.
 */
public class KeyHandler implements Keys {

  /**
   * Gets the Key/key information if it exists.
   *
   * @param volume  Storage Volume
   * @param bucket  Name of the bucket
   * @param key Name of the key
   * @param info Tag info
   * @param req Request
   * @param uriInfo Uri Info
   * @param headers Http Header
   * @return Response
   * @throws OzoneException
   */
  @Override
  public Response getKey(String volume, String bucket, String key, String info,
      Request req, UriInfo uriInfo, HttpHeaders headers)
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
       * @param uriInfo - UriInfo
       *
       * @return Response
       *
       * @throws IOException - From the file system operations
       */
      @Override
      public Response doProcess(KeyArgs args, InputStream input,
                                Request request, HttpHeaders headers,
                                UriInfo uriInfo)
          throws IOException, OzoneException, NoSuchAlgorithmException {
        if (info == null) {
          return getKey(args);
        } else if (info.equals(Header.OZONE_INFO_QUERY_KEY)) {
          return getKeyInfo(args);
        } else if (info.equals(Header.OZONE_INFO_QUERY_KEY_DETAIL)) {
          return getKeyInfoDetail(args);
        }

        OzoneException ozException = ErrorTable
            .newError(ErrorTable.INVALID_QUERY_PARAM, args);
        ozException.setMessage("Unrecognized query param : " + info);
        throw ozException;
      }
    }.handleCall(volume, bucket, key, req, headers, uriInfo, null);
  }

  /**
   * Gets the Key if it exists.
   */
  private Response getKey(KeyArgs args)
      throws IOException, OzoneException {
    StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
    LengthInputStream stream = fs.newKeyReader(args);
    return OzoneRestUtils.getResponse(args, HTTP_OK, stream);
  }

  /**
   * Gets the Key information if it exists.
   */
  private Response getKeyInfo(KeyArgs args)
      throws IOException, OzoneException {
    StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
    KeyInfo keyInfo = fs.getKeyInfo(args);
    return OzoneRestUtils.getResponse(args, HTTP_OK, keyInfo.toJsonString());
  }

  /**
   * Gets the Key detail information if it exists.
   */
  private Response getKeyInfoDetail(KeyArgs args)
      throws IOException, OzoneException {
    StorageHandler fs = StorageHandlerBuilder.getStorageHandler();
    KeyInfo keyInfo = fs.getKeyInfoDetails(args);
    return OzoneRestUtils.getResponse(args, HTTP_OK, keyInfo.toJsonString());
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
        args.setSize(contentLen);

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
        return OzoneRestUtils.getResponse(args, HTTP_CREATED, "");
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
        return OzoneRestUtils.getResponse(args, HTTP_OK, "");
      }
    }.handleCall(volume, bucket, keys, req, headers, info, null);
  }

  /**
   * Renames an existing key within a bucket.
   *
   * @param volume      Storage Volume Name
   * @param bucket      Name of the bucket
   * @param key         Name of the Object
   * @param toKeyName   New name of the Object
   * @param req         http Request
   * @param info        UriInfo
   * @param headers     HttpHeaders
   * @return Response
   * @throws OzoneException
   */
  @Override
  public Response renameKey(String volume, String bucket, String key,
      String toKeyName, Request req, UriInfo info, HttpHeaders headers)
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
        fs.renameKey(args, toKeyName);
        return OzoneRestUtils.getResponse(args, HTTP_OK, "");
      }
    }.handleCall(volume, bucket, key, req, headers, info, null);
  }
}
