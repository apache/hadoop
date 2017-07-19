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

package org.apache.hadoop.ozone.web.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.headers.Header;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Set of Utility functions used in ozone.
 */
@InterfaceAudience.Private
public final class OzoneUtils {

  public static final String ENCODING_NAME = "UTF-8";
  public static final Charset ENCODING = Charset.forName(ENCODING_NAME);

  private OzoneUtils() {
    // Never constructed
  }

  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName)
      throws IllegalArgumentException {

    if (resName == null) {
      throw new IllegalArgumentException("Bucket or Volume name is null");
    }

    if ((resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH) ||
        (resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH)) {
      throw new IllegalArgumentException(
          "Bucket or Volume length is illegal, " +
              "valid length is 3-63 characters");
    }

    if ((resName.charAt(0) == '.') || (resName.charAt(0) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot start with a period or dash");
    }

    if ((resName.charAt(resName.length() - 1) == '.') ||
        (resName.charAt(resName.length() - 1) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot end with a period or dash");
    }

    boolean isIPv4 = true;
    char prev = (char) 0;

    for (int index = 0; index < resName.length(); index++) {
      char currChar = resName.charAt(index);

      if (currChar != '.') {
        isIPv4 = ((currChar >= '0') && (currChar <= '9')) && isIPv4;
      }

      if (currChar > 'A' && currChar < 'Z') {
        throw new IllegalArgumentException(
            "Bucket or Volume name does not support uppercase characters");
      }


      if ((currChar != '.') && (currChar != '-')) {
        if ((currChar < '0') || (currChar > '9' && currChar < 'a') ||
            (currChar > 'z')) {
          throw new IllegalArgumentException("Bucket or Volume name has an " +
                                                 "unsupported character : " +
                                                 currChar);
        }
      }

      if ((prev == '.') && (currChar == '.')) {
        throw new IllegalArgumentException("Bucket or Volume name should not " +
                                               "have two contiguous periods");
      }

      if ((prev == '-') && (currChar == '.')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have period after dash");
      }

      if ((prev == '.') && (currChar == '-')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have dash after period");
      }
      prev = currChar;
    }

    if (isIPv4) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot be an IPv4 address or all numeric");
    }
  }

  /**
   * Verifies that max key length is a valid value.
   *
   * @param length
   *          The max key length to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyMaxKeyLength(String length)
      throws IllegalArgumentException {
    int maxKey = 0;
    try {
      maxKey = Integer.parseInt(length);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(
          "Invalid max key length, the vaule should be digital.");
    }

    if (maxKey <= 0) {
      throw new IllegalArgumentException(
          "Invalid max key length, the vaule should be a positive number.");
    }
  }

  /**
   * Returns a random Request ID.
   *
   * Request ID is returned to the client as well as flows through the system
   * facilitating debugging on why a certain request failed.
   *
   * @return String random request ID
   */
  public static String getRequestID() {
    return UUID.randomUUID().toString();
  }

  /**
   * Return host name if possible.
   *
   * @return Host Name or localhost
   */
  public static String getHostName() {
    String host = "localhost";
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // Ignore the error
    }
    return host;
  }

  /**
   * Basic validate routine to make sure that all the
   * required headers are in place.
   *
   * @param request - http request
   * @param headers - http headers
   * @param reqId - request id
   * @param resource - Resource Name
   * @param hostname - Hostname
   *
   * @throws OzoneException
   */
  public static void validate(Request request, HttpHeaders headers,
                              String reqId, String resource, String hostname)
      throws OzoneException {

    List<String> ozHeader =
        headers.getRequestHeader(Header.OZONE_VERSION_HEADER);
    if (ozHeader == null) {
      throw ErrorTable
          .newError(ErrorTable.MISSING_VERSION, reqId, resource, hostname);
    }

    List<String> date = headers.getRequestHeader(HttpHeaders.DATE);
    if (date == null) {
      throw ErrorTable
          .newError(ErrorTable.MISSING_DATE, reqId, resource, hostname);
    }

    /*
    TODO :
    Ignore the results for time being. Eventually we can validate if the
    request Date time is too skewed and reject if it is so.
    */
    parseDate(date.get(0), reqId, resource, hostname);

  }

  /**
   * Parses the Date String coming from the Users.
   *
   * @param dateString - Date String
   * @param reqID - Ozone Request ID
   * @param resource - Resource Name
   * @param hostname - HostName
   *
   * @return - Date
   *
   * @throws OzoneException - in case of parsing error
   */
  public static synchronized Date parseDate(String dateString, String reqID,
                                            String resource, String hostname)
      throws OzoneException {
    SimpleDateFormat format =
        new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
    format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));

    try {
      return format.parse(dateString);
    } catch (ParseException ex) {
      OzoneException exp =
          ErrorTable.newError(ErrorTable.BAD_DATE, reqID, resource, hostname);
      exp.setMessage(ex.getMessage());
      throw exp;
    }
  }

  /**
   * Returns a response with appropriate OZONE headers and payload.
   *
   * @param args - UserArgs or Inherited class
   * @param statusCode - HttpStatus code
   * @param payload - Content Body
   *
   * @return JAX-RS Response
   */
  public static Response getResponse(UserArgs args, int statusCode,
                                     String payload) {
    SimpleDateFormat format =
        new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
    format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));
    String date = format.format(new Date(System.currentTimeMillis()));
    return Response.ok(payload)
        .header(Header.OZONE_SERVER_NAME, args.getHostName())
        .header(Header.OZONE_REQUEST_ID, args.getRequestID())
        .header(HttpHeaders.DATE, date).status(statusCode).build();
  }

  /**
   * Returns a response with appropriate OZONE headers and payload.
   *
   * @param args - UserArgs or Inherited class
   * @param statusCode - HttpStatus code
   * @param stream InputStream
   *
   * @return JAX-RS Response
   */
  public static Response getResponse(UserArgs args, int statusCode,
                                     LengthInputStream stream) {
    SimpleDateFormat format =
        new SimpleDateFormat(OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
    format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));
    String date = format.format(new Date(System.currentTimeMillis()));
    return Response.ok(stream, MediaType.APPLICATION_OCTET_STREAM)
        .header(Header.OZONE_SERVER_NAME, args.getHostName())
        .header(Header.OZONE_REQUEST_ID, args.getRequestID())
        .header(HttpHeaders.DATE, date).status(statusCode)
        .header(HttpHeaders.CONTENT_LENGTH, stream.getLength())
        .build();

  }

  /**
   * Checks and creates Ozone Metadir Path if it does not exist.
   *
   * @param conf - Configuration
   *
   * @return File MetaDir
   */
  public static File getScmMetadirPath(Configuration conf) {
    String metaDirPath = conf.getTrimmed(OzoneConfigKeys
        .OZONE_CONTAINER_METADATA_DIRS);
    Preconditions.checkNotNull(metaDirPath);
    File dirPath = new File(metaDirPath);
    if (!dirPath.exists() && !dirPath.mkdirs()) {
      throw new IllegalArgumentException("Unable to create paths. Path: " +
          dirPath);
    }
    return dirPath;
  }
}
