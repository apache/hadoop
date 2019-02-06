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

package org.apache.hadoop.ozone;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ozone.client.io.LengthInputStream;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of Utility functions used in ozone.
 */
@InterfaceAudience.Private
public final class OzoneRestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneRestUtils.class);


  private OzoneRestUtils() {
    // Never constructed
  }

  /**
   * Date format that used in ozone. Here the format is thread safe to use.
   */
  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat format = new SimpleDateFormat(
          OzoneConsts.OZONE_DATE_FORMAT, Locale.US);
      format.setTimeZone(TimeZone.getTimeZone(OzoneConsts.OZONE_TIME_ZONE));

      return format;
    }
  };

  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName)
      throws IllegalArgumentException {
    HddsClientUtils.verifyResourceName(resName);
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
    try {
      return DATE_FORMAT.get().parse(dateString);
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
    String date = DATE_FORMAT.get().format(new Date(Time.now()));
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
    String date = DATE_FORMAT.get().format(new Date(Time.now()));
    return Response.ok(stream, MediaType.APPLICATION_OCTET_STREAM)
        .header(Header.OZONE_SERVER_NAME, args.getHostName())
        .header(Header.OZONE_REQUEST_ID, args.getRequestID())
        .header(HttpHeaders.DATE, date).status(statusCode)
        .header(HttpHeaders.CONTENT_LENGTH, stream.getLength())
        .build();

  }



  /**
   * Convert time in millisecond to a human readable format required in ozone.
   * @return a human readable string for the input time
   */
  public static String formatTime(long millis) {
    return DATE_FORMAT.get().format(millis);
  }

  /**
   * Convert time in ozone date format to millisecond.
   * @return time in milliseconds
   */
  public static long formatDate(String date) throws ParseException {
    Preconditions.checkNotNull(date, "Date string should not be null.");
    return DATE_FORMAT.get().parse(date).getTime();
  }

}
