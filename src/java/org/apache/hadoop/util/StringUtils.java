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

package org.apache.hadoop.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.*;

/**
 * General string utils
 * @author Owen O'Malley
 */
public class StringUtils {

  /**
   * Make a string representation of the exception.
   * @param e The exception to stringify
   * @return A string with exception name and call stack.
   */
  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }
  
  /**
   * Given a full hostname, return the word upto the first dot.
   * @param fullHostname the full hostname
   * @return the hostname to the first dot
   */
  public static String simpleHostname(String fullHostname) {
    int offset = fullHostname.indexOf('.');
    if (offset != -1) {
      return fullHostname.substring(0, offset);
    }
    return fullHostname;
  }

  private static DecimalFormat oneDecimal = new DecimalFormat("0.0");
  
  /**
   * Given an integer, return a string that is in an approximate, but human 
   * readable format. 
   * It uses the bases 'k', 'm', and 'g' for 1024, 1024**2, and 1024**3.
   * @param number the number to format
   * @return a human readable form of the integer
   */
  public static String humanReadableInt(long number) {
    long absNumber = Math.abs(number);
    double result = number;
    String suffix = "";
    if (absNumber < 1024) {
      // nothing
    } else if (absNumber < 1024 * 1024) {
      result = number / 1024.0;
      suffix = "k";
    } else if (absNumber < 1024 * 1024 * 1024) {
      result = number / (1024.0 * 1024);
      suffix = "m";
    } else {
      result = number / (1024.0 * 1024 * 1024);
      suffix = "g";
    }
    return oneDecimal.format(result) + suffix;
  }
  
  /**
   * Format a percentage for presentation to the user.
   * @param done the percentage to format (0.0 to 1.0)
   * @param digits the number of digits past the decimal point
   * @return a string representation of the percentage
   */
  public static String formatPercent(double done, int digits) {
    DecimalFormat percentFormat = new DecimalFormat("0.00%");
    double scale = Math.pow(10.0, digits+2);
    double rounded = Math.floor(done * scale);
    percentFormat.setDecimalSeparatorAlwaysShown(false);
    percentFormat.setMinimumFractionDigits(digits);
    percentFormat.setMaximumFractionDigits(digits);
    return percentFormat.format(rounded / scale);
  }
  
  /**
   * Given an array of strings, return a comma-separated list of its elements.
   * @param strs Array of strings
   * @return Empty string if strs.length is 0, comma separated list of strings
   * otherwise
   */
  
  public static String arrayToString(String[] strs) {
    if (strs.length == 0) { return ""; }
    StringBuffer sbuf = new StringBuffer();
    sbuf.append(strs[0]);
    for (int idx = 1; idx < strs.length; idx++) {
      sbuf.append(",");
      sbuf.append(strs[idx]);
    }
    return sbuf.toString();
  }

  /**
   * Given an array of bytes it will convert the bytes to a hex string
   * representation of the bytes
   * @param bytes
   * @return hex string representation of the byte array
   */
  public static String byteToHexString(byte bytes[]) {
    StringBuffer retString = new StringBuffer();
    for (int i = 0; i < bytes.length; ++i) {
      retString.append(Integer.toHexString(0x0100 + (bytes[i] & 0x00FF))
          .substring(1));
    }
    return retString.toString();
  }

  /**
   * Given a hexstring this will return the byte array corresponding to the
   * string
   * @param hex the hex String array
   * @return a byte array that is a hex string representation of the given
   *         string. The size of the byte array is therefore hex.length/2
   */
  public static byte[] hexStringToByte(String hex) {
    byte[] bts = new byte[hex.length() / 2];
    for (int i = 0; i < bts.length; i++) {
      bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bts;
  }
  /**
   * 
   * @param uris
   */
  public static String uriToString(URI[] uris){
    String ret = null;
    ret = uris[0].toString();
    for(int i = 1; i < uris.length;i++){
      ret = ret + "," + uris[i].toString();
    }
    return ret;
  }
  
  /**
   * 
   * @param str
   */
  public static URI[] stringToURI(String[] str){
    if (str == null) 
      return null;
    URI[] uris = new URI[str.length];
    for (int i = 0; i < str.length;i++){
      try{
        uris[i] = new URI(str[i]);
      }catch(URISyntaxException ur){
        System.out.println("Exception in specified URI's " + StringUtils.stringifyException(ur));
        //making sure its asssigned to null in case of an error
        uris[i] = null;
      }
    }
    return uris;
  }
  
  /**
   * 
   * @param str
   */
  public static Path[] stringToPath(String[] str){
    Path[] p = new Path[str.length];
    for (int i = 0; i < str.length;i++){
      p[i] = new Path(str[i]);
    }
    return p;
  }
  /**
   * 
   * Given a finish and start time in long milliseconds, returns a 
   * String in the format Xhrs, Ymins, Z sec, for the time difference between two times. 
   * If finish time comes before start time then negative valeus of X, Y and Z wil return. 
   * 
   * @param finishTime finish time
   * @param startTime start time
   */
  public static String formatTimeDiff(long finishTime, long startTime){
    StringBuffer buf = new StringBuffer() ;
    
    long timeDiff = finishTime - startTime ; 
    long hours = timeDiff / (60*60*1000) ;
    long rem = (timeDiff % (60*60*1000)) ;
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000) ;
    long seconds = rem / 1000 ;
    
    if( hours != 0 ){
      buf.append(hours);
      buf.append("hrs, ");
    }
    if( minutes != 0 ){
      buf.append(minutes);
      buf.append("mins, ");
    }
    // return "0sec if no difference
    buf.append(seconds);
    buf.append("sec");
    return buf.toString(); 
  }
  /**
   * Formats time in ms and appends difference (finishTime - startTime) 
   * as returned by formatTimeDiff().
   * If finish time is 0, empty string is returned, if start time is 0 
   * then difference is not appended to return value. 
   * @param dateFormat date format to use
   * @param finishTime fnish time
   * @param startTime start time
   * @return formatted value. 
   */
  public static String getFormattedTimeWithDiff(DateFormat dateFormat, 
      long finishTime, long startTime){
    StringBuffer buf = new StringBuffer();
    if( 0 != finishTime ) {
      buf.append(dateFormat.format(new Date(finishTime)));
      if( 0 != startTime ){
        buf.append(" (" + formatTimeDiff(finishTime , startTime) + ")");
      }
    }
    return buf.toString();
  }
  
  /**
   * returns an arraylist of strings  
   * @param str the comma seperated string values
   * @return the arraylist of the comma seperated string values
   */
  public static String[] getStrings(String str){
    if (str == null)
      return null;
    StringTokenizer tokenizer = new StringTokenizer (str,",");
    List values = new ArrayList();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return (String[])values.toArray(new String[values.size()]);
  }
}