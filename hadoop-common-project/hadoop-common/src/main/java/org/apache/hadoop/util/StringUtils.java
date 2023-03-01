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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.LogManager;

import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;

/**
 * General string utils
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StringUtils {

  /**
   * Priority of the StringUtils shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 0;

  /**
   * Shell environment variables: $ followed by one letter or _ followed by
   * multiple letters, numbers, or underscores.  The group captures the
   * environment variable name without the leading $.
   */
  public static final Pattern SHELL_ENV_VAR_PATTERN =
    Pattern.compile("\\$([A-Za-z_]{1}[A-Za-z0-9_]*)");

  /**
   * Windows environment variables: surrounded by %.  The group captures the
   * environment variable name without the leading and trailing %.
   */
  public static final Pattern WIN_ENV_VAR_PATTERN = Pattern.compile("%(.*?)%");

  /**
   * Regular expression that matches and captures environment variable names
   * according to platform-specific rules.
   */
  public static final Pattern ENV_VAR_PATTERN = Shell.WINDOWS ?
    WIN_ENV_VAR_PATTERN : SHELL_ENV_VAR_PATTERN;

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
    if (InetAddresses.isInetAddress(fullHostname)) {
      return fullHostname;
    }
    int offset = fullHostname.indexOf('.');
    if (offset != -1) {
      return fullHostname.substring(0, offset);
    }
    return fullHostname;
  }
  
  /**
   * Given an integer, return a string that is in an approximate, but human 
   * readable format. 
   * @param number the number to format
   * @return a human readable form of the integer
   *
   * @deprecated use {@link TraditionalBinaryPrefix#long2String(long, String, int)}.
   */
  @Deprecated
  public static String humanReadableInt(long number) {
    return TraditionalBinaryPrefix.long2String(number, "", 1);
  }

  /**
   * The same as String.format(Locale.ENGLISH, format, objects).
   * @param format format.
   * @param objects objects.
   * @return format string.
   */
  public static String format(final String format, final Object... objects) {
    return String.format(Locale.ENGLISH, format, objects);
  }

  /**
   * Format a percentage for presentation to the user.
   * @param fraction the percentage as a fraction, e.g. 0.1 = 10%
   * @param decimalPlaces the number of decimal places
   * @return a string representation of the percentage
   */
  public static String formatPercent(double fraction, int decimalPlaces) {
    return format("%." + decimalPlaces + "f%%", fraction*100);
  }
  
  /**
   * Given an array of strings, return a comma-separated list of its elements.
   * @param strs Array of strings
   * @return Empty string if strs.length is 0, comma separated list of strings
   * otherwise
   */
  
  public static String arrayToString(String[] strs) {
    if (strs.length == 0) { return ""; }
    StringBuilder sbuf = new StringBuilder();
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
   * @param bytes bytes.
   * @param start start index, inclusively
   * @param end end index, exclusively
   * @return hex string representation of the byte array
   */
  public static String byteToHexString(byte[] bytes, int start, int end) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes == null");
    }
    StringBuilder s = new StringBuilder(); 
    for(int i = start; i < end; i++) {
      s.append(format("%02x", bytes[i]));
    }
    return s.toString();
  }

  /**
   * Same as byteToHexString(bytes, 0, bytes.length).
   * @param bytes bytes.
   * @return byteToHexString.
   */
  public static String byteToHexString(byte bytes[]) {
    return byteToHexString(bytes, 0, bytes.length);
  }

  /**
   * Convert a byte to a hex string.
   * @see #byteToHexString(byte[])
   * @see #byteToHexString(byte[], int, int)
   * @param b byte
   * @return byte's hex value as a String
   */
  public static String byteToHexString(byte b) {
    return byteToHexString(new byte[] {b});
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
   * uriToString.
   * @param uris uris.
   * @return uriToString.
   */
  public static String uriToString(URI[] uris){
    if (uris == null) {
      return null;
    }
    StringBuilder ret = new StringBuilder(uris[0].toString());
    for(int i = 1; i < uris.length;i++){
      ret.append(",");
      ret.append(uris[i].toString());
    }
    return ret.toString();
  }
  
  /**
   * @param str
   *          The string array to be parsed into an URI array.
   * @return <tt>null</tt> if str is <tt>null</tt>, else the URI array
   *         equivalent to str.
   * @throws IllegalArgumentException
   *           If any string in str violates RFC&nbsp;2396.
   */
  public static URI[] stringToURI(String[] str){
    if (str == null) 
      return null;
    URI[] uris = new URI[str.length];
    for (int i = 0; i < str.length;i++){
      try{
        uris[i] = new URI(str[i]);
      }catch(URISyntaxException ur){
        throw new IllegalArgumentException(
            "Failed to create uri for " + str[i], ur);
      }
    }
    return uris;
  }
  
  /**
   * stringToPath.
   * @param str str.
   * @return path array.
   */
  public static Path[] stringToPath(String[] str){
    if (str == null) {
      return null;
    }
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
   * @return a String in the format Xhrs, Ymins, Z sec,
   *         for the time difference between two times.
   */
  public static String formatTimeDiff(long finishTime, long startTime){
    long timeDiff = finishTime - startTime; 
    return formatTime(timeDiff); 
  }
  
  /**
   * 
   * Given the time in long milliseconds, returns a 
   * String in the format Xhrs, Ymins, Z sec. 
   * 
   * @param timeDiff The time difference to format
   * @return formatTime String.
   */
  public static String formatTime(long timeDiff){
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60*60*1000);
    long rem = (timeDiff % (60*60*1000));
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000);
    long seconds = rem / 1000;
    
    if (hours != 0){
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0){
      buf.append(minutes);
      buf.append("mins, ");
    }
    // return "0sec if no difference
    buf.append(seconds);
    buf.append("sec");
    return buf.toString(); 
  }

  /**
   *
   * Given the time in long milliseconds, returns a String in the sortable
   * format Xhrs, Ymins, Zsec. X, Y, and Z are always two-digit. If the time is
   * more than 100 hours ,it is displayed as 99hrs, 59mins, 59sec.
   *
   * @param timeDiff The time difference to format
   * @return format time sortable.
   */
  public static String formatTimeSortable(long timeDiff) {
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60 * 60 * 1000);
    long rem = (timeDiff % (60 * 60 * 1000));
    long minutes = rem / (60 * 1000);
    rem = rem % (60 * 1000);
    long seconds = rem / 1000;

    // if hours is more than 99 hours, it will be set a max value format
    if (hours > 99) {
      hours = 99;
      minutes = 59;
      seconds = 59;
    }

    buf.append(String.format("%02d", hours));
    buf.append("hrs, ");

    buf.append(String.format("%02d", minutes));
    buf.append("mins, ");

    buf.append(String.format("%02d", seconds));
    buf.append("sec");
    return buf.toString();
  }

  /**
   * Formats time in ms and appends difference (finishTime - startTime)
   * as returned by formatTimeDiff().
   * If finish time is 0, empty string is returned, if start time is 0
   * then difference is not appended to return value.
   *
   * @param dateFormat date format to use
   * @param finishTime finish time
   * @param startTime  start time
   * @return formatted value.
   */
  public static String getFormattedTimeWithDiff(FastDateFormat dateFormat,
      long finishTime, long startTime) {
    String formattedFinishTime = dateFormat.format(finishTime);
    return getFormattedTimeWithDiff(formattedFinishTime, finishTime, startTime);
  }
  /**
   * Formats time in ms and appends difference (finishTime - startTime)
   * as returned by formatTimeDiff().
   * If finish time is 0, empty string is returned, if start time is 0
   * then difference is not appended to return value.
   * @param formattedFinishTime formattedFinishTime to use
   * @param finishTime finish time
   * @param startTime start time
   * @return formatted value.
   */
  public static String getFormattedTimeWithDiff(String formattedFinishTime,
      long finishTime, long startTime){
    StringBuilder buf = new StringBuilder();
    if (0 != finishTime) {
      buf.append(formattedFinishTime);
      if (0 != startTime){
        buf.append(" (" + formatTimeDiff(finishTime , startTime) + ")");
      }
    }
    return buf.toString();
  }
  
  /**
   * Returns an arraylist of strings.
   * @param str the comma separated string values
   * @return the arraylist of the comma separated string values
   */
  public static String[] getStrings(String str){
    String delim = ",";
    return getStrings(str, delim);
  }

  /**
   * Returns an arraylist of strings.
   * @param str the string values
   * @param delim delimiter to separate the values
   * @return the arraylist of the separated string values
   */
  public static String[] getStrings(String str, String delim){
    Collection<String> values = getStringCollection(str, delim);
    if(values.size() == 0) {
      return null;
    }
    return values.toArray(new String[values.size()]);
  }

  /**
   * Returns a collection of strings.
   * @param str comma separated string values
   * @return an <code>ArrayList</code> of string values
   */
  public static Collection<String> getStringCollection(String str){
    String delim = ",";
    return getStringCollection(str, delim);
  }

  /**
   * Returns a collection of strings.
   * 
   * @param str
   *          String to parse
   * @param delim
   *          delimiter to separate the values
   * @return Collection of parsed elements.
   */
  public static Collection<String> getStringCollection(String str, String delim) {
    List<String> values = new ArrayList<String>();
    if (str == null)
      return values;
    StringTokenizer tokenizer = new StringTokenizer(str, delim);
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return values;
  }

  /**
   * Returns a collection of strings, trimming leading and trailing whitespace
   * on each value. Duplicates are not removed.
   *
   * @param str
   *          String separated by delim.
   * @param delim
   *          Delimiter to separate the values in str.
   * @return Collection of string values.
   */
  public static Collection<String> getTrimmedStringCollection(String str,
      String delim) {
    List<String> values = new ArrayList<String>();
    if (str == null)
      return values;
    StringTokenizer tokenizer = new StringTokenizer(str, delim);
    while (tokenizer.hasMoreTokens()) {
      String next = tokenizer.nextToken();
      if (next == null || next.trim().isEmpty()) {
        continue;
      }
      values.add(next.trim());
    }
    return values;
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and
   * trailing whitespace on each value. Duplicate and empty values are removed.
   *
   * @param str a comma separated <code>String</code> with values, may be null
   * @return a <code>Collection</code> of <code>String</code> values, empty
   *         Collection if null String input
   */
  public static Collection<String> getTrimmedStringCollection(String str){
    Set<String> set = new LinkedHashSet<String>(
      Arrays.asList(getTrimmedStrings(str)));
    set.remove("");
    return set;
  }
  
  /**
   * Splits a comma or newline separated value <code>String</code>, trimming
   * leading and trailing whitespace on each value.
   *
   * @param str a comma or newline separated <code>String</code> with values,
   *            may be null
   * @return an array of <code>String</code> values, empty array if null String
   *         input
   */
  public static String[] getTrimmedStrings(String str){
    if (null == str || str.trim().isEmpty()) {
      return emptyStringArray;
    }

    return str.trim().split("\\s*[,\n]\\s*");
  }

  final public static String[] emptyStringArray = {};
  final public static char COMMA = ',';
  final public static String COMMA_STR = ",";
  final public static char ESCAPE_CHAR = '\\';
  
  /**
   * Split a string using the default separator
   * @param str a string that may have escaped separator
   * @return an array of strings
   */
  public static String[] split(String str) {
    return split(str, ESCAPE_CHAR, COMMA);
  }
  
  /**
   * Split a string using the given separator
   * @param str a string that may have escaped separator
   * @param escapeChar a char that be used to escape the separator
   * @param separator a separator char
   * @return an array of strings
   */
  public static String[] split(
      String str, char escapeChar, char separator) {
    if (str==null) {
      return null;
    }
    ArrayList<String> strList = new ArrayList<String>();
    StringBuilder split = new StringBuilder();
    int index = 0;
    while ((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
      ++index; // move over the separator for next search
      strList.add(split.toString());
      split.setLength(0); // reset the buffer 
    }
    strList.add(split.toString());
    // remove trailing empty split(s)
    int last = strList.size(); // last split
    while (--last>=0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new String[strList.size()]);
  }

  /**
   * Split a string using the given separator, with no escaping performed.
   * @param str a string to be split. Note that this may not be null.
   * @param separator a separator char
   * @return an array of strings
   */
  public static String[] split(
      String str, char separator) {
    // String.split returns a single empty result for splitting the empty
    // string.
    if (str.isEmpty()) {
      return new String[]{""};
    }
    ArrayList<String> strList = new ArrayList<String>();
    int startIndex = 0;
    int nextIndex = 0;
    while ((nextIndex = str.indexOf(separator, startIndex)) != -1) {
      strList.add(str.substring(startIndex, nextIndex));
      startIndex = nextIndex + 1;
    }
    strList.add(str.substring(startIndex));
    // remove trailing empty split(s)
    int last = strList.size(); // last split
    while (--last>=0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new String[strList.size()]);
  }
  
  /**
   * Finds the first occurrence of the separator character ignoring the escaped
   * separators starting from the index. Note the substring between the index
   * and the position of the separator is passed.
   * @param str the source string
   * @param separator the character to find
   * @param escapeChar character used to escape
   * @param start from where to search
   * @param split used to pass back the extracted string
   * @return index.
   */
  public static int findNext(String str, char separator, char escapeChar, 
                             int start, StringBuilder split) {
    int numPreEscapes = 0;
    for (int i = start; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if (numPreEscapes == 0 && curChar == separator) { // separator 
        return i;
      } else {
        split.append(curChar);
        numPreEscapes = (curChar == escapeChar)
                        ? (++numPreEscapes) % 2
                        : 0;
      }
    }
    return -1;
  }
  
  /**
   * Escape commas in the string using the default escape char
   * @param str a string
   * @return an escaped string
   */
  public static String escapeString(String str) {
    return escapeString(str, ESCAPE_CHAR, COMMA);
  }
  
  /**
   * Escape <code>charToEscape</code> in the string 
   * with the escape char <code>escapeChar</code>
   * 
   * @param str string
   * @param escapeChar escape char
   * @param charToEscape the char to be escaped
   * @return an escaped string
   */
  public static String escapeString(
      String str, char escapeChar, char charToEscape) {
    return escapeString(str, escapeChar, new char[] {charToEscape});
  }
  
  // check if the character array has the character 
  private static boolean hasChar(char[] chars, char character) {
    for (char target : chars) {
      if (character == target) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * escapeString.
   *
   * @param str str.
   * @param escapeChar escapeChar.
   * @param charsToEscape array of characters to be escaped
   * @return escapeString.
   */
  public static String escapeString(String str, char escapeChar, 
                                    char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    for (int i=0; i<str.length(); i++) {
      char curChar = str.charAt(i);
      if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
        // special char
        result.append(escapeChar);
      }
      result.append(curChar);
    }
    return result.toString();
  }
  
  /**
   * Unescape commas in the string using the default escape char
   * @param str a string
   * @return an unescaped string
   */
  public static String unEscapeString(String str) {
    return unEscapeString(str, ESCAPE_CHAR, COMMA);
  }
  
  /**
   * Unescape <code>charToEscape</code> in the string 
   * with the escape char <code>escapeChar</code>
   * 
   * @param str string
   * @param escapeChar escape char
   * @param charToEscape the escaped char
   * @return an unescaped string
   */
  public static String unEscapeString(
      String str, char escapeChar, char charToEscape) {
    return unEscapeString(str, escapeChar, new char[] {charToEscape});
  }
  
  /**
   * unEscapeString.
   * @param str str.
   * @param escapeChar escapeChar.
   * @param charsToEscape array of characters to unescape
   * @return escape string.
   */
  public static String unEscapeString(String str, char escapeChar, 
                                      char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder(str.length());
    boolean hasPreEscape = false;
    for (int i=0; i<str.length(); i++) {
      char curChar = str.charAt(i);
      if (hasPreEscape) {
        if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
          // no special char
          throw new IllegalArgumentException("Illegal escaped string " + str + 
              " unescaped " + escapeChar + " at " + (i-1));
        } 
        // otherwise discard the escape char
        result.append(curChar);
        hasPreEscape = false;
      } else {
        if (hasChar(charsToEscape, curChar)) {
          throw new IllegalArgumentException("Illegal escaped string " + str + 
              " unescaped " + curChar + " at " + i);
        } else if (curChar == escapeChar) {
          hasPreEscape = true;
        } else {
          result.append(curChar);
        }
      }
    }
    if (hasPreEscape ) {
      throw new IllegalArgumentException("Illegal escaped string " + str + 
          ", not expecting " + escapeChar + " in the end." );
    }
    return result.toString();
  }
  
  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  public static String toStartupShutdownString(String prefix, String[] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for(String s : msg)
      b.append("\n").append(prefix).append(s);
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Print a log message for starting up and shutting down
   * @param clazz the class of the server
   * @param args arguments
   * @param log the target log object
   */
  public static void startupShutdownMessage(Class<?> clazz, String[] args,
                                     final org.slf4j.Logger log) {
    final String hostname = NetUtils.getHostname();
    final String classname = clazz.getSimpleName();
    log.info(createStartupShutdownMessage(classname, hostname, args));

    if (SystemUtils.IS_OS_UNIX) {
      try {
        SignalLogger.INSTANCE.register(log);
      } catch (Throwable t) {
        log.warn("failed to register any UNIX signal loggers: ", t);
      }
    }
    ShutdownHookManager.get().addShutdownHook(
      new Runnable() {
        @Override
        public void run() {
          log.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
            "Shutting down " + classname + " at " + hostname}));
          LogManager.shutdown();
        }
      }, SHUTDOWN_HOOK_PRIORITY);

  }

  /**
   * Generate the text for the startup/shutdown message of processes.
   * @param classname short classname of the class
   * @param hostname hostname
   * @param args Command arguments
   * @return a string to log.
   */
  public static String createStartupShutdownMessage(String classname,
      String hostname, String[] args) {
    return toStartupShutdownString("STARTUP_MSG: ", new String[] {
        "Starting " + classname,
        "  host = " + hostname,
        "  args = " + (args != null ? Arrays.asList(args) : new ArrayList<>()),
        "  version = " + VersionInfo.getVersion(),
        "  classpath = " + System.getProperty("java.class.path"),
        "  build = " + VersionInfo.getUrl() + " -r "
                     + VersionInfo.getRevision()  
                     + "; compiled by '" + VersionInfo.getUser()
                     + "' on " + VersionInfo.getDate(),
        "  java = " + System.getProperty("java.version") }
    );
  }

  /**
   * The traditional binary prefixes, kilo, mega, ..., exa,
   * which can be represented by a 64-bit integer.
   * TraditionalBinaryPrefix symbol are case insensitive. 
   */
  public enum TraditionalBinaryPrefix {
    KILO(10),
    MEGA(KILO.bitShift + 10),
    GIGA(MEGA.bitShift + 10),
    TERA(GIGA.bitShift + 10),
    PETA(TERA.bitShift + 10),
    EXA (PETA.bitShift + 10);

    public final long value;
    public final char symbol;
    public final int bitShift;
    public final long bitMask;

    private TraditionalBinaryPrefix(int bitShift) {
      this.bitShift = bitShift;
      this.value = 1L << bitShift;
      this.bitMask = this.value - 1L;
      this.symbol = toString().charAt(0);
    }

    /**
     * The TraditionalBinaryPrefix object corresponding to the symbol.
     *
     * @param symbol symbol.
     * @return traditional binary prefix object.
     */
    public static TraditionalBinaryPrefix valueOf(char symbol) {
      symbol = Character.toUpperCase(symbol);
      for(TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
        if (symbol == prefix.symbol) {
          return prefix;
        }
      }
      throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
    }

    /**
     * Convert a string to long.
     * The input string is first be trimmed
     * and then it is parsed with traditional binary prefix.
     *
     * For example,
     * "-1230k" will be converted to -1230 * 1024 = -1259520;
     * "891g" will be converted to 891 * 1024^3 = 956703965184;
     *
     * @param s input string
     * @return a long value represented by the input string.
     */
    public static long string2long(String s) {
      s = s.trim();
      final int lastpos = s.length() - 1;
      final char lastchar = s.charAt(lastpos);
      if (Character.isDigit(lastchar))
        return Long.parseLong(s);
      else {
        long prefix;
        try {
          prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid size prefix '" + lastchar
              + "' in '" + s
              + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)");
        }
        long num = Long.parseLong(s.substring(0, lastpos));
        if (num > (Long.MAX_VALUE/prefix) || num < (Long.MIN_VALUE/prefix)) {
          throw new IllegalArgumentException(s + " does not fit in a Long");
        }
        return num * prefix;
      }
    }

    /**
     * Convert a long integer to a string with traditional binary prefix.
     * 
     * @param n the value to be converted
     * @param unit The unit, e.g. "B" for bytes.
     * @param decimalPlaces The number of decimal places.
     * @return a string with traditional binary prefix.
     */
    public static String long2String(long n, String unit, int decimalPlaces) {
      if (unit == null) {
        unit = "";
      }
      //take care a special case
      if (n == Long.MIN_VALUE) {
        return "-8 " + EXA.symbol + unit;
      }

      final StringBuilder b = new StringBuilder();
      //take care negative numbers
      if (n < 0) {
        b.append('-');
        n = -n;
      }
      if (n < KILO.value) {
        //no prefix
        b.append(n);
        return (unit.isEmpty()? b: b.append(" ").append(unit)).toString();
      } else {
        //find traditional binary prefix
        int i = 0;
        for(; i < values().length && n >= values()[i].value; i++);
        TraditionalBinaryPrefix prefix = values()[i - 1];

        if ((n & prefix.bitMask) == 0) {
          //exact division
          b.append(n >> prefix.bitShift);
        } else {
          final String  format = "%." + decimalPlaces + "f";
          String s = format(format, n/(double)prefix.value);
          //check a special rounding up case
          if (s.startsWith("1024")) {
            prefix = values()[i];
            s = format(format, n/(double)prefix.value);
          }
          b.append(s);
        }
        return b.append(' ').append(prefix.symbol).append(unit).toString();
      }
    }
  }

    /**
     * Escapes HTML Special characters present in the string.
     * @param string param string.
     * @return HTML Escaped String representation
     */
    public static String escapeHTML(String string) {
      if(string == null) {
        return null;
      }
      StringBuilder sb = new StringBuilder();
      boolean lastCharacterWasSpace = false;
      char[] chars = string.toCharArray();
      for(char c : chars) {
        if(c == ' ') {
          if(lastCharacterWasSpace){
            lastCharacterWasSpace = false;
            sb.append("&nbsp;");
          }else {
            lastCharacterWasSpace=true;
            sb.append(" ");
          }
        }else {
          lastCharacterWasSpace = false;
          switch(c) {
          case '<': sb.append("&lt;"); break;
          case '>': sb.append("&gt;"); break;
          case '&': sb.append("&amp;"); break;
          case '"': sb.append("&quot;"); break;
          default : sb.append(c);break;
          }
        }
      }
      
      return sb.toString();
    }

  /**
   * a byte description of the given long interger value.
   *
   * @param len len.
   * @return a byte description of the given long interger value.
   */
  public static String byteDesc(long len) {
    return TraditionalBinaryPrefix.long2String(len, "B", 2);
  }

  /**
   * limitDecimalTo2.
   *
   * @param d double param.
   * @return string value ("%.2f").
   * @deprecated use StringUtils.format("%.2f", d).
   */
  @Deprecated
  public static String limitDecimalTo2(double d) {
    return format("%.2f", d);
  }
  
  /**
   * Concatenates strings, using a separator.
   *
   * @param separator Separator to join with.
   * @param strings Strings to join.
   * @return join string.
   */
  public static String join(CharSequence separator, Iterable<?> strings) {
    Iterator<?> i = strings.iterator();
    if (!i.hasNext()) {
      return "";
    }
    StringBuilder sb = new StringBuilder(i.next().toString());
    while (i.hasNext()) {
      sb.append(separator);
      sb.append(i.next().toString());
    }
    return sb.toString();
  }

  public static String join(char separator, Iterable<?> strings) {
    return join(separator + "", strings);
  }

  /**
   * Concatenates strings, using a separator.
   *
   * @param separator to join with
   * @param strings to join
   * @return  the joined string
   */
  public static String join(CharSequence separator, String[] strings) {
    // Ideally we don't have to duplicate the code here if array is iterable.
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : strings) {
      if (first) {
        first = false;
      } else {
        sb.append(separator);
      }
      sb.append(s);
    }
    return sb.toString();
  }

  public static String join(char separator, String[] strings) {
    return join(separator + "", strings);
  }

  /**
   * Convert SOME_STUFF to SomeStuff
   *
   * @param s input string
   * @return camelized string
   */
  public static String camelize(String s) {
    StringBuilder sb = new StringBuilder();
    String[] words = split(StringUtils.toLowerCase(s), ESCAPE_CHAR,  '_');

    for (String word : words)
      sb.append(org.apache.commons.lang3.StringUtils.capitalize(word));

    return sb.toString();
  }

  /**
   * Matches a template string against a pattern, replaces matched tokens with
   * the supplied replacements, and returns the result.  The regular expression
   * must use a capturing group.  The value of the first capturing group is used
   * to look up the replacement.  If no replacement is found for the token, then
   * it is replaced with the empty string.
   * 
   * For example, assume template is "%foo%_%bar%_%baz%", pattern is "%(.*?)%",
   * and replacements contains 2 entries, mapping "foo" to "zoo" and "baz" to
   * "zaz".  The result returned would be "zoo__zaz".
   * 
   * @param template String template to receive replacements
   * @param pattern Pattern to match for identifying tokens, must use a capturing
   *   group
   * @param replacements Map&lt;String, String&gt; mapping tokens identified by
   * the capturing group to their replacement values
   * @return String template with replacements
   */
  public static String replaceTokens(String template, Pattern pattern,
      Map<String, String> replacements) {
    StringBuffer sb = new StringBuffer();
    Matcher matcher = pattern.matcher(template);
    while (matcher.find()) {
      String replacement = replacements.get(matcher.group(1));
      if (replacement == null) {
        replacement = "";
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
  
  /**
   * Get stack trace for a given thread.
   * @param t thread.
   * @return stack trace string.
   */
  public static String getStackTrace(Thread t) {
    final StackTraceElement[] stackTrace = t.getStackTrace();
    StringBuilder str = new StringBuilder();
    for (StackTraceElement e : stackTrace) {
      str.append(e.toString() + "\n");
    }
    return str.toString();
  }

  /**
   * From a list of command-line arguments, remove both an option and the 
   * next argument.
   *
   * @param name  Name of the option to remove.  Example: -foo.
   * @param args  List of arguments.
   * @return      null if the option was not found; the value of the 
   *              option otherwise.
   * @throws IllegalArgumentException if the option's argument is not present
   */
  public static String popOptionWithArgument(String name, List<String> args)
      throws IllegalArgumentException {
    String val = null;
    for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
      String cur = iter.next();
      if (cur.equals("--")) {
        // stop parsing arguments when you see --
        break;
      } else if (cur.equals(name)) {
        iter.remove();
        if (!iter.hasNext()) {
          throw new IllegalArgumentException("option " + name + " requires 1 " +
              "argument.");
        }
        val = iter.next();
        iter.remove();
        break;
      }
    }
    return val;
  }
  
  /**
   * From a list of command-line arguments, remove an option.
   *
   * @param name  Name of the option to remove.  Example: -foo.
   * @param args  List of arguments.
   * @return      true if the option was found and removed; false otherwise.
   */
  public static boolean popOption(String name, List<String> args) {
    for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
      String cur = iter.next();
      if (cur.equals("--")) {
        // stop parsing arguments when you see --
        break;
      } else if (cur.equals(name)) {
        iter.remove();
        return true;
      }
    }
    return false;
  }
  
  /**
   * From a list of command-line arguments, return the first non-option
   * argument.  Non-option arguments are those which either come after 
   * a double dash (--) or do not start with a dash.
   *
   * @param args  List of arguments.
   * @return      The first non-option argument, or null if there were none.
   */
  public static String popFirstNonOption(List<String> args) {
    for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
      String cur = iter.next();
      if (cur.equals("--")) {
        if (!iter.hasNext()) {
          return null;
        }
        cur = iter.next();
        iter.remove();
        return cur;
      } else if (!cur.startsWith("-")) {
        iter.remove();
        return cur;
      }
    }
    return null;
  }

  /**
   * Converts all of the characters in this String to lower case with
   * Locale.ENGLISH.
   *
   * @param str  string to be converted
   * @return     the str, converted to lowercase.
   */
  public static String toLowerCase(String str) {
    return str.toLowerCase(Locale.ENGLISH);
  }

  /**
   * Converts all of the characters in this String to upper case with
   * Locale.ENGLISH.
   *
   * @param str  string to be converted
   * @return     the str, converted to uppercase.
   */
  public static String toUpperCase(String str) {
    return str.toUpperCase(Locale.ENGLISH);
  }

  /**
   * Compare strings locale-freely by using String#equalsIgnoreCase.
   *
   * @param s1  Non-null string to be converted
   * @param s2  string to be converted
   * @return     the str, converted to uppercase.
   */
  public static boolean equalsIgnoreCase(String s1, String s2) {
    Preconditions.checkNotNull(s1);
    // don't check non-null against s2 to make the semantics same as
    // s1.equals(s2)
    return s1.equalsIgnoreCase(s2);
  }

  /**
   * <p>Checks if the String contains only unicode letters.</p>
   *
   * <p><code>null</code> will return <code>false</code>.
   * An empty String (length()=0) will return <code>true</code>.</p>
   *
   * <pre>
   * StringUtils.isAlpha(null)   = false
   * StringUtils.isAlpha("")     = true
   * StringUtils.isAlpha("  ")   = false
   * StringUtils.isAlpha("abc")  = true
   * StringUtils.isAlpha("ab2c") = false
   * StringUtils.isAlpha("ab-c") = false
   * </pre>
   *
   * @param str  the String to check, may be null
   * @return <code>true</code> if only contains letters, and is non-null
   */
  public static boolean isAlpha(String str) {
    if (str == null) {
      return false;
    }
    int sz = str.length();
    for (int i = 0; i < sz; i++) {
      if (!Character.isLetter(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Same as WordUtils#wrap in commons-lang 2.6. Unlike commons-lang3, leading
   * spaces on the first line are NOT stripped.
   *
   * @param str  the String to be word wrapped, may be null
   * @param wrapLength  the column to wrap the words at, less than 1 is treated
   *                   as 1
   * @param newLineStr  the string to insert for a new line,
   *  <code>null</code> uses the system property line separator
   * @param wrapLongWords  true if long words (such as URLs) should be wrapped
   * @return a line with newlines inserted, <code>null</code> if null input
   */
  public static String wrap(String str, int wrapLength, String newLineStr,
      boolean wrapLongWords) {
    if(str == null) {
      return null;
    } else {
      if(newLineStr == null) {
        newLineStr = System.lineSeparator();
      }

      if(wrapLength < 1) {
        wrapLength = 1;
      }

      int inputLineLength = str.length();
      int offset = 0;
      StringBuffer wrappedLine = new StringBuffer(inputLineLength + 32);

      while(inputLineLength - offset > wrapLength) {
        if(str.charAt(offset) == 32) {
          ++offset;
        } else {
          int spaceToWrapAt = str.lastIndexOf(32, wrapLength + offset);
          if(spaceToWrapAt >= offset) {
            wrappedLine.append(str.substring(offset, spaceToWrapAt));
            wrappedLine.append(newLineStr);
            offset = spaceToWrapAt + 1;
          } else if(wrapLongWords) {
            wrappedLine.append(str.substring(offset, wrapLength + offset));
            wrappedLine.append(newLineStr);
            offset += wrapLength;
          } else {
            spaceToWrapAt = str.indexOf(32, wrapLength + offset);
            if(spaceToWrapAt >= 0) {
              wrappedLine.append(str.substring(offset, spaceToWrapAt));
              wrappedLine.append(newLineStr);
              offset = spaceToWrapAt + 1;
            } else {
              wrappedLine.append(str.substring(offset));
              offset = inputLineLength;
            }
          }
        }
      }

      wrappedLine.append(str.substring(offset));
      return wrappedLine.toString();
    }
  }
}
