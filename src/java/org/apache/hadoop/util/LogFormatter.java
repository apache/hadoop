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

import java.util.logging.*;
import java.io.*;
import java.net.InetAddress;
import java.text.*;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;

/** @deprecated use {@link org.apache.commons.logging.LogFactory} instead. */
public class LogFormatter extends Formatter {
  private static final String FORMAT = "yyMMdd HHmmss";
  private static final String NEWLINE = System.getProperty("line.separator");

  private final Date date = new Date();
  private final SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);

  private static boolean loggedSevere= false;

  private static boolean showTime = true;
  private static boolean showThreadIDs = false;
  
  // install when this class is loaded
  static {
    Handler[] handlers = LogFormatter.getLogger("").getHandlers();
    for (int i = 0; i < handlers.length; i++) {
      handlers[i].setFormatter(new LogFormatter());
      handlers[i].setLevel(Level.FINEST);
    }
  }

  public static String initFileHandler( Configuration conf, String opName )
      throws IOException {
          String logDir=System.getProperty("hadoop.log.dir");
          String userHome=System.getProperty("user.dir");
          if( logDir==null ) {
        	  logDir=System.getProperty("hadoop.home.dir");
        	  if(logDir==null) {
        		  logDir=userHome;
        	  } else {
                  logDir+=File.separator+"logs";   
              }
          }
          
          if(!logDir.equals(userHome)) {
              File logDirFile = new File( logDir );
              if(!logDirFile.exists()) {
                  if(!logDirFile.mkdirs()) {
                      logDir=userHome;
                  }
              } else if( !logDirFile.isDirectory()) {
                  logDir=userHome;
              }
          }
          
          String hostname;
          try {
          	hostname=InetAddress.getLocalHost().getHostName();
          	int index=hostname.indexOf('.');
          	if( index != -1 ) {
          		hostname=hostname.substring(0, index);
          	}
          } catch (java.net.UnknownHostException e) {
          	hostname="localhost";
          }
          
          String id = System.getProperty( "hadoop.id.str", 
                                          System.getProperty("user.name") );
          String logFile = logDir+File.separator+"hadoop-"+id
               +"-"+opName+"-"+hostname+".log";

          int logFileSize = conf.getInt( "hadoop.logfile.size", 10000000 );
          int logFileCount = conf.getInt( "hadoop.logfile.count", 10 );
          
          FileHandler fh=new FileHandler(logFile, logFileSize, logFileCount, false);
          fh.setFormatter(new LogFormatter());
          fh.setLevel(Level.FINEST);
          
          Logger rootLogger = LogFormatter.getLogger("");
          rootLogger.info( "directing logs to directory "+logDir );
          
          Handler[] handlers = rootLogger.getHandlers();
          for( int i=0; i<handlers.length; i++ ) {
          	rootLogger.removeHandler( handlers[i]);
          }
          rootLogger.addHandler(fh);
          
          return logFile;
  }
      
  /** Gets a logger and, as a side effect, installs this as the default
   * formatter. */
  public static Logger getLogger(String name) {
    // just referencing this class installs it
    return Logger.getLogger(name);
  }
  
  /** When true, time is logged with each entry. */
  public static void showTime(boolean showTime) {
    LogFormatter.showTime = showTime;
  }

  /** When set true, thread IDs are logged. */
  public static void setShowThreadIDs(boolean showThreadIDs) {
    LogFormatter.showThreadIDs = showThreadIDs;
  }

  /**
   * Format the given LogRecord.
   * @param record the log record to be formatted.
   * @return a formatted log record
   */
  public synchronized String format(LogRecord record) {
    StringBuffer buffer = new StringBuffer();

    // the date
    if (showTime) {
      date.setTime(record.getMillis());
      formatter.format(date, buffer, new FieldPosition(0));
    }
    
    // the thread id
    if (showThreadIDs) {
      buffer.append(" 0x");
      String threadID = Integer.toHexString(record.getThreadID());
      for (int i = 0; i < 8 - threadID.length(); i++) 
        buffer.append('0');
      buffer.append(threadID);
    }

    // handle SEVERE specially
    if (record.getLevel() == Level.SEVERE) {
      buffer.append(" SEVERE");                   // flag it in log
      loggedSevere= true;                         // set global flag
    }

    // the message
    buffer.append(" ");
    buffer.append(formatMessage(record));

    buffer.append(NEWLINE);

    if (record.getThrown() != null) {
      try {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        pw.close();
        buffer.append(sw.toString());
      } catch (Exception ex) {
      }
    }
    return buffer.toString();
  }

  /**
   * Returns <code>true</code> if this <code>LogFormatter</code> has
   * logged something at <code>Level.SEVERE</code>
   */
  public static boolean hasLoggedSevere() {
    return loggedSevere;
  }

  /** Returns a stream that, when written to, adds log lines. */
  public static PrintStream getLogStream(final Logger logger,
                                         final Level level) {
    return new PrintStream(new ByteArrayOutputStream() {
        private int scan = 0;

        private boolean hasNewline() {
          for (; scan < count; scan++) {
            if (buf[scan] == '\n')
              return true;
          }
          return false;
        }

        public void flush() throws IOException {
          if (!hasNewline())
            return;
          logger.log(level, toString().trim());
          reset();
          scan = 0;
        }
      }, true);
  }
}
