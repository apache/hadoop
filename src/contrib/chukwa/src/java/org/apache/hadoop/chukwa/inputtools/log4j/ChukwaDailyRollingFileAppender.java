/*
 * Copyright (C) The Apache Software Foundation. All rights reserved.
 *
 * This software is published under the terms of the Apache Software
 * License version 1.1, a copy of which has been included with this
 * distribution in the LICENSE.txt file.  */



package org.apache.hadoop.chukwa.inputtools.log4j;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.util.RecordConstants;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
    ChukwaDailyRollingFileAppender is a slightly modified version of
    DailyRollingFileAppender, with modified versions of its
    <code>subAppend()</code> and <code>rollOver()</code> functions. 
    We would have preferred to sub-class DailyRollingFileAppender but
    its authors clearly did not intend that to be a viable option since
    they made too much of the class private or package-private

    DailyRollingFileAppender extends {@link FileAppender} so that the
    underlying file is rolled over at a user chosen frequency.

    <p>The rolling schedule is specified by the <b>DatePattern</b>
    option. This pattern should follow the {@link SimpleDateFormat}
    conventions. In particular, you <em>must</em> escape literal text
    within a pair of single quotes. A formatted version of the date
    pattern is used as the suffix for the rolled file name.

    <p>For example, if the <b>File</b> option is set to
    <code>/foo/bar.log</code> and the <b>DatePattern</b> set to
    <code>'.'yyyy-MM-dd</code>, on 2001-02-16 at midnight, the logging
    file <code>/foo/bar.log</code> will be copied to
    <code>/foo/bar.log.2001-02-16</code> and logging for 2001-02-17
    will continue in <code>/foo/bar.log</code> until it rolls over
    the next day.

    <p>Is is possible to specify monthly, weekly, half-daily, daily,
    hourly, or minutely rollover schedules.

    <p><table border="1" cellpadding="2">
    <tr>
    <th>DatePattern</th>
    <th>Rollover schedule</th>
    <th>Example</th>

    <tr>
    <td><code>'.'yyyy-MM</code>
    <td>Rollover at the beginning of each month</td>

    <td>At midnight of May 31st, 2002 <code>/foo/bar.log</code> will be
    copied to <code>/foo/bar.log.2002-05</code>. Logging for the month
    of June will be output to <code>/foo/bar.log</code> until it is
    also rolled over the next month.

    <tr>
    <td><code>'.'yyyy-ww</code>

    <td>Rollover at the first day of each week. The first day of the
    week depends on the locale.</td>

    <td>Assuming the first day of the week is Sunday, on Saturday
    midnight, June 9th 2002, the file <i>/foo/bar.log</i> will be
    copied to <i>/foo/bar.log.2002-23</i>.  Logging for the 24th week
    of 2002 will be output to <code>/foo/bar.log</code> until it is
    rolled over the next week.

    <tr>
    <td><code>'.'yyyy-MM-dd</code>

    <td>Rollover at midnight each day.</td>

    <td>At midnight, on March 8th, 2002, <code>/foo/bar.log</code> will
    be copied to <code>/foo/bar.log.2002-03-08</code>. Logging for the
    9th day of March will be output to <code>/foo/bar.log</code> until
    it is rolled over the next day.

    <tr>
    <td><code>'.'yyyy-MM-dd-a</code>

    <td>Rollover at midnight and midday of each day.</td>

    <td>At noon, on March 9th, 2002, <code>/foo/bar.log</code> will be
    copied to <code>/foo/bar.log.2002-03-09-AM</code>. Logging for the
    afternoon of the 9th will be output to <code>/foo/bar.log</code>
    until it is rolled over at midnight.

    <tr>
    <td><code>'.'yyyy-MM-dd-HH</code>

    <td>Rollover at the top of every hour.</td>

    <td>At approximately 11:00.000 o'clock on March 9th, 2002,
    <code>/foo/bar.log</code> will be copied to
    <code>/foo/bar.log.2002-03-09-10</code>. Logging for the 11th hour
    of the 9th of March will be output to <code>/foo/bar.log</code>
    until it is rolled over at the beginning of the next hour.


    <tr>
    <td><code>'.'yyyy-MM-dd-HH-mm</code>

    <td>Rollover at the beginning of every minute.</td>

    <td>At approximately 11:23,000, on March 9th, 2001,
    <code>/foo/bar.log</code> will be copied to
    <code>/foo/bar.log.2001-03-09-10-22</code>. Logging for the minute
    of 11:23 (9th of March) will be output to
    <code>/foo/bar.log</code> until it is rolled over the next minute.

    </table>

    <p>Do not use the colon ":" character in anywhere in the
    <b>DatePattern</b> option. The text before the colon is interpeted
    as the protocol specificaion of a URL which is probably not what
    you want. */
    
public class ChukwaDailyRollingFileAppender extends FileAppender {

	static Logger log = Logger.getLogger(ChukwaDailyRollingFileAppender.class);
  // The code assumes that the following constants are in a increasing
  // sequence.
  static final int TOP_OF_TROUBLE=-1;
  static final int TOP_OF_MINUTE = 0;
  static final int TOP_OF_HOUR   = 1;
  static final int HALF_DAY      = 2;
  static final int TOP_OF_DAY    = 3;
  static final int TOP_OF_WEEK   = 4;
  static final int TOP_OF_MONTH  = 5;

  static final String adaptorType = ChukwaAgentController.CharFileTailUTF8NewLineEscaped;

  static final Object lock = new Object();
  static String lastRotation = "";
  
  /**
    The date pattern. By default, the pattern is set to
    "'.'yyyy-MM-dd" meaning daily rollover.
   */
  private String datePattern = "'.'yyyy-MM-dd";

  /**
      The log file will be renamed to the value of the
      scheduledFilename variable when the next interval is entered. For
      example, if the rollover period is one hour, the log file will be
      renamed to the value of "scheduledFilename" at the beginning of
      the next hour. 

      The precise time when a rollover occurs depends on logging
      activity. 
   */
  private String scheduledFilename;

  /**
    The next time we estimate a rollover should occur. */
  private long nextCheck = System.currentTimeMillis () - 1;

  /**
   * Regex to select log files to be deleted
   */
  private String cleanUpRegex = null;
  
  /**
   * Set the maximum number of backup files to keep around.
   */
  private int maxBackupIndex = 10;
  
  Date now = new Date();

  SimpleDateFormat sdf;

  RollingCalendar rc = new RollingCalendar();

  int checkPeriod = TOP_OF_TROUBLE;

  ChukwaAgentController chukwaClient;
  boolean chukwaClientIsNull = true;
  static final Object chukwaLock = new Object();
  
  String chukwaClientHostname;
  int chukwaClientPortNum;
  long chukwaClientConnectNumRetry;
  long chukwaClientConnectRetryInterval;

  String recordType;
  
  
  
  
  // The gmtTimeZone is used only in computeCheckPeriod() method.
  static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");


  /**
    The default constructor does nothing. */
  public ChukwaDailyRollingFileAppender() throws IOException{
    super();
  }

  /**
     Instantiate a <code>DailyRollingFileAppender</code> and open the
     file designated by <code>filename</code>. The opened filename will
     become the output destination for this appender.

   */
  public ChukwaDailyRollingFileAppender (Layout layout, String filename,
      String datePattern) throws IOException {
    super(layout, filename, true);
    System.out.println("Daily Rolling File Appender successfully registered file with agent: " + filename);
    this.datePattern = datePattern;
    activateOptions();
  }

  /**
    The <b>DatePattern</b> takes a string in the same format as
    expected by {@link SimpleDateFormat}. This options determines the
    rollover schedule.
   */
  public void setDatePattern(String pattern) {
    datePattern = pattern;
  }

  /** Returns the value of the <b>DatePattern</b> option. */
  public String getDatePattern() {
    return datePattern;
  }
  
  public String getRecordType(){
    if (recordType != null)
      return recordType;
    else
      return "unknown";
  }
  
  public void setRecordType(String recordType){
    this.recordType = recordType;
  }

  public void activateOptions() {
    super.activateOptions();
    if(datePattern != null && fileName != null) {
      now.setTime(System.currentTimeMillis());
      sdf = new SimpleDateFormat(datePattern);
      int type = computeCheckPeriod();
      printPeriodicity(type);
      rc.setType(type);
      File file = new File(fileName);
      scheduledFilename = fileName+sdf.format(new Date(file.lastModified()));

    } else {
      LogLog.error("Either File or DatePattern options are not set for appender ["
          +name+"].");
    }
  }

  void printPeriodicity(int type) {
    switch(type) {
    case TOP_OF_MINUTE:
      LogLog.debug("Appender ["+name+"] to be rolled every minute.");
      break;
    case TOP_OF_HOUR:
      LogLog.debug("Appender ["+name
          +"] to be rolled on top of every hour.");
      break;
    case HALF_DAY:
      LogLog.debug("Appender ["+name
          +"] to be rolled at midday and midnight.");
      break;
    case TOP_OF_DAY:
      LogLog.debug("Appender ["+name
          +"] to be rolled at midnight.");
      break;
    case TOP_OF_WEEK:
      LogLog.debug("Appender ["+name
          +"] to be rolled at start of week.");
      break;
    case TOP_OF_MONTH:
      LogLog.debug("Appender ["+name
          +"] to be rolled at start of every month.");
      break;
    default:
      LogLog.warn("Unknown periodicity for appender ["+name+"].");
    }
  }


  // This method computes the roll over period by looping over the
  // periods, starting with the shortest, and stopping when the r0 is
  // different from from r1, where r0 is the epoch formatted according
  // the datePattern (supplied by the user) and r1 is the
  // epoch+nextMillis(i) formatted according to datePattern. All date
  // formatting is done in GMT and not local format because the test
  // logic is based on comparisons relative to 1970-01-01 00:00:00
  // GMT (the epoch).

  int computeCheckPeriod() {
    RollingCalendar rollingCalendar = new RollingCalendar(gmtTimeZone, Locale.ENGLISH);
    // set sate to 1970-01-01 00:00:00 GMT
    Date epoch = new Date(0);
    if(datePattern != null) {
      for(int i = TOP_OF_MINUTE; i <= TOP_OF_MONTH; i++) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(datePattern);
        simpleDateFormat.setTimeZone(gmtTimeZone); // do all date formatting in GMT
        String r0 = simpleDateFormat.format(epoch);
        rollingCalendar.setType(i);
        Date next = new Date(rollingCalendar.getNextCheckMillis(epoch));
        String r1 =  simpleDateFormat.format(next);
        //System.out.println("Type = "+i+", r0 = "+r0+", r1 = "+r1);
        if(r0 != null && r1 != null && !r0.equals(r1)) {
          return i;
        }
      }
    }
    return TOP_OF_TROUBLE; // Deliberately head for trouble...
  }

  /**
    Rollover the current file to a new file.
   */
  void rollOver() throws IOException {

    /* Compute filename, but only if datePattern is specified */
    if (datePattern == null) {
      errorHandler.error("Missing DatePattern option in rollOver().");
      return;
    }

    String datedFilename = fileName+sdf.format(now);
    // It is too early to roll over because we are still within the
    // bounds of the current interval. Rollover will occur once the
    // next interval is reached.
    if (scheduledFilename.equals(datedFilename)) {
      return;
    }

	
    // close current file, and rename it to datedFilename
    this.closeFile();


    File target  = new File(scheduledFilename);
    if (target.exists()) {
      target.delete();
    }

    File file = new File(fileName);
    
    boolean result = file.renameTo(target);
    if(result) {
      LogLog.debug(fileName +" -> "+ scheduledFilename);
    } else {
      LogLog.error("Failed to rename ["+fileName+"] to ["+scheduledFilename+"].");
    }

    try {
      // This will also close the file. This is OK since multiple
      // close operations are safe.
      this.setFile(fileName, false, this.bufferedIO, this.bufferSize);
    }
    catch(IOException e) {
      errorHandler.error("setFile("+fileName+", false) call failed.");
    }    
    scheduledFilename = datedFilename;
    cleanUp();
  }

  
  public String getCleanUpRegex() {
    return cleanUpRegex;
  }

  public void setCleanUpRegex(String cleanUpRegex) {
    this.cleanUpRegex = cleanUpRegex;
  }

  public int getMaxBackupIndex() {
    return maxBackupIndex;
  }

  public void setMaxBackupIndex(int maxBackupIndex) {
    this.maxBackupIndex = maxBackupIndex;
  }

  protected synchronized void cleanUp() {
    String regex = "";
    try {
      File actualFile = new File(fileName);
      
      String directoryName = actualFile.getParent();
      String actualFileName = actualFile.getName();
      File dirList = new File(directoryName);
      
      
      if (cleanUpRegex == null || !cleanUpRegex.contains("$fileName")) {
        LogLog.error("cleanUpRegex == null || !cleanUpRegex.contains(\"$fileName\")");
        return;
      }
      regex =cleanUpRegex.replace("$fileName", actualFileName);
      String[] dirFiles = dirList.list(new LogFilter(actualFileName,regex));
      
      List<String> files = new ArrayList<String>();
      for(String file: dirFiles) {
        files.add(file); 
      }
      Collections.sort(files);
      
      while(files.size() > maxBackupIndex) {
        String file = files.remove(0);
        File f = new File(directoryName + "/" +file);
        f.delete();
        LogLog.debug("Removing: " +file);
      }
    } catch(Exception e) {
      errorHandler.error("cleanUp("+fileName+"," + regex +") call failed.");
    }
  }
  
  private class LogFilter implements FilenameFilter {
    private Pattern p = null;
    private String logFile = null;
 
    public LogFilter(String logFile,String regex) {
      this.logFile = logFile;
      p = Pattern.compile(regex); 
    }
 
    @Override
    public boolean accept(File dir, String name) {
      // ignore current log file
      if (name.intern() == this.logFile.intern() ) {
        return false;
      }
      //ignore file without the same prefix
      if (!name.startsWith(logFile)) {
        return false;
      }
      return p.matcher(name).find();
    }
  }
  
  private class ClientFinalizer extends Thread 
  {
	  private ChukwaAgentController chukwaClient = null;
	  private String recordType = null;
	  private String fileName = null;
	  public ClientFinalizer(ChukwaAgentController chukwaClient,String recordType, String fileName)
	  {
		  this.chukwaClient = chukwaClient;
		  this.recordType = recordType;
		  this.fileName = fileName;
	  }
	    public synchronized void run() 
	    {
	      try 
	      {
	    	  if (chukwaClient != null)
	    	  {
	    		  log.debug("ShutdownHook: removing:" + fileName);
	    		  chukwaClient.removeFile(recordType, fileName);
	    	  }
	    	  else
	    	  {
	    		  LogLog.warn("chukwaClient is null cannot do any cleanup");
	    	  }
	      } 
	      catch (Throwable e) 
	      {
	    	  LogLog.warn("closing the controller threw an exception:\n" + e);
	      }
	    }
	  }
	  private ClientFinalizer clientFinalizer = null;
  
  /**
   * This method differentiates DailyRollingFileAppender from its
   * super class.
   *
   * <p>Before actually logging, this method will check whether it is
   * time to do a rollover. If it is, it will schedule the next
   * rollover time and then rollover.
   * */
  protected void subAppend(LoggingEvent event) 
  {
	  try
	  {
		  //we set up the chukwa adaptor here because this is the first
		  //point which is called after all setters have been called with
		  //their values from the log4j.properties file, in particular we
		  //needed to give setCukwaClientPortNum() and -Hostname() a shot
		  
		  // Make sure only one thread can do this
		  // and use the boolean to avoid the first level locking
		  if (chukwaClientIsNull)
		  {
			  synchronized(chukwaLock)
			  {
				  if (chukwaClient == null){
					  if (getChukwaClientHostname() != null && getChukwaClientPortNum() != 0){
						  chukwaClient = new ChukwaAgentController(getChukwaClientHostname(), getChukwaClientPortNum());
						  log.debug("setup adaptor with hostname " + getChukwaClientHostname() + " and portnum " + getChukwaClientPortNum());
					  }
					  else{
						  chukwaClient = new ChukwaAgentController();
						  log.debug("setup adaptor with no args, which means it used its defaults");
					  }

					  chukwaClientIsNull = false;
					  
					  //if they haven't specified, default to retrying every minute for 2 hours
					  long retryInterval = chukwaClientConnectRetryInterval;
					  if (retryInterval == 0)
						  retryInterval = 1000 * 60;
					  long numRetries = chukwaClientConnectNumRetry;
					  if (numRetries == 0)
						  numRetries = 120;
					  String log4jFileName = getFile();
					  String recordType = getRecordType();
					  long adaptorID = chukwaClient.addFile(recordType, log4jFileName, numRetries, retryInterval);

					  // Setup a shutdownHook for the controller
					  clientFinalizer = new ClientFinalizer(chukwaClient,recordType,log4jFileName);
					  Runtime.getRuntime().addShutdownHook(clientFinalizer);

					  
					  if (adaptorID > 0){
						  log.debug("Added file tailing adaptor to chukwa agent for file " + log4jFileName + "using this recordType :" + recordType);
					  }
					  else{
						  log.debug("Chukwa adaptor not added, addFile(" + log4jFileName + ") returned " + adaptorID);
					  }
					  
				  }				  
			  }
		  }
		  

		  long n = System.currentTimeMillis();
		  if (n >= nextCheck) {
			  now.setTime(n);
			  nextCheck = rc.getNextCheckMillis(now);
			  try {
				  rollOver();
			  }
			  catch(IOException ioe) {
				  LogLog.error("rollOver() failed.", ioe);
			  }
		  }
		  //escape the newlines from record bodies and then write this record to the log file
		  this.qw.write(RecordConstants.escapeAllButLastRecordSeparator("\n",this.layout.format(event)));

		  if(layout.ignoresThrowable()) {
			  String[] s = event.getThrowableStrRep();
			  if (s != null) {
				  int len = s.length;
				  for(int i = 0; i < len; i++) {
					  this.qw.write(s[i]);
					  this.qw.write(Layout.LINE_SEP);
				  }
			  }
		  }

		  if(this.immediateFlush) {
			  this.qw.flush();
		  }		  
	  }
	  catch(Throwable e)
	  {
		  System.err.println("Exception in ChukwaRollingAppender: " + e.getMessage());
		  e.printStackTrace();
	  }
    
  }

  public String getChukwaClientHostname() {
    return chukwaClientHostname;
  }

  public void setChukwaClientHostname(String chukwaClientHostname) {
    this.chukwaClientHostname = chukwaClientHostname;
  }

  public int getChukwaClientPortNum() {
    return chukwaClientPortNum;
  }

  public void setChukwaClientPortNum(int chukwaClientPortNum) {
    this.chukwaClientPortNum = chukwaClientPortNum;
  }

  public void setChukwaClientConnectNumRetry(int i){
    this.chukwaClientConnectNumRetry = i;
  }
  
  public void setChukwaClientConnectRetryInterval(long i) {
    this.chukwaClientConnectRetryInterval = i;
  }
  
}



/**
 *  RollingCalendar is a helper class to DailyRollingFileAppender.
 *  Given a periodicity type and the current time, it computes the
 *  start of the next interval.  
 * */
class RollingCalendar extends GregorianCalendar {

  /**
	 * 
	 */
	private static final long serialVersionUID = 2153481574198792767L;
int type = ChukwaDailyRollingFileAppender.TOP_OF_TROUBLE;

  RollingCalendar() {
    super();
  }  

  RollingCalendar(TimeZone tz, Locale locale) {
    super(tz, locale);
  }  

  void setType(int type) {
    this.type = type;
  }

  public long getNextCheckMillis(Date now) {
    return getNextCheckDate(now).getTime();
  }

  public Date getNextCheckDate(Date now) {
    this.setTime(now);

    switch(type) {
    case ChukwaDailyRollingFileAppender.TOP_OF_MINUTE:
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
      this.add(Calendar.MINUTE, 1);
      break;
    case ChukwaDailyRollingFileAppender.TOP_OF_HOUR:
      this.set(Calendar.MINUTE, 0);
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
      this.add(Calendar.HOUR_OF_DAY, 1);
      break;
    case ChukwaDailyRollingFileAppender.HALF_DAY:
      this.set(Calendar.MINUTE, 0);
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
      int hour = get(Calendar.HOUR_OF_DAY);
      if(hour < 12) {
        this.set(Calendar.HOUR_OF_DAY, 12);
      } else {
        this.set(Calendar.HOUR_OF_DAY, 0);
        this.add(Calendar.DAY_OF_MONTH, 1);
      }
      break;
    case ChukwaDailyRollingFileAppender.TOP_OF_DAY:
      this.set(Calendar.HOUR_OF_DAY, 0);
      this.set(Calendar.MINUTE, 0);
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
      this.add(Calendar.DATE, 1);
      break;
    case ChukwaDailyRollingFileAppender.TOP_OF_WEEK:
      this.set(Calendar.DAY_OF_WEEK, getFirstDayOfWeek());
      this.set(Calendar.HOUR_OF_DAY, 0);
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
      this.add(Calendar.WEEK_OF_YEAR, 1);
      break;
    case ChukwaDailyRollingFileAppender.TOP_OF_MONTH:
      this.set(Calendar.DATE, 1);
      this.set(Calendar.HOUR_OF_DAY, 0);
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
      this.add(Calendar.MONTH, 1);
      break;
    default:
      throw new IllegalStateException("Unknown periodicity type.");
    }
    return getTime();
  }
}

