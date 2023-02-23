package org.apache.hadoop.fs.azurebfs.services;

public class RetryReasonConstants {
  public static final String CONNECTION_TIMEOUT_JDK_MESSAGE = "connect timed out";
  public static final String READ_TIMEOUT_JDK_MESSAGE = "Read timed out";
  public static final String CONNECTION_RESET_MESSAGE = "Connection reset";
  public static final String OPERATION_BREACH_MESSAGE = "Operations per second is over the account limit.";
  public static final String CONNECTION_RESET_ABBREVIATION = "CR";
  public static final String CONNECTION_TIMEOUT_ABBREVIATION = "CT";
  public static final String READ_TIMEOUT_ABBREVIATION = "RT";
  public static final String INGRESS_LIMIT_BREACH_ABBREVIATION = "ING";
  public static final String EGRESS_LIMIT_BREACH_ABBREVIATION = "EGR";
  public static final String OPERATION_LIMIT_BREACH_ABBREVIATION = "OPR";
  public static final String UNKNOWN_HOST_EXCEPTION_ABBREVIATION = "UH";
  public static final String IO_EXCEPTION_ABBREVIATION = "IOE";
  public static final String SOCKET_EXCEPTION_ABBREVIATION = "SE";


}
