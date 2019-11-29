package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

public class AbfsAuthorizerUnhandledException extends AzureBlobFileSystemException {
  public AbfsAuthorizerUnhandledException(Exception innerException) {
    super("An unhandled Authorizer request processing exception",
        innerException);
  }
}
