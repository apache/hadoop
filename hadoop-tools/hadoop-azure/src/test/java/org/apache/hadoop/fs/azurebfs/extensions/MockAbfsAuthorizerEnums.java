package org.apache.hadoop.fs.azurebfs.extensions;

public class MockAbfsAuthorizerEnums {
  public enum accessPermissions {
    Read, Write, Execute, ReadWrite, ReadExecute, ReadWriteExecute,
    SuperUserOrOwner, None
  }

  public enum WriteReadMode {
    WRITE_MODE, READ_MODE
  }
}
