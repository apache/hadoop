package org.apache.hadoop.fs.contract.localfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestLocalFSConractVectoredRead extends AbstractContractVectoredReadTest {

  public TestLocalFSConractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LocalFSContract(conf);
  }
}
