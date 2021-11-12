package org.apache.hadoop.fs.contract.rawlocal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestRawLocalContractVectoredRead extends AbstractContractVectoredReadTest {

  public TestRawLocalContractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new RawlocalFSContract(conf);
  }
}
