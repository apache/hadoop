package org.apache.hadoop.fs.contract.sftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestSFTPContractSeek extends AbstractContractSeekTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new SFTPContract(conf);
  }
}
