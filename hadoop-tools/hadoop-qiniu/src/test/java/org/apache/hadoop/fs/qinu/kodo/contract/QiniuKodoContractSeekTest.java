package org.apache.hadoop.fs.qinu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class QiniuKodoContractSeekTest extends AbstractContractSeekTest {
  @Override
  protected AbstractFSContract createContract(Configuration configuration) {
    return new QiniuKodoContract(configuration);
  }
}
