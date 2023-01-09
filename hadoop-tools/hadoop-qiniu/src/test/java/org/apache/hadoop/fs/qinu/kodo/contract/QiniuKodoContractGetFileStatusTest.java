package org.apache.hadoop.fs.qinu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class QiniuKodoContractGetFileStatusTest extends AbstractContractGetFileStatusTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new QiniuKodoContract(conf);
  }
}
