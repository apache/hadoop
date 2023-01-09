package org.apache.hadoop.fs.qinu.kodo.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

public class QiniuKodoContractDistCpTest extends AbstractContractDistCpTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new QiniuKodoContract(conf);
  }

}
