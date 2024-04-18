package org.apache.hadoop.fs.azurebfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractBulkDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class ITestAbfsContractBulkDelete extends AbstractContractBulkDeleteTest {

    private final boolean isSecure;
    private final ABFSContractTestBinding binding;

    public ITestAbfsContractBulkDelete() throws Exception {
        binding = new ABFSContractTestBinding();
        this.isSecure = binding.isSecureMode();
    }

    @Override
    public void setup() throws Exception {
        binding.setup();
        super.setup();
    }

    @Override
    protected Configuration createConfiguration() {
        return binding.getRawConfiguration();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
        return new AbfsFileSystemContract(conf, isSecure);
    }
}
