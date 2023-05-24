package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MockQiniuKodo extends DelegateToFileSystem {

    public MockQiniuKodo(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, new QiniuKodoFileSystem(), conf, "mockKodo", false);
    }

    @Override
    public int getUriDefaultPort() {
        // return Constants.S3A_DEFAULT_PORT;
        return super.getUriDefaultPort();
    }
}
