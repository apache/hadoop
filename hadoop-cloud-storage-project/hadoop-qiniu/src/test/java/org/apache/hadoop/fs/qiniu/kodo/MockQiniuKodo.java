package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MockQiniuKodo extends DelegateToFileSystem {

    public MockQiniuKodo(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, new QiniuKodoFileSystem(), conf, TestConstants.MOCKKODO_SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return super.getUriDefaultPort();
    }
}
