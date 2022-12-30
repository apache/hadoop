package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class QiniuKodo extends DelegateToFileSystem {

    public QiniuKodo(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, new QiniuKodoFileSystem(), conf, "qiniu", false);
    }

    @Override
    public int getUriDefaultPort() {
        // return Constants.S3A_DEFAULT_PORT;
        return super.getUriDefaultPort();
    }

    @Override
    public String toString() {
        return "Qiniu{" + "URI =" + fsImpl.getUri() +
                "; fsImpl=" + fsImpl +
                '}';
    }
}
