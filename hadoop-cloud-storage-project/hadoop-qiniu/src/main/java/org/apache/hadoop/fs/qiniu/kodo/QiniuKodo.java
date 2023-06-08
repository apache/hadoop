package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QiniuKodo extends DelegateToFileSystem {
    public QiniuKodo(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, new QiniuKodoFileSystem(), conf, Constants.KODO_SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return super.getUriDefaultPort();
    }
}
