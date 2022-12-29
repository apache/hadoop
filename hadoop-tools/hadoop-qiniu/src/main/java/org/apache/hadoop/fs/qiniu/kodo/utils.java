package org.apache.hadoop.fs.qiniu.kodo;

import com.qiniu.storage.Region;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

class utils {

    private static final Logger LOG = LoggerFactory.getLogger(utils.class);

    static final String PATH_SEPARATOR = "/";

    static String pathToKey(Path workingDir, Path path) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }
        return path.toUri().getPath().substring(1);
    }

    static Path keyToPath(URI uri, Path workingDir, String key) {
        return new Path(PATH_SEPARATOR + key).makeQualified(uri, workingDir);
    }

    static String keyToFileKey(String key) {
        if (key == null || !key.endsWith(PATH_SEPARATOR)) {
            return key;
        }
        return key.substring(0, key.length() - PATH_SEPARATOR.length());
    }

    static String keyToDirKey(String key) {
        if (key == null || key.length() == 0 || key.endsWith(PATH_SEPARATOR)) {
            return key;
        }
        return key + PATH_SEPARATOR;
    }

    static Boolean isKeyDir(String key) {
        if (key == null || key.length() == 0) {
            return false;
        }

        return key.endsWith(PATH_SEPARATOR);
    }
}
