package org.apache.hadoop.fs.qiniu.kodo.util;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

import static org.apache.hadoop.fs.qiniu.kodo.util.QiniuKodoUtils.*;
import static org.junit.Assert.*;

public class QiniuKodoUtilsTest {
    @Test
    public void testPathToKey() {
        assertEquals("p1/c1", pathToKey(new Path("qiniu://bucket/user/user1"), new Path("/p1/c1")));
        assertEquals("user/user1/p1/c1", pathToKey(new Path("qiniu://bucket/user/user1"), new Path("./p1/c1")));
        assertEquals("user/user1/p1/c1", pathToKey(new Path("qiniu://bucket/user/user1"), new Path("p1/c1")));
    }

    @Test
    public void testKeyToPath() {
        URI uri = URI.create("qiniu://bucket/user/user1");
        assertEquals(new Path("qiniu://bucket/p1/c1"), keyToPath(uri, new Path("qiniu://bucket/user/user1"), "p1/c1"));
        assertEquals(new Path("qiniu://bucket/p1/c1"), keyToPath(uri, new Path("/bucket/user/user1"), "p1/c1"));
    }

    @Test
    public void testKeyToFileKey() {
        assertNull(keyToFileKey(null));
        assertEquals("a", keyToFileKey("a"));
        assertEquals("a", keyToFileKey("a/"));
    }

    @Test
    public void testKeyToDirKey() {
        assertNull(keyToDirKey(null));
        assertEquals("", keyToDirKey(""));
        assertEquals("a/", keyToDirKey("a/"));
        assertEquals("a/", keyToDirKey("a"));
    }

    @Test
    public void testIsKeyDir() {
        assertFalse(isKeyDir(null));
        assertFalse(isKeyDir(""));
        assertFalse(isKeyDir("user/user1"));
        assertTrue(isKeyDir("user/user1/"));
    }
}
