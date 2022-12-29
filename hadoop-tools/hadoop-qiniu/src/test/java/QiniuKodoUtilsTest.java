import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.apache.hadoop.fs.qiniu.kodo.QiniuKodoUtils.*;
import static org.junit.Assert.*;

public class QiniuKodoUtilsTest {


    @Test
    public void testIsKeyDir() {
        boolean is = isKeyDir("user/yangsen/");
        assertTrue(is);
    }

    @Test
    public void testPathToKey() {
        assertEquals("p1/c1", pathToKey(new Path(""), new Path("/p1/c1")));
    }
}
