import org.junit.Assert;
import org.junit.Test;

public class utils {


    @Test
    public void testKeyDir() {
        boolean is = isKeyDir("user/yangsen/");
        Assert.assertTrue(is);
    }


    Boolean isKeyDir(String key) {
        if (key == null || key.length() == 0) {
            return false;
        }

        return key.endsWith("/");
    }
}
