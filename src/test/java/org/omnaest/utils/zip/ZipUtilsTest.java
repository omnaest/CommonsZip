package org.omnaest.utils.zip;

import java.io.File;
import java.util.Map;

import org.junit.Test;

public class ZipUtilsTest
{

    @Test
    public void testToStream() throws Exception
    {
        Map<String, byte[]> map = ZipUtils.read()
                                          .fromTAR(new File("D:\\databases\\pmc\\oa_package\\1f\\0d\\PMC3520946.tar.gz"))
                                          .toMap();

        System.out.println(map.keySet());
    }

}
