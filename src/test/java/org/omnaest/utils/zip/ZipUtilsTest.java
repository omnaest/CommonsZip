package org.omnaest.utils.zip;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

public class ZipUtilsTest
{

    @Test
    @Ignore
    public void testToStream() throws Exception
    {
        Map<String, byte[]> map = ZipUtils.read()
                                          .fromTarGzip(new File("D:\\databases\\pmc\\oa_package\\1f\\0d\\PMC3520946.tar.gz"))
                                          .toMap();

        System.out.println(map.keySet());

        map.entrySet()
           .stream()
           .filter(entry -> entry.getKey()
                                 .contains("pdf"))
           .findFirst()
           .ifPresent(entry ->
           {
               try
               {
                   FileUtils.writeByteArrayToFile(new File("C:/Temp/pmc3520946.pdf"), entry.getValue());
               }
               catch (IOException e)
               {
                   throw new IllegalStateException(e);
               }
           });
    }

}
