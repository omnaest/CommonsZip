/*******************************************************************************
 * Copyright 2021 Danny Kunz
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.omnaest.utils.zip;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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

    @Test
    //    @Ignore
    public void testToZip() throws Exception
    {
        File file = org.omnaest.utils.FileUtils.createRandomTempFile();
        ZipUtils.read()
                .fromUncompressedInputStream(IOUtils.toInputStream("test", StandardCharsets.UTF_8))
                .toZip("test.txt")
                .writeTo(file);

        String content = ZipUtils.read()
                                 .fromZip(file)
                                 .getEntryAsString("test.txt");
        assertEquals("test", content);

    }

}
