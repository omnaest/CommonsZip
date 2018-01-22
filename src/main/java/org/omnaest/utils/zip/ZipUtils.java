package org.omnaest.utils.zip;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.omnaest.utils.PredicateUtils;

/**
 * Helper around gzip tar files
 * <br>
 * <br>
 * Example:
 * 
 * <pre>
 * Map<String, byte[]> map = ZipUtils.read()
 *                                   .fromTarGzip(new File("some file.tar.gz"))
 *                                   .toMap();
 * 
 * </pre>
 * 
 * @see #read()
 * @author omnaest
 */
public class ZipUtils
{
    public static interface Reader
    {
        public TARReader fromTarGzip(File file) throws FileNotFoundException;

        public TARReader fromTarGzip(InputStream inputStream);
    }

    public static interface TARReader
    {

        public static class TAREntry
        {
            private String name;
            private byte[] content;

            public TAREntry(String name, byte[] content)
            {
                super();
                this.name = name;
                this.content = content;
            }

            public String getName()
            {
                return name;
            }

            public byte[] getContent()
            {
                return content;
            }

            @Override
            public String toString()
            {
                return "TAREntry [name=" + name + "]";
            }

        }

        public Map<String, byte[]> toMap() throws IOException;

        public Stream<TAREntry> toStream() throws IOException;
    }

    public static Reader read()
    {
        return new Reader()
        {

            @Override
            public TARReader fromTarGzip(File file) throws FileNotFoundException
            {
                return this.fromTarGzip(new FileInputStream(file));
            }

            @Override
            public TARReader fromTarGzip(InputStream inputStream)
            {
                return new TARReader()
                {
                    @Override
                    public Stream<TAREntry> toStream() throws IOException
                    {
                        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GZIPInputStream(new BufferedInputStream(inputStream)));

                        int recordSize = tarArchiveInputStream.getRecordSize();

                        return IntStream.range(0, recordSize)
                                        .mapToObj(index ->
                                        {
                                            try
                                            {
                                                ArchiveEntry entry = tarArchiveInputStream.getNextEntry();
                                                if (entry != null)
                                                {
                                                    String name = entry.getName();
                                                    int size = (int) entry.getSize();

                                                    byte[] content = new byte[size];
                                                    org.apache.commons.io.IOUtils.read(tarArchiveInputStream, content);

                                                    return new TAREntry(name, content);
                                                }
                                                else
                                                {
                                                    return null;
                                                }
                                            }
                                            catch (IOException e)
                                            {
                                                throw new IllegalStateException(e);
                                            }
                                        })
                                        .filter(PredicateUtils.notNull())
                                        .onClose(() ->
                                        {
                                            try
                                            {
                                                tarArchiveInputStream.close();
                                                inputStream.close();
                                            }
                                            catch (IOException e)
                                            {
                                                //do nothing
                                            }
                                        });

                    }

                    @Override
                    public Map<String, byte[]> toMap() throws IOException
                    {
                        Map<String, byte[]> retmap = new LinkedHashMap<>();

                        this.toStream()
                            .forEach(entry ->
                            {
                                retmap.put(entry.getName(), entry.getContent());
                            });

                        return retmap;
                    }
                };
            }
        };
    }
}
