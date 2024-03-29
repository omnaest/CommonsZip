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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.omnaest.utils.ByteArrayUtils;
import org.omnaest.utils.ByteArrayUtils.MultiByteArrayContainer;
import org.omnaest.utils.CollectorUtils;
import org.omnaest.utils.MapperUtils;
import org.omnaest.utils.PredicateUtils;
import org.omnaest.utils.element.cached.CachedElement;
import org.omnaest.utils.exception.RuntimeIOException;
import org.omnaest.utils.exception.handler.ExceptionHandler;
import org.omnaest.utils.stream.Streamable;

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
    private static final class ZipContentImpl implements ZipContent
    {
        private final CachedElement<byte[]> dataSupplierCache;

        private ZipContentImpl(CachedElement<byte[]> dataSupplierCache)
        {
            this.dataSupplierCache = dataSupplierCache;
        }

        @Override
        public ZipContent writeTo(File file)
        {
            try
            {
                FileUtils.writeByteArrayToFile(file, this.dataSupplierCache.get());
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
            return this;
        }

        @Override
        public InputStream toInputStream()
        {
            return new ByteArrayInputStream(this.dataSupplierCache.get());
        }

        @Override
        public Stream<ZipContentEntry> stream()
        {
            Map<String, byte[]> nameToData = new LinkedHashMap<>();

            byte[] zippedData = this.dataSupplierCache.get();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(zippedData); ZipInputStream zis = new ZipInputStream(bais))
            {
                ZipEntry zipEntry = zis.getNextEntry();
                while (zipEntry != null)
                {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
                    {
                        IOUtils.copy(zis, baos);
                        baos.flush();
                        nameToData.put(zipEntry.getName(), baos.toByteArray());
                    }

                    zis.closeEntry();
                    zipEntry = zis.getNextEntry();
                }
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }

            MultiByteArrayContainer<String> byteArrayContainer = ByteArrayUtils.toMultiByteArrayContainer(nameToData);

            return byteArrayContainer.stream()
                                     .map(entry ->
                                     {
                                         return new ZipContentEntry()
                                         {
                                             @Override
                                             public String getName()
                                             {
                                                 return entry.getKey();
                                             }

                                             @Override
                                             public String getAsString()
                                             {
                                                 return entry.get()
                                                             .toString();
                                             }

                                             @Override
                                             public InputStream getAsInputStream()
                                             {
                                                 return entry.get()
                                                             .toInputStream();
                                             }

                                             @Override
                                             public byte[] get()
                                             {
                                                 return entry.get()
                                                             .toByteArray();
                                             }
                                         };
                                     });
        }

        @Override
        public InputStream getEntry(String zipEntryName)
        {
            byte[] unzippedData = new byte[0];
            byte[] zippedData = this.dataSupplierCache.get();
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ByteArrayInputStream bais = new ByteArrayInputStream(zippedData);
                    ZipInputStream zis = new ZipInputStream(bais))
            {
                ZipEntry zipEntry = zis.getNextEntry();
                while (zipEntry != null)
                {
                    if (zipEntry.getName()
                                .equals(zipEntryName))
                    {
                        IOUtils.copy(zis, baos);
                        break;
                    }

                    zis.closeEntry();
                    zipEntry = zis.getNextEntry();
                }
                baos.flush();
                unzippedData = baos.toByteArray();
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }

            return new ByteArrayInputStream(unzippedData);
        }

        @Override
        public String getEntryAsString(String zipEntryName)
        {
            try
            {
                return IOUtils.toString(this.getEntry(zipEntryName), StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public ExtractedZipContent toContainer()
        {
            Map<String, ZipContentEntry> nameToContent = this.stream()
                                                             .collect(CollectorUtils.toLinkedHashMap(ZipContentEntry::getName, MapperUtils.identity()));
            return new ExtractedZipContent()
            {
                @Override
                public Stream<ZipContentEntry> stream()
                {
                    return nameToContent.values()
                                        .stream();
                }
            };
        }
    }

    public static interface Reader
    {
        public TARReader fromTarGzip(File file) throws FileNotFoundException;

        public TARReader fromTarGzip(InputStream inputStream);

        public TARReader fromTarGzip(byte[] data);

        public UncompressedContentReader fromUncompressedFile(File file);

        public UncompressedContentReader fromUncompressedInputStream(InputStream inputStream);

        public ZipContent fromZip(File file);

        public ZipContent fromZip(byte[] data);

        public ZipContent fromZip(InputStream inputStream);

        public GZIPReader fromGzip(InputStream inputStream) throws IOException;

        public GZIPReader fromGzip(byte[] data) throws IOException;

        public GZIPReader fromGzip(File file) throws IOException;

        public BZ2Reader fromBZ2(File file) throws FileNotFoundException;

        public BZ2Reader fromBZ2(InputStream inputStream);

        public Reader withExceptionHandler(ExceptionHandler exceptionHandler);

        public Reader withSilentlyIgnoreExceptionHandler();

        public Reader withExceptionHandler(BiConsumer<String, Exception> exceptionHandler);

    }

    public static interface UncompressedContentReader
    {

        public ZipContent toZip(String zipEntryName);

    }

    public static interface ZipContent extends Streamable<ZipContentEntry>
    {
        public ZipContent writeTo(File file);

        public InputStream getEntry(String zipEntryName);

        public String getEntryAsString(String zipEntryName);

        public InputStream toInputStream();

        public ExtractedZipContent toContainer();

    }

    public static interface ExtractedZipContent extends Streamable<ZipContentEntry>
    {

    }

    public static interface ZipContentEntry extends Supplier<byte[]>
    {
        public String getName();

        @Override
        public byte[] get();

        public String getAsString();

        public InputStream getAsInputStream();
    }

    public static interface GZIPReader
    {
        public InputStream asInputStream();

        public byte[] asByteArray();
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

            public InputStream getContentAsInputStream()
            {
                return new ByteArrayInputStream(this.content);
            }

            @Override
            public String toString()
            {
                return "TAREntry [name=" + name + "]";
            }

        }

        public Map<String, byte[]> toMap() throws IOException;

        public Stream<TAREntry> toStream() throws IOException;

        public Optional<TAREntry> first() throws IOException;
    }

    public static interface BZ2Reader
    {
        public InputStream asInputStream() throws FileNotFoundException, IOException;
    }

    public static Reader read()
    {
        return new Reader()
        {
            private ExceptionHandler exceptionHandler = ExceptionHandler.rethrowingExceptionHandler();

            @Override
            public Reader withExceptionHandler(ExceptionHandler exceptionHandler)
            {
                this.exceptionHandler = exceptionHandler;
                return this;
            }

            @Override
            public Reader withExceptionHandler(BiConsumer<String, Exception> exceptionHandler)
            {
                return this.withExceptionHandler(ExceptionHandler.fromBiConsumer(exceptionHandler));
            }

            @Override
            public Reader withSilentlyIgnoreExceptionHandler()
            {
                return this.withExceptionHandler(ExceptionHandler.noOperationExceptionHandler());
            }

            @Override
            public GZIPReader fromGzip(File file) throws IOException
            {
                return this.fromGzip(new BufferedInputStream(new FileInputStream(file)));
            }

            @Override
            public GZIPReader fromGzip(InputStream inputStream) throws IOException
            {
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream, 1024 * 1024);
                return new GZIPReader()
                {
                    private InputStream inputStream = gzipInputStream;

                    @Override
                    public InputStream asInputStream()
                    {
                        return this.inputStream;
                    }

                    @Override
                    public byte[] asByteArray()
                    {
                        try
                        {
                            byte[] data = IOUtils.toByteArray(gzipInputStream);
                            this.inputStream.close();
                            this.inputStream = new ByteArrayInputStream(data);
                            return data;
                        }
                        catch (IOException e)
                        {
                            throw new IllegalStateException(e);
                        }
                    }
                };
            }

            @Override
            public GZIPReader fromGzip(byte[] data) throws IOException
            {
                return this.fromGzip(new ByteArrayInputStream(data));
            }

            @Override
            public TARReader fromTarGzip(File file) throws FileNotFoundException
            {
                return this.fromTarGzip(new BufferedInputStream(new FileInputStream(file)));
            }

            @Override
            public TARReader fromTarGzip(byte[] data)
            {
                return this.fromTarGzip(new ByteArrayInputStream(data));
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
                                                exceptionHandler.accept(e);
                                                return null;
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

                    @Override
                    public Optional<TAREntry> first() throws IOException
                    {
                        return this.toStream()
                                   .findFirst();
                    }
                };
            }

            @Override
            public UncompressedContentReader fromUncompressedFile(File file)
            {
                try
                {
                    return this.fromUncompressedInputStream(new BufferedInputStream(new FileInputStream(file)));
                }
                catch (FileNotFoundException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }

            @Override
            public UncompressedContentReader fromUncompressedInputStream(InputStream inputStream)
            {
                return new UncompressedContentReader()
                {
                    @Override
                    public ZipContent toZip(String zipEntryName)
                    {
                        CachedElement<byte[]> dataSupplierCache = CachedElement.of(() ->
                        {
                            byte[] data = new byte[0];
                            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ZipOutputStream zipOutputStream = new ZipOutputStream(baos);)
                            {
                                zipOutputStream.putNextEntry(new ZipEntry(zipEntryName));
                                IOUtils.copy(inputStream, zipOutputStream);
                                zipOutputStream.close();
                                baos.flush();
                                data = baos.toByteArray();
                            }
                            catch (IOException e)
                            {
                                throw new IllegalStateException(e);
                            }
                            finally
                            {
                                try
                                {
                                    inputStream.close();
                                }
                                catch (IOException e)
                                {
                                    //
                                }
                            }
                            return data;
                        });
                        return new ZipContentImpl(dataSupplierCache);
                    }
                };
            }

            @Override
            public ZipContent fromZip(File file)
            {
                try
                {
                    return this.fromZip(FileUtils.readFileToByteArray(file));
                }
                catch (IOException e)
                {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public ZipContent fromZip(byte[] data)
            {
                return new ZipContentImpl(CachedElement.of(() -> data));
            }

            @Override
            public ZipContent fromZip(InputStream inputStream)
            {
                try
                {
                    return this.fromZip(IOUtils.toByteArray(inputStream));
                }
                catch (IOException e)
                {
                    throw new RuntimeIOException(e);
                }
            }

            @Override
            public BZ2Reader fromBZ2(InputStream inputStream)
            {
                return new BZ2Reader()
                {
                    @Override
                    public InputStream asInputStream() throws FileNotFoundException, IOException
                    {
                        try
                        {
                            return new org.apache.commons.compress.compressors.CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(inputStream));
                        }
                        catch (CompressorException e)
                        {
                            throw new IOException(e);
                        }
                    }
                };
            }

            @Override
            public BZ2Reader fromBZ2(File file) throws FileNotFoundException
            {
                return this.fromBZ2(new FileInputStream(file));
            }

        };
    }
}
