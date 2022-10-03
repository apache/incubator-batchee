/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.cli.zip;

import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Zips {
    private static final int FILE_BUFFER_SIZE = 32768;
    private static final int COPY_BUFFER_SIZE = 1024;

    public static void unzip(final File zipFile, final File destination) throws IOException {
        ZipInputStream in = null;
        try {
            mkdir(destination);

            in = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipFile), FILE_BUFFER_SIZE));

            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                final String path = entry.getName();
                final File file = new File(destination, path);
                if (!file.toPath().normalize().startsWith(destination.toPath().normalize())) {
                    throw new IOException("Bad zip entry");
                }

                if (entry.isDirectory()) {
                    continue;
                }

                mkdir(file.getParentFile());

                final OutputStream out = new BufferedOutputStream(new FileOutputStream(file), FILE_BUFFER_SIZE);
                try {
                    copy(in, out);
                } finally {
                    IOUtils.closeQuietly(out);
                }

                final long lastModified = entry.getTime();
                if (lastModified > 0) {
                    file.setLastModified(lastModified);
                }

            }
        } catch (final IOException e) {
            throw new IOException("Unable to unzip " + zipFile.getAbsolutePath(), e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private static void copy(final ZipInputStream from, final OutputStream to) throws IOException {
        final byte[] buffer = new byte[COPY_BUFFER_SIZE];
        int length;
        while ((length = from.read(buffer)) != -1) {
            to.write(buffer, 0, length);
        }
        to.flush();
    }

    private static void mkdir(final File file) {
        if (!file.exists() && !file.mkdirs()) {
            throw new BatchContainerRuntimeException("Can't create " + file.getPath());
        }
    }

    private Zips() {
        // no-op
    }
}
