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
package org.apache.batchee.cli.command;

import org.apache.batchee.cli.command.api.Command;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Command(name = "names", description = "list known batches")
public class Names extends JobOperatorCommand {

    private static final Pattern JOB_XML_PATTERN = Pattern.compile("META-INF/batch-jobs/(.*)\\.xml");

    @Override
    public void doRun() {
        final Set<String> names = operator().getJobNames();
        info("");
        info("names");
        info("-----");
        if (names != null) {
            for (final String name : names) {
                info(name);
            }
        }

        // try to find not yet started jobs
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final Enumeration<URL> resources;
        try {
            resources = loader.getResources("META-INF/batch-jobs");
        } catch (final IOException e) {
            return;
        }
        while (resources.hasMoreElements()) {
            final URL url = resources.nextElement();
            final File file = toFile(url);

            if (file != null) {
                if (file.isDirectory()) {
                    findInDirectory(file);
                } else {
                    findInJar(file);
                }
            }
        }
        info("-----");
    }

    private void findInJar(final File file) {
        final JarFile jar;
        try {
            jar = new JarFile(file);
        } catch (final IOException e) {
            info(String.format("could not load jobs in file %s", file.getAbsolutePath()));
            return;
        }
        final Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            final JarEntry entry = entries.nextElement();

            if (entry.isDirectory()) {
                continue;
            }

            final Matcher matcher = JOB_XML_PATTERN.matcher(entry.getName());
            if (matcher.matches()) {
                info(matcher.group(1));
            }
        }
    }

    private void findInDirectory(final File file) {
        final String[] batches = file.list(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.endsWith(".xml");
            }
        });
        if (batches != null) {
            for (final String batch : batches) {
                info(batch);
            }
        }
    }

    private static File toFile(final URL url) {
        final File file;
        final String externalForm = url.toExternalForm();
        if ("jar".equals(url.getProtocol())) {
            file = new File(externalForm.substring("jar:file:".length(), externalForm.lastIndexOf('!')));
        } else if ("file".equals(url.getProtocol())) {
            file = new File(externalForm.substring("file:".length()));
        } else {
            file = null;
        }
        return file;
    }
}
