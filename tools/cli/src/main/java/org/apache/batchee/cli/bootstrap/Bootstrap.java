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
package org.apache.batchee.cli.bootstrap;

import org.apache.batchee.cli.classloader.ChildFirstURLClassLoader;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;

// Note: don't depend from any other classes excepted classloader
public class Bootstrap {
    public static void main(final String[] args) throws Exception {
        final String batcheeHome = System.getProperty("batchee.home");
        if (batcheeHome == null) {
            throw new IllegalArgumentException("batchee.home system property not set");
        }

        final File home = new File(batcheeHome);
        if (!home.isDirectory()) {
            throw new IllegalArgumentException("batchee.home doesn't exist '" + batcheeHome + "'");
        }

        final File libsDir = new File(batcheeHome, "lib");
        if (!libsDir.isDirectory()) {
            throw new IllegalArgumentException("lib directory doesn't exist '" + libsDir.getAbsolutePath() + "'");
        }

        // don't use a filter here to keep jar/assembly easy to do in pom.xml
        final File[] libs = libsDir.listFiles();
        if (libs == null) {
            throw new IllegalArgumentException("no lib found");
        }

        final Collection<URL> urls = new ArrayList<URL>(libs.length);
        for (final File f : libs) {
            if (f.getName().endsWith(".jar")) {
                urls.add(f.toURI().toURL());
            }
        }

        final URLClassLoader loader = new ChildFirstURLClassLoader(urls.toArray(new URL[urls.size()]), Thread.currentThread().getContextClassLoader());

        Thread.currentThread().setContextClassLoader(loader);
        final Class<?> cli = loader.loadClass("org.apache.batchee.cli.BatchEECLI");
        cli.getMethod("main", String[].class).invoke(null, new Object[] { args });
    }

    private Bootstrap() {
        // no-op
    }
}
