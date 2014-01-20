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
package org.apache.batchee.cli.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;

// Note: don't depend from any other classes
public class ChildFirstURLClassLoader  extends URLClassLoader {
    private final ClassLoader system;
    private final Collection<File> resources = new CopyOnWriteArrayList<File>();
    private File applicationFolder;

    public ChildFirstURLClassLoader(final URL[] urls, final ClassLoader parent) {
        super(urls, parent);
        system = ClassLoader.getSystemClassLoader();
    }

    public void addResource(final File resource) {
        if (resource.isDirectory()) {
            resources.add(resource);
        }
    }

    @Override
    public URL findResource(final String name) {
        try {
            final Collection<URL> urls = findResourceUrls(name, name);
            if (urls != null) { // if not null -> not empty by design
                return urls.iterator().next();
            }
        } catch (final MalformedURLException e) {
            // no-op: use parent behavior
        }
        return super.findResource(name);
    }

    @Override
    public Enumeration<URL> findResources(final String name) throws IOException {
        final Enumeration<URL> defaultResources = super.findResources(name);
        if (name == null) {
            return defaultResources;
        }

        final Collection<URL> urls = findResourceUrls(name, name);
        if (urls != null) {
            urls.addAll(Collections.list(defaultResources));
            return Collections.enumeration(urls);
        }
        return defaultResources;
    }

    private Collection<URL> findResourceUrls(final String inName, final String nameWithoutSlash) throws MalformedURLException {
        final String name;
        if (inName.startsWith("/") && inName.length() > 1) {
            name = inName.substring(1);
        } else {
            name = inName;
        }

        Collection<URL> urls = null; // created lazily
        if ((name.startsWith("META-INF/batch-jobs/")
                || name.endsWith("batch.xml") || name.endsWith("batchee.xml"))
                && nameWithoutSlash.endsWith(".xml")) {
            for (final File folder : resources) {
                final File resource = new File(folder, nameWithoutSlash.replace("META-INF/", ""));
                if (resource.isFile()) {
                    if (urls == null) {
                        urls = new LinkedList<URL>();
                    }
                    urls.add(resource.toURI().toURL());
                }
            }
        }
        return urls;
    }

    @Override
    public Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        // already loaded?
        Class<?> clazz = findLoadedClass(name);
        if (clazz != null) {
            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        }

        /* needed if classloader hierarchy is batchee* <- cdi container <- app
        // lifecycle classes and cdi extensions need to be loaded from "container/lifecyle" loader
        if ((name.endsWith("Lifecycle") && name.startsWith("org.apache.batchee.cli.lifecycle.impl."))
                || name.startsWith("org.apache.batchee.container.cdi.")
                || name.startsWith("org.apache.batchee.container.services.factory.CDIBatchArtifactFactory")) {
            if (getParent() == system ) {
                final String path = name.replace('.', '/').concat(".class");
                try {
                    InputStream res = getResourceAsStream(path);
                    if (res != null && !BufferedInputStream.class.isInstance(res)) {
                        res = new BufferedInputStream(res);
                    }

                    if (res != null) {
                        final ByteArrayOutputStream bout = new ByteArrayOutputStream(6 * 1024);
                        try {
                            IOUtils.copy(res, bout);
                            final byte[] bytes = bout.toByteArray();
                            clazz = defineClass(name, bytes, 0, bytes.length);
                            if (resolve) {
                                resolveClass(clazz);
                            }
                            return clazz;
                        } catch (final IOException e) {
                            throw new ClassNotFoundException(name, e);
                        } finally {
                            IOUtils.closeQuietly(res);
                        }
                    }
                } catch (final ClassNotFoundException cnfe) {
                    // no-op
                }
            } else {
                return super.loadClass(name, resolve);
            }
        }
        */

        // JSE classes
        try {
            clazz = system.loadClass(name);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (final ClassNotFoundException ignored) {
            // no-op
        }

        clazz = loadInternal(name, resolve);
        if (clazz != null) {
            return clazz;
        }

        // finally delegate
        clazz = loadFromParent(name, resolve);
        if (clazz != null) {
            return clazz;
        }

        throw new ClassNotFoundException(name);
    }

    private Class<?> loadFromParent(final String name, final boolean resolve) {
        ClassLoader parent = getParent();
        if (parent == null) {
            parent = system;
        }
        try {
            final Class<?> clazz = Class.forName(name, false, parent);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (final ClassNotFoundException ignored) {
            // no-op
        }
        return null;
    }

    private Class<?> loadInternal(final String name, final boolean resolve) {
        try {
            final Class<?> clazz = findClass(name);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (final ClassNotFoundException ignored) {
            // no-op
        }
        return null;
    }

    public void setApplicationFolder(final File applicationFolder) {
        this.applicationFolder = applicationFolder;
    }

    public File getApplicationFolder() {
        return applicationFolder;
    }

    public void addUrls(final Collection<URL> urls) {
        for (final URL url : urls) {
            addURL(url);
        }
    }
}
