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

import org.apache.batchee.cli.classloader.ChildFirstURLClassLoader;
import org.apache.batchee.cli.command.api.Option;
import org.apache.batchee.cli.lifecycle.Lifecycle;
import org.apache.batchee.cli.zip.Zips;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.jaxrs.client.BatchEEJAXRSClientFactory;
import org.apache.batchee.jaxrs.client.ClientConfiguration;
import org.apache.batchee.jaxrs.client.ClientSecurity;
import org.apache.batchee.jaxrs.client.ClientSslConfiguration;
import org.apache.commons.io.FileUtils;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;

import static java.lang.Thread.currentThread;

/**
 * base class handling:
 * - classloader enriched with libs folders (and subfolders)
 * - Lifecycle (allow to start/stop a container)
 *
 * Note: the classloader is created from libs command, it is handy to organize batches
 *       by folders to be able to run them contextual using this command.
*/
public abstract class JobOperatorCommand implements Runnable {    // Remote config

    @Option(name = "url", description = "when using JAXRS the batchee resource url")
    protected String baseUrl = null;

    @Option(name = "json", description = "when using JAXRS the json provider")
    private String jsonProvider = null;

    @Option(name = "user", description = "when using JAXRS the username")
    private String username = null;

    @Option(name = "password", description = "when using JAXRS the password")
    private String password = null;

    @Option(name = "auth", description = "when using JAXRS the authentication type (Basic)")
    private String type = "Basic";

    @Option(name = "hostnameVerifier", description = "when using JAXRS the hostname verifier")
    private String hostnameVerifier = null;

    @Option(name = "keystorePassword", description = "when using JAXRS the keystorePassword")
    private String keystorePassword = null;

    @Option(name = "keystoreType", description = "when using JAXRS the keystoreType (JKS)")
    private String keystoreType = "JKS";

    @Option(name = "keystorePath", description = "when using JAXRS the keystorePath")
    private String keystorePath = null;

    @Option(name = "sslContextType", description = "when using JAXRS the sslContextType (TLS)")
    private String sslContextType = "TLS";

    @Option(name = "keyManagerType", description = "when using JAXRS the keyManagerType (SunX509)")
    private String keyManagerType = "SunX509";

    @Option(name = "keyManagerPath", description = "when using JAXRS the keyManagerPath")
    private String keyManagerPath = null;

    @Option(name = "trustManagerAlgorithm", description = "when using JAXRS the trustManagerAlgorithm")
    private String trustManagerAlgorithm = null;

    @Option(name = "trustManagerProvider", description = "when using JAXRS the trustManagerProvider")
    private String trustManagerProvider = null;

    // local config

    @Option(name = "lifecycle", description = "the lifecycle class to use")
    private String lifecycle = null;

    @Option(name = "libs", description = "folder containing additional libraries, the folder is added too to the loader")
    private String libs = null;

    @Option(name = "archive", description = "a bar archive")
    private String archive = null;

    @Option(name = "work", description = "work directory (default to java.io.tmp/work)")
    private String work = System.getProperty("batchee.home", System.getProperty("java.io.tmpdir")) + "/work";

    @Option(name = "sharedLibs", description = "folder containing shared libraries, the folder is added too to the loader")
    private String sharedLibs = null;

    @Option(name = "addFolderToLoader", description = "force shared lib and libs folders to be added to the classloader")
    private boolean addFolderToLoader = false;

    protected JobOperator operator;

    protected JobOperator operator() {
        if (operator != null) {
            return operator;
        }

        if (baseUrl == null) {
            return operator = BatchRuntime.getJobOperator();
        }

        final ClientConfiguration configuration = new ClientConfiguration();
        configuration.setBaseUrl(baseUrl);
        configuration.setJsonProvider(jsonProvider);
        if (hostnameVerifier != null || keystorePath != null || keyManagerPath != null) {
            final ClientSslConfiguration ssl = new ClientSslConfiguration();
            configuration.setSsl(ssl);
            ssl.setHostnameVerifier(hostnameVerifier);
            ssl.setKeystorePassword(keystorePassword);
            ssl.setKeyManagerPath(keyManagerPath);
            ssl.setKeyManagerType(keyManagerType);
            ssl.setKeystorePath(keystorePath);
            ssl.setKeystoreType(keystoreType);
            ssl.setSslContextType(sslContextType);
            ssl.setTrustManagerAlgorithm(trustManagerAlgorithm);
            ssl.setTrustManagerProvider(trustManagerProvider);
        }
        final ClientSecurity security = new ClientSecurity();
        configuration.setSecurity(security);
        security.setUsername(username);
        security.setPassword(password);
        security.setType(type);

        return operator = BatchEEJAXRSClientFactory.newClient(configuration);
    }

    protected void info(final String text) {
        System.out.println(text);
    }

    protected abstract void doRun();

    @Override
    public final void run() {
        System.setProperty("org.apache.batchee.init.verbose.sysout", "true");

        final ClassLoader oldLoader = currentThread().getContextClassLoader();
        final ClassLoader loader;
        try {
            loader = createLoader(oldLoader);
        } catch (final MalformedURLException e) {
            throw new BatchContainerRuntimeException(e);
        }

        if (loader != oldLoader) {
            currentThread().setContextClassLoader(loader);
        }

        try {
            final Lifecycle<Object> lifecycleInstance;
            final Object state;

            if (lifecycle != null) {
                lifecycleInstance = createLifecycle(loader);
                state = lifecycleInstance.start();

                registerShutdownHook(lifecycleInstance, state);
            } else {
                lifecycleInstance = null;
                state = null;
            }

            try {
                doRun();
            } finally {
                if (lifecycleInstance != null) {
                    lifecycleInstance.stop(state);
                }
            }
        } finally {
            currentThread().setContextClassLoader(oldLoader);
        }
    }

    private void registerShutdownHook(final Lifecycle<Object> lifecycleInstance, final Object state) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // as System.out as the Logger might already be resetted.
                // Additionally we want to give this message to the person hitting Ctrl-C
                // and not
                System.out.println("\n    Shutting down the JBatch engine started...\n");
                lifecycleInstance.stop(state);
             }
        });
    }

    private Lifecycle<Object> createLifecycle(final ClassLoader loader) {
        // some shortcuts are nicer to use from CLI
        if ("openejb".equalsIgnoreCase(lifecycle)) {
            lifecycle = "org.apache.batchee.cli.lifecycle.impl.OpenEJBLifecycle";
        } else if ("cdi".equalsIgnoreCase(lifecycle)) {
            lifecycle = "org.apache.batchee.cli.lifecycle.impl.CdiCtrlLifecycle";
        } else if ("spring".equalsIgnoreCase(lifecycle)) {
            lifecycle = "org.apache.batchee.cli.lifecycle.impl.SpringLifecycle";
        }

        try {
            return (Lifecycle<Object>) loader.loadClass(lifecycle).newInstance();
        } catch (final Exception e) {
            throw new BatchContainerRuntimeException(e);
        }
    }

    private ClassLoader createLoader(final ClassLoader parent) throws MalformedURLException {
        final Collection<URL> urls = new LinkedList<URL>();

        if (libs != null) {
            final File folder = new File(libs);
            if (folder.exists()) {
                addFolder(folder, urls);
            }
        }

        // we add libs/*.jar and libs/xxx/*.jar to be able to sort libs but only one level to keep it simple
        File resources = null;
        File exploded = null;
        if (archive != null) {
            final File bar = new File(archive);
            if (bar.exists()) {
                if (bar.isFile()) { // bar to unzip
                    exploded = new File(work, bar.getName());
                } else if (bar.isDirectory()) { // already unpacked
                    exploded = bar;
                } else {
                    throw new IllegalArgumentException("unsupported archive type for: '" + archive + "'");
                }

                final File timestamp = new File(exploded, "timestamp.txt");

                long ts = Long.MIN_VALUE;
                if (exploded.exists()) {
                    if (timestamp.exists()) {
                        try {
                            ts = Long.parseLong(FileUtils.readFileToString(timestamp).trim());
                        } catch (final IOException e) {
                            ts = Long.MIN_VALUE;
                        }
                    }
                }

                if (ts == Long.MIN_VALUE || ts < bar.lastModified()) {
                    explode(bar, exploded, timestamp, bar.lastModified());
                }

                if (archive.endsWith(".bar") || new File(exploded, "BATCH-INF").exists()) {
                    // bar archives are split accross 3 folders
                    addFolder(new File(exploded, "BATCH-INF/classes"), urls);
                    addFolderIfExist(new File(exploded, "BATCH-INF/lib"), urls);
                    resources = new File(exploded, "BATCH-INF");
                } else if (archive.endsWith(".war") || new File(exploded, "WEB-INF").exists()) {
                    addFolderIfExist(new File(exploded, "WEB-INF/classes"), urls);
                    addLibs(new File(exploded, "WEB-INF/lib"), urls);
                } else {
                    throw new IllegalArgumentException("unknown or unsupported archive type: " + archive);
                }
            } else {
                throw new IllegalArgumentException("'" + archive + "' doesn't exist");
            }
        }

        final ClassLoader sharedClassLoader = createSharedClassLoader(parent);
        if (libs == null && archive == null) {
            return sharedClassLoader;
        }

        final ChildFirstURLClassLoader classLoader = new ChildFirstURLClassLoader(urls.toArray(new URL[urls.size()]), sharedClassLoader);
        if (resources != null && resources.exists()) {
            classLoader.addResource(resources);
        }
        if (exploded != null) {
            classLoader.setApplicationFolder(exploded);
        }
        return classLoader;
    }

    private static void addFolderIfExist(final File file, final Collection<URL> urls) throws MalformedURLException {
        if (file.isDirectory()) {
            urls.add(file.toURI().toURL());
        }
    }

    private ClassLoader createSharedClassLoader(final ClassLoader parent) throws MalformedURLException {
        final ClassLoader usedParent;
        if (sharedLibs != null) { // add it later to let specific libs be taken before
            final Collection<URL> sharedUrls = new LinkedList<URL>();
            final File folder = new File(sharedLibs);
            addJars(folder, sharedUrls);
            if (ChildFirstURLClassLoader.class.isInstance(parent)) { // merge it
                ChildFirstURLClassLoader.class.cast(parent).addUrls(sharedUrls);
                usedParent = parent;
            } else {
                usedParent = new ChildFirstURLClassLoader(sharedUrls.toArray(new URL[sharedUrls.size()]), parent);
            }
        } else {
            usedParent = parent;
        }
        return usedParent;
    }

    private void addJars(final File folder, final Collection<URL> urls) throws MalformedURLException {
        if (!folder.isDirectory()) {
            return;
        }

        addLibs(folder, urls);
        if (addFolderToLoader) {
            urls.add(folder.toURI().toURL());
        }
    }

    private void addFolder(File folder, Collection<URL> urls) throws MalformedURLException {
        if (!folder.exists()) {
            return;
        }

        addJars(folder, urls);

        final File[] subFolders = folder.listFiles(DirFilter.INSTANCE);
        if (subFolders != null) {
            for (final File f : subFolders) {
                addJars(f, urls);
            }
        }
    }

    private static void addLibs(final File folder, final Collection<URL> urls) throws MalformedURLException {
        if (!folder.isDirectory()) {
            return;
        }

        final File[] additionals = folder.listFiles(JarFilter.INSTANCE);
        if (additionals != null) {
            for (final File toAdd : additionals) {
                urls.add(toAdd.toURI().toURL());
            }
        }
    }

    private static void explode(final File source, final File target, final File timestampFile, final long time) {
        try {
            FileUtils.deleteDirectory(target);
            Zips.unzip(source, target);
            FileUtils.write(timestampFile, Long.toString(time));
        } catch (final IOException e) {
            // no-op
        }
    }

    private static class JarFilter implements FilenameFilter {
        public static final FilenameFilter INSTANCE = new JarFilter();

        private JarFilter() {
            // no-op
        }

        @Override
        public boolean accept(final File dir, final String name) {
            return name.endsWith(".jar") || name.endsWith(".zip");
        }
    }

    private static class DirFilter implements FileFilter {
        public static final FileFilter INSTANCE = new DirFilter();

        private DirFilter() {
            // no-op
        }

        @Override
        public boolean accept(final File dir) {
            return dir.isDirectory() && !dir.getName().startsWith(".");
        }
    }
}
