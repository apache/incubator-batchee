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
package org.apache.batchee.tools.maven;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;

import javax.batch.operations.JobOperator;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Start a job.
 */
@Mojo(name = "start", requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class StartMojo extends JobActionMojoBase {
    /**
     * the job name of the job to start
     */
    @Parameter(required = true, property = "batchee.job")
    protected String jobName;

    /**
     * used to find project binaries if running the batch locally
     */
    @Parameter(defaultValue = "${project.build.outputDirectory}", required = true, readonly = true )
    protected File projectBinaries;

    /**
     * used to find project dependencies if running the batch locally
     */
    @Parameter(defaultValue = "${project.artifacts}", readonly = true, required = true)
    protected Set<Artifact> dependencies;

    /**
     * if the project (binaries + dependencies) should be added during the execution to the classpath
     */
    @Parameter(property = "batchee.use-project", defaultValue = "true")
    protected boolean useProjectClasspath;

    /**
     * manual entries added in the execution classpath
     */
    @Parameter
    protected List<String> additionalClasspathEntries;

    @Override
    public void execute() throws MojoExecutionException {
        final JobOperator jobOperator = getOrCreateOperator();

        final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
        final ClassLoader loader = createStartLoader(oldLoader);
        Thread.currentThread().setContextClassLoader(loader);

        final long id;
        try {
            id = jobOperator.start(jobName, toProperties(jobParameters));
        } finally {
            Thread.currentThread().setContextClassLoader(oldLoader);
        }

        getLog().info("Started job " + jobName + ", id is #" + id);

        if (wait) {
            waitEnd(jobOperator, id);
        }
    }

    private ClassLoader createStartLoader(final ClassLoader parent) throws MojoExecutionException {
        final Collection<URL> urls = new LinkedList<URL>();
        if (useProjectClasspath) {
            if (projectBinaries != null && projectBinaries.exists()) {
                try {
                    urls.add(projectBinaries.toURI().toURL());
                } catch (final MalformedURLException e) {
                    throw new MojoExecutionException(e.getMessage(), e);
                }
            }
            if (dependencies != null) {
                for (final Artifact dependency : dependencies) {
                    try {
                        urls.add(dependency.getFile().toURI().toURL());
                    } catch (final MalformedURLException e) {
                        throw new MojoExecutionException(e.getMessage(), e);
                    }
                }
            }
        }
        if (additionalClasspathEntries != null) {
            for (final String entry : additionalClasspathEntries) {
                try {
                    final File file = new File(entry);
                    if (file.exists()) {
                        urls.add(file.toURI().toURL());
                    }
                } catch (final MalformedURLException e) {
                    throw new MojoExecutionException(e.getMessage(), e);
                }
            }
        }
        return new URLClassLoader(urls.toArray(new URL[urls.size()]), parent);
    }
}
