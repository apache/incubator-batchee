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

import org.apache.maven.ProjectDependenciesResolver;
import org.apache.maven.archiver.MavenArchiveConfiguration;
import org.apache.maven.archiver.MavenArchiver;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.codehaus.plexus.archiver.ArchiveEntry;
import org.codehaus.plexus.archiver.Archiver;
import org.codehaus.plexus.archiver.ArchiverException;
import org.codehaus.plexus.archiver.ResourceIterator;
import org.codehaus.plexus.archiver.jar.JarArchiver;
import org.codehaus.plexus.archiver.util.DefaultFileSet;
import org.codehaus.plexus.components.io.fileselectors.FileInfo;
import org.codehaus.plexus.components.io.fileselectors.FileSelector;
import org.codehaus.plexus.components.io.resources.PlexusIoFileResource;
import org.codehaus.plexus.components.io.resources.PlexusIoResource;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * create a bar archive using packaging bar and this plugin:
 * &lt;packaging&gt;bar&lt;/packaging&gt;
 * <p>
 * <pre><code>
 * &lt;plugin&gt;
 *   &lt;groupId&gt;org.apache.batchee&lt;/groupId&gt;
 *   &lt;artifactId&gt;batchee-maven-plugin&lt;/artifactId&gt;
 *   &lt;version&gt;0.1-incubating-SNAPSHOT&lt;/version&gt;
 *   &lt;extensions&gt;true&lt;/extensions&gt;
 * &lt;/plugin&gt;
 * </code></pre>
 */
@Mojo(name = "bar", requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME, defaultPhase = LifecyclePhase.PACKAGE)
public class BarMojo extends AbstractMojo {
    private static final BatchDescriptorSelector JOB_SELECTOR = new BatchDescriptorSelector();

    @Component(role = Archiver.class, hint = "jar")
    private JarArchiver jarArchiver;

    @Component
    private MavenProjectHelper helper;

    @Parameter(readonly = true, defaultValue = "${project}")
    private MavenProject project;

    @Parameter(defaultValue = "${localRepository}")
    protected ArtifactRepository localRepo;

    @Parameter(defaultValue = "${project.remoteArtifactRepositories}")
    protected List<ArtifactRepository> remoteRepos;

    @Component(role = ProjectDependenciesResolver.class)
    protected ProjectDependenciesResolver resolver;

    @Parameter(defaultValue = "${session}", required = true)
    protected MavenSession session;

    @Parameter(property = "batchee.finalName", defaultValue = "${project.build.finalName}", required = true)
    private String finalName;

    @Parameter(defaultValue = "${project.build.directory}", required = true)
    private File outputDirectory;

    @Parameter(property = "batchee.classifier")
    private String classifier;

    @Parameter(property = "batchee.classes", defaultValue = "${project.build.directory}/classes")
    private File classes;

    @Parameter(property = "batchee.maven-descriptors", defaultValue = "true")
    private boolean mavenDescriptors;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (classifier == null && !"bar".equals(project.getPackaging())) {
            getLog().info("Packaging type is not 'bar' so ignoring bar creation");
            return;
        }

        final File bar = createBar();

        if (classifier != null) {
            helper.attachArtifact(project, "bar", classifier, bar);
        } else {
            project.getArtifact().setFile(bar);
        }
    }

    private File createBar() throws MojoExecutionException {
        final File target = new File(outputDirectory, finalName + (classifier != null ? classifier : "") + ".bar");

        // lib/*
        try {
            for (final Artifact dependency : resolver.resolve(project, asList("compile", "runtime", "system"), session)) {
                if ("provided".equals(dependency.getScope())) { // not sure why compile triggers provided using resolver
                    continue;
                }
                jarArchiver.addFile(dependency.getFile(), "BATCH-INF/lib/" + artifactPath(dependency));
            }
        } catch (final Exception e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        // classes
        if (classes.isDirectory()) {
            final FileSelector classesSelector = new FileSelector() {
                @Override
                public boolean isSelected(final FileInfo fileInfo) throws IOException {
                    return !JOB_SELECTOR.isSelected(fileInfo);
                }
            };

            final DefaultFileSet fileSet = new DefaultFileSet();
            fileSet.setDirectory(classes);
            fileSet.setIncludingEmptyDirectories(false);

            fileSet.setPrefix("BATCH-INF/classes/");
            fileSet.setFileSelectors(new FileSelector[]{classesSelector});
            jarArchiver.addFileSet(fileSet);

            fileSet.setPrefix("BATCH-INF/");
            fileSet.setFileSelectors(new FileSelector[]{JOB_SELECTOR});
            jarArchiver.addFileSet(fileSet);
        }

        final MavenArchiveConfiguration archiveConfiguration = new MavenArchiveConfiguration();
        archiveConfiguration.setAddMavenDescriptor(mavenDescriptors);

        final MavenArchiver archiver = new MavenArchiver();
        archiver.setOutputFile(target);
        archiver.setArchiver(new JarArchiver() {
            {
                archiveType = "bar";
            }

            @Override
            public void addFile( final File inputFile, final String destFileName ) throws ArchiverException {
                jarArchiver.addFile(inputFile, destFileName);
            }

            @Override
            public ResourceIterator getResources() throws ArchiverException {
                final Iterator<ResourceIterator> iterators = asList(jarArchiver.getResources(), super.getResources()).iterator();
                return new ResourceIterator() {
                    private ResourceIterator ri = iterators.next();

                    @Override
                    public boolean hasNext() {
                        if (ri.hasNext()) {
                            return true;
                        } else if (iterators.hasNext()) {
                            ri = iterators.next();
                        }
                        return ri.hasNext();
                    }

                    @Override
                    public ArchiveEntry next() {
                        final ArchiveEntry next = ri.next();
                        final PlexusIoResource resource = next.getResource();
                        if (resource != null && PlexusIoFileResource.class.isInstance(resource)) {
                            final File file = PlexusIoFileResource.class.cast(resource).getFile();
                            final String name = next.getName();

                            if (JOB_SELECTOR.accept(resource.getName())) {
                                return ArchiveEntry.createEntry(name.replace(File.separatorChar, '/').replaceFirst("META\\-INF/", ""), file, next.getType(), next.getMode());
                            }
                        }
                        return next;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        });

        try {
            archiver.createArchive(session, project, archiveConfiguration);
        } catch (final Exception e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        return target;
    }

    // artifactId-[classifier-]version.type
    private static String artifactPath(final Artifact artifact) {
        final StringBuilder sb = new StringBuilder();

        sb.append(artifact.getArtifactId());
        sb.append("-");

        if (artifact.hasClassifier()) {
            sb.append(artifact.getClassifier());
            sb.append("-");
        }

        if (artifact.getBaseVersion() != null) {
            sb.append(artifact.getBaseVersion());
        } else if (artifact.getVersion() != null) {
            sb.append(artifact.getVersion());
        } else {
            sb.append(artifact.getVersionRange().toString());
        }

        sb.append('.').append(artifact.getType());
        return sb.toString();
    }

    private static class BatchDescriptorSelector implements FileSelector {
        private static final String JOB_XML_PATH = "META-INF/batch-jobs/";

        @Override
        public boolean isSelected(final FileInfo fileInfo) throws IOException {
            final String name = fileInfo.getName();
            return accept(name);
        }

        public boolean accept(final String name) {
            final String nameWithoutSlash;
            if (name.startsWith("/")) {
                nameWithoutSlash = name.substring(1);
            } else {
                nameWithoutSlash = name;
            }
            return (nameWithoutSlash.replace('\\', '/').startsWith(JOB_XML_PATH) && nameWithoutSlash.endsWith(".xml"))
                    || "META-INF/batch.xml".equals(name)
                    || "META-INF/batchee.xml".equals(name);
        }
    }
}
