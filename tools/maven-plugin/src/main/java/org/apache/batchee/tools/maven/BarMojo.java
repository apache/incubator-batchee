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
import org.codehaus.plexus.archiver.Archiver;
import org.codehaus.plexus.archiver.jar.JarArchiver;
import org.codehaus.plexus.archiver.util.DefaultFileSet;
import org.codehaus.plexus.components.io.fileselectors.FileInfo;
import org.codehaus.plexus.components.io.fileselectors.FileSelector;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * create a bar archive using packaging bar and this plugin:
 * <packaging>bar</packaging>
 * <p/>
 * <plugin>
 * <groupId>org.apache.batchee</groupId>
 * <artifactId>batchee-maven-plugin</artifactId>
 * <version>0.1-incubating-SNAPSHOT</version>
 * <extensions>true</extensions>
 * </plugin>
 */
@Mojo(name = "bar", requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME, defaultPhase = LifecyclePhase.PACKAGE)
public class BarMojo extends AbstractMojo {
    private static final String JOB_XML_PATH = "META-INF/batch-jobs/";

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

        final MavenArchiver archiver = new MavenArchiver();
        archiver.setOutputFile(target);
        archiver.setArchiver(jarArchiver);

        // lib/*
        try {
            for (final Artifact dependency : resolver.resolve(project, asList("compile", "runtime", "system"), session)) {
                if ("provided".equals(dependency.getScope())) { // not sure why compile triggers provided using resolver
                    continue;
                }
                jarArchiver.addFile(dependency.getFile(), "libs/" + artifactPath(dependency));
            }
        } catch (final Exception e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        // classes
        if (classes.isDirectory()) {
            final FileSelector jobSelector = new FileSelector() {
                @Override
                public boolean isSelected(final FileInfo fileInfo) throws IOException {
                    final String name = fileInfo.getName();
                    return name.replace('\\', '/').startsWith(JOB_XML_PATH) && name.endsWith(".xml");
                }
            };

            final DefaultFileSet fileSet = new DefaultFileSet();
            fileSet.setDirectory(classes);
            fileSet.setIncludingEmptyDirectories(false);

            fileSet.setPrefix("batch/classes/");
            fileSet.setFileSelectors(new FileSelector[]{
                    new FileSelector() {
                        @Override
                        public boolean isSelected(final FileInfo fileInfo) throws IOException {
                            return !jobSelector.isSelected(fileInfo);
                        }
                    }
            });
            jarArchiver.addFileSet(fileSet);

            fileSet.setPrefix("batch/jobs/");
            fileSet.setFileSelectors(new FileSelector[]{ jobSelector });
            jarArchiver.addFileSet(fileSet);
        }

        try {
            archiver.createArchive(session, project, new MavenArchiveConfiguration());
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
}
