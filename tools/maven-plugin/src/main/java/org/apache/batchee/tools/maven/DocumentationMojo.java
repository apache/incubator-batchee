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

import org.apache.batchee.tools.maven.doc.ComponentDocumentationGenerator;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

@Mojo(name = "doc", defaultPhase = LifecyclePhase.PROCESS_CLASSES)
public class DocumentationMojo extends AbstractMojo {
    @Parameter(property = "batchee.classes", defaultValue = "${project.build.outputDirectory}")
    protected File classes;

    @Parameter(property = "batchee.output", defaultValue = "${project.build.directory}/generated-docs/${project.artifactId}-jbatch.adoc")
    protected File output;

    @Parameter(property = "batchee.formatter", defaultValue = "adoc")
    protected String formatter;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        new ComponentDocumentationGenerator(classes, output, formatter) {
            @Override
            protected void warn(final String s) {
                getLog().warn(s);
            }
        }.execute();
    }
}
