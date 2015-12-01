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


import org.apache.batchee.tools.maven.doc.DiagramGenerator;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

@Mojo(name = "diagram")
public class DiagramMojo extends AbstractMojo {
    @Parameter(property = "batchee.path", required = true)
    protected String path;

    @Parameter(property = "batchee.failIfMissing", defaultValue = "false")
    protected boolean failIfMissing;

    @Parameter(property = "batchee.viewer", defaultValue = "false")
    protected boolean view;

    @Parameter(property = "batchee.width", defaultValue = "640")
    protected int width;

    @Parameter(property = "batchee.height", defaultValue = "480")
    protected int height;

    @Parameter(property = "batchee.adjust", defaultValue = "false")
    protected boolean adjust;

    @Parameter(property = "batchee.outputDir", defaultValue = "${project.build.directory}/batchee-diagram/")
    protected File output;

    @Parameter(property = "batchee.format", defaultValue = "png")
    protected String format;

    @Parameter(property = "batchee.outputFileName")
    protected String outputFileName;

    @Parameter(property = "batchee.rotateEdges", defaultValue = "true")
    protected boolean rotateEdges;

    // level, spring[distant],kk[unweight|dijstra],circle
    @Parameter(property = "batchee.layout", defaultValue = "level")
    protected String layout;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        new DiagramGenerator(path, failIfMissing, view, width, height, adjust, output, format, outputFileName, rotateEdges, layout) {
            @Override
            protected void warn(final String s) {
                getLog().warn(s);
            }

            @Override
            protected void info(String s) {
                getLog().info(s);
            }
        }.execute();
    }
}
