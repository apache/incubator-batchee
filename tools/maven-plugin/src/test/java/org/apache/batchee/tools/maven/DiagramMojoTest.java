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

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertTrue;

public class DiagramMojoTest {
    @Test
    public void generate() throws MojoFailureException, MojoExecutionException {
        final File target = new File("target/output/diagram.png");
        target.delete();

        final DiagramMojo mojo = new DiagramMojo();
        mojo.path = "src/test/resources/META-INF/batch-jobs/diagram.xml";
        mojo.failIfMissing = true;
        mojo.output = target.getParentFile();
        mojo.view = false;
        mojo.width = 640;
        mojo.height = 480;
        mojo.adjust = true;
        mojo.format = "png";
        mojo.layout = "spring170";
        mojo.execute();

        assertTrue(target.exists());
    }

    @Test(expectedExceptions = Exception.class)
    public void fail() throws MojoFailureException, MojoExecutionException {
        final DiagramMojo mojo = new DiagramMojo();
        mojo.path = "missing-batch.xml";
        mojo.failIfMissing = true;
        mojo.execute();
    }
}
