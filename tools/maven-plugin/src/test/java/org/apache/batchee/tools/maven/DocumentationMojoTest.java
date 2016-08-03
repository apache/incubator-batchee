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

import org.apache.batchee.doc.api.Documentation;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.codehaus.plexus.util.IOUtil;
import org.junit.Test;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DocumentationMojoTest {
    @Test
    public void documentAdoc() throws MojoFailureException, MojoExecutionException, IOException {
        final File out = new File("target/DocumentationMojoTest/output.adoc");
        new DocumentationMojo() {{
            classes = new File("target/test-classes/org/apache/batchee/tools/maven/");
            output = out;
        }}.execute();
        final FileInputStream fis = new FileInputStream(out);
        assertEquals(
            "= myChildComponent\n" +
            "\n" +
            "a child comp\n" +
            "\n" +
            "|===\n" +
            "|Name|Description\n" +
            "|config2|2\n" +
            "|configByDefault|this is an important config\n" +
            "|expl|this one is less important\n" +
            "|===\n" +
            "\n" +
            "= myComponent\n" +
            "\n" +
            "|===\n" +
            "|Name|Description\n" +
            "|configByDefault|this is an important config\n" +
            "|expl|this one is less important\n" +
            "|===\n" +
            "\n" +
            "= org.apache.batchee.tools.maven.batchlet.SimpleBatchlet\n" +
            "\n" +
            "|===\n" +
            "|Name|Description\n" +
            "|fail|-\n" +
            "|sleep|-\n" +
            "|===\n" +
            "\n", IOUtil.toString(fis));
        fis.close();
    }

    @Test
    public void documentMd() throws MojoFailureException, MojoExecutionException, IOException {
        final File out = new File("target/DocumentationMojoTest/output.adoc");
        new DocumentationMojo() {{
            classes = new File("target/test-classes/org/apache/batchee/tools/maven/");
            output = out;
            formatter = "md";
        }}.execute();
        final FileInputStream fis = new FileInputStream(out);
        assertEquals(
            "# myChildComponent\n" +
            "\n" +
            "a child comp\n" +
            "\n" +
            "* `config2`: 2\n" +
            "* `configByDefault`: this is an important config\n" +
            "* `expl`: this one is less important\n" +
            "\n" +
            "# myComponent\n" +
            "\n" +
            "* `configByDefault`: this is an important config\n" +
            "* `expl`: this one is less important\n" +
            "\n" +
            "# org.apache.batchee.tools.maven.batchlet.SimpleBatchlet\n" +
            "\n" +
            "* `fail`\n" +
            "* `sleep`\n" +
            "\n", IOUtil.toString(fis));
        fis.close();
    }

    @Named
    public static class MyComponent {
        @Inject
        @BatchProperty
        @Documentation("this is an important config")
        private String configByDefault;

        @Inject
        @BatchProperty(name = "expl")
        @Documentation("this one is less important")
        private String explicit;

        private String ignored1;

        @BatchProperty
        private String ignored;
    }

    @Named
    @Documentation("a child comp")
    public static class MyChildComponent extends MyComponent {
        @Inject
        @BatchProperty
        @Documentation("2")
        private String config2;

        private String ignored2;
    }
}
