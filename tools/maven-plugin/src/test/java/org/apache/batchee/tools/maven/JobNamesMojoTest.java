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

import org.testng.annotations.Test;

import javax.batch.operations.JobOperator;
import java.util.Locale;

import static org.apache.batchee.tools.maven.BatchEEMojoTestFactory.execute;
import static org.apache.batchee.tools.maven.BatchEEMojoTestFactory.mojo;
import static org.apache.batchee.tools.maven.BatchEEMojoTestFactory.output;
import static org.apache.batchee.util.Batches.waitForEnd;
import static org.testng.Assert.assertTrue;

public class JobNamesMojoTest {
    @Test
    public void noJobNames() throws Exception {
        assertTrue(execute(JobNamesMojo.class).toLowerCase(Locale.ENGLISH).contains("job names (0)"));
    }

    @Test
    public void aJobName() throws Exception {
        final JobNamesMojo mojo = mojo(JobNamesMojo.class);
        final JobOperator operator = mojo.getOrCreateOperator();
        waitForEnd(operator, operator.start("simple", null));
        waitForEnd(operator, operator.start("simple-2", null));

        mojo.execute();
        final String output = output(mojo);
        assertTrue(output.toLowerCase(Locale.ENGLISH).contains("job names (2)"));
        assertTrue(output.toLowerCase(Locale.ENGLISH).contains("simple"));
        assertTrue(output.toLowerCase(Locale.ENGLISH).contains("simple2"));
    }
}
