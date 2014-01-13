/*
 * Copyright 2012 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.test.cdi;

import org.apache.batchee.test.tck.lifecycle.ContainerLifecycle;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import static org.apache.batchee.util.Batches.waitForEnd;
import static org.testng.Assert.assertEquals;

@Listeners(ContainerLifecycle.class)
public class InjectionsTest {
    @Test
    public void run() {
        final JobOperator operator = BatchRuntime.getJobOperator();
        final long id = operator.start("injections", null);
        waitForEnd(operator, id);
        assertEquals(operator.getStepExecutions(id).iterator().next().getExitStatus(), "true");
    }
}
