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
package org.apache.batchee.container.impl;

import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.services.persistence.MemoryPersistenceManagerService;
import org.apache.batchee.spi.PersistenceManagerService;
import org.junit.Test;

import javax.batch.operations.JobOperator;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.batchee.util.Batches.waitForEnd;
import static org.junit.Assert.assertEquals;

public class JobOperatorImplTest {
    @Test
    public void runningExecutionMemory_BATCHEE112() {
        final JobOperator operator = new JobOperatorImpl(new ServicesManager() {{
            init(new Properties() {{
                setProperty(PersistenceManagerService.class.getSimpleName(), MemoryPersistenceManagerService.class.getName());
            }});
        }});
        for (int i = 0; i < 10; i++) {
            final long id = operator.start("simple", new Properties() {{
                setProperty("duration", "3000");
            }});
            final List<Long> running = operator.getRunningExecutions("simple");
            assertEquals("Iteration: " + i, singletonList(id), running);
            waitForEnd(operator, id);
        }
    }
}
