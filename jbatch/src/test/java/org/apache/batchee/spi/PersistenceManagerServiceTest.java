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
package org.apache.batchee.spi;

import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.StepExecutionImpl;
import org.apache.batchee.container.impl.controller.chunk.CheckpointData;
import org.apache.batchee.container.impl.controller.chunk.CheckpointDataKey;
import org.apache.batchee.container.impl.controller.chunk.CheckpointType;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.InternalJobExecution;
import org.apache.batchee.container.services.persistence.JDBCPersistenceManagerService;
import org.apache.batchee.container.services.persistence.JPAPersistenceManagerService;
import org.apache.batchee.container.services.persistence.MemoryPersistenceManagerService;
import org.apache.batchee.container.status.JobStatus;
import org.junit.Test;

import javax.batch.operations.NoSuchJobExecutionException;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PersistenceManagerServiceTest {
    @Test
    public void cleanUpUntil() {
        for (final PersistenceManagerService service : asList(
            new JDBCPersistenceManagerService() {{
                init(new Properties());
            }},
            new JPAPersistenceManagerService() {{
                init(new Properties());
            }},
            new MemoryPersistenceManagerService() {{
                init(new Properties());
            }})) {
            System.out.println("");
            System.out.println(" " + service);
            System.out.println("");

            for (int i = 0; i < 3; i++) {
                final JobInstance instance = service.createJobInstance("test", "xml");
                final RuntimeJobExecution exec = service.createJobExecution(instance, new Properties(), BatchStatus.COMPLETED);
                final StepExecutionImpl step = service.createStepExecution(exec.getExecutionId(), new StepContextImpl("step"));
                service.createStepStatus(step.getStepExecutionId()).setBatchStatus(BatchStatus.STARTED);
                service.setCheckpointData(
                    new CheckpointDataKey(instance.getInstanceId(), "step", CheckpointType.READER),
                    new CheckpointData(instance.getInstanceId(), "step", CheckpointType.READER) {{
                        setRestartToken("restart".getBytes());
                    }});
                service.createJobStatus(instance.getInstanceId());
                service.updateJobStatus(instance.getInstanceId(), new JobStatus(instance) {{
                    setBatchStatus(BatchStatus.COMPLETED);
                }});
                service.updateWithFinalExecutionStatusesAndTimestamps(exec.getExecutionId(), BatchStatus.COMPLETED, "ok", new Timestamp(System.currentTimeMillis()));

                // sanity checks we persisted data before deleting them
                assertNotNull(service.getJobStatus(instance.getInstanceId()));
                assertNotNull(service.getJobInstanceIdByExecutionId(exec.getExecutionId()));
                assertNotNull(service.jobOperatorGetJobExecution(exec.getExecutionId()));
                assertNotNull(service.getStepExecutionByStepExecutionId(step.getStepExecutionId()));
                assertNotNull(service.getCheckpointData(new CheckpointDataKey(instance.getInstanceId(), "step", CheckpointType.READER)));

                if (i != 2) { // skip last since we are not there to have a break
                    try { // add some delay
                        Thread.sleep(1000);
                    } catch (final InterruptedException e) {
                        Thread.interrupted();
                        fail();
                    }
                }
            }

            // now delete until the first one (+ delta)
            final List<Long> instances = service.jobOperatorGetJobInstanceIds("test", 0, 10);
            assertEquals(3, instances.size());

            Collections.sort(instances); // we use increments everywhere so should be fine

            final InternalJobExecution firstExec = service.jobOperatorGetJobExecution(service.getMostRecentExecutionId(instances.iterator().next()));
            final Date until = new Date(firstExec.getEndTime().getTime() + 100);
            service.cleanUp(until);

            assertEquals(2, service.jobOperatorGetJobInstanceIds("test", 0, 10).size());
            assertTrue(service.getStepExecutionsForJobExecution(firstExec.getExecutionId()).isEmpty());
            assertNull(service.getCheckpointData(new CheckpointDataKey(firstExec.getInstanceId(), "step", CheckpointType.READER)));
            try {
                service.getJobInstanceIdByExecutionId(firstExec.getExecutionId());
                fail();
            } catch (final NoSuchJobExecutionException nsje) {
                // ok
            }
            assertFalse(service.jobOperatorGetJobInstanceIds("test", 0, 10).contains(firstExec.getInstanceId()));
        }
    }
}
