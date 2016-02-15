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
package org.apache.batchee.test.partitioned;

import org.apache.batchee.test.tck.lifecycle.ContainerLifecycle;
import org.apache.batchee.util.Batches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.api.partition.PartitionMapper;
import javax.batch.api.partition.PartitionPlan;
import javax.batch.api.partition.PartitionPlanImpl;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Listeners(ContainerLifecycle.class)
public class PartitionedBatchTest {

    private static final Logger log = LoggerFactory.getLogger(PartitionedBatchTest.class);


    @Test
    public void testStopPartitionedBatch() throws Exception {

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        long executionId = jobOperator.start("partition-stop", new Properties());

        do {
            log.info("Waiting til batch is started");
            Thread.sleep(50);
        }
        while (jobOperator.getJobExecution(executionId).getBatchStatus() != BatchStatus.STARTED);

        Thread.sleep(100);

        jobOperator.stop(executionId);

        BatchStatus status = Batches.waitFor(jobOperator, executionId);
        Assert.assertEquals(status, BatchStatus.STOPPED);
    }


    public static class StopReader extends AbstractItemReader {

        private static final int MAX_INVOCATIONS = 2;


        @Inject
        @BatchProperty
        private Integer idx;

        private int invocations;


        @Override
        public Object readItem() throws Exception {
            if (invocations++ < MAX_INVOCATIONS) {

                Thread.sleep(5);
                return invocations;
            }

            log.info("{} invoked {} times", idx, invocations);
            return null;
        }
    }

    public static class StopWriter extends AbstractItemWriter {

        private static final Map<Integer, List<Object>> STORAGE = new HashMap<Integer, List<Object>>(2);


        @Inject
        @BatchProperty
        private Integer idx;

        @Override
        public void writeItems(List<Object> items) throws Exception {

            List<Object> objects = STORAGE.get(idx);
            if (objects == null) {
                objects = new ArrayList<Object>();
                STORAGE.put(idx, objects);
            }

            objects.addAll(items);
        }
    }

    public static class StopMapper implements PartitionMapper {

        private static final int NUMBER_OF_PARTITIONS = 50;
        private static final int NUMBER_OF_THREADS = 5;

        @Override
        public PartitionPlan mapPartitions() throws Exception {

            Properties[] props = new Properties[NUMBER_OF_PARTITIONS];
            for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
                Properties properties = new Properties();
                properties.setProperty("idx", String.valueOf(i + 1));

                props[i] = properties;
            }

            PartitionPlanImpl plan = new PartitionPlanImpl();
            plan.setPartitions(NUMBER_OF_PARTITIONS);
            plan.setThreads(NUMBER_OF_THREADS);
            plan.setPartitionProperties(props);

            return plan;
        }
    }
}
