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
package org.apache.batchee.util;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import java.util.Arrays;
import java.util.Collection;

public class Batches {
    private static final Collection<BatchStatus> BATCH_END_STATUSES = Arrays.asList(BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.STOPPED, BatchStatus.ABANDONED);

    private Batches() {
        // no-op
    }

    public static void waitForEnd(final long id) {
        waitForEnd(BatchRuntime.getJobOperator(), id);
    }

    public static void waitForEnd(final JobOperator jobOperator, final long id) {
        do {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                return;
            }
        } while (!BATCH_END_STATUSES.contains(jobOperator.getJobExecution(id).getBatchStatus()));
    }
}
