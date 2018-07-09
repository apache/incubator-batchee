/*
 * Copyright 2013 International Business Machines Corp.
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
package org.apache.batchee.container.services.executor;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.util.BatchWorkUnit;
import org.apache.batchee.spi.BatchThreadPoolService;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.batchee.container.util.ClassLoaderAwareHandler.runnableLoaderAware;

public abstract class AbstractThreadPoolService implements BatchThreadPoolService {
    private final static Logger LOGGER = Logger.getLogger(AbstractThreadPoolService.class.getName());

    protected ExecutorService executorService;

    volatile boolean shutdown = false;

    private Set<BatchWorkUnit> runningBatchWorkUnits = Collections.synchronizedSet(new HashSet<BatchWorkUnit>());

    protected abstract ExecutorService newExecutorService(Properties batchConfig);

    @Override
    public void init(final Properties batchConfig) throws BatchContainerServiceException {
        executorService = newExecutorService(batchConfig);
    }

    @Override
    public void shutdown() throws BatchContainerServiceException {
        this.shutdown = true;
        if (!runningBatchWorkUnits.isEmpty()) {
            JobOperator jobOperator = BatchRuntime.getJobOperator();
            for (BatchWorkUnit batchWorkUnit : runningBatchWorkUnits) {
                try {
                    long executionId = batchWorkUnit.getJobExecutionImpl().getExecutionId();
                    if (executionId >= 0) {
                        jobOperator.stop(executionId);
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failure while shutting down execution", e);
                }
            }
        }

        executorService.shutdownNow();
        executorService = null;
    }

    @Override
    public void executeTask(final Runnable work, final Object config) {
        if (shutdown) {
            throw new IllegalStateException("Refuse to start Batch Task due to shutdown being in progress!");
        }
        executorService.execute(runnableLoaderAware(new ActiveWorkTracker(work)));
    }

    @Override
    public String toString() {
        return getClass().getName();
    }

    class ActiveWorkTracker implements Runnable {
        private final Runnable work;

        ActiveWorkTracker(Runnable work) {
            this.work = work;
        }

        @Override
        public void run() {
            try {
                if (work instanceof BatchWorkUnit) {
                    runningBatchWorkUnits.add((BatchWorkUnit) work);
                }
                work.run();
            } finally {
                if (work instanceof BatchWorkUnit) {
                    runningBatchWorkUnits.remove(work);
                }
            }
        }
    }

}
