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
package org.apache.batchee.test;

import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.controller.JobController;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.modelresolver.PropertyResolverFactory;
import org.apache.batchee.container.navigator.JobNavigator;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.services.persistence.MemoryPersistenceManagerService;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.PersistenceManagerService;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import java.util.Properties;

public class StepLauncher {
    private static final Properties EMPTY_PROPERTIES = new Properties();
    private static final JSLProperties EMPTY_JSL_PROPERTIES = new JSLProperties();

    private static final Properties TEST_PROPERTIES = new Properties();

    static { // default ATM but could be TestMemoryPersistenceManager
        TEST_PROPERTIES.put(PersistenceManagerService.class.getName(), MemoryPersistenceManagerService.class.getName());
    }

    public static StepExecution execute(final Step step) {
        return execute(step, EMPTY_PROPERTIES);
    }

    /**
     * @param step the step to execute, Note: it can be modified by this method
     * @param jobParams the job parameters properties
     * @return the job execution
     */
    public static Result exec(final Step step, final Properties jobParams) {
        step.setNextFromAttribute(null); // don't allow next

        // services
        final ServicesManager manager = new ServicesManager();
        manager.init(TEST_PROPERTIES);

        final PersistenceManagerService persistenceManagerService = manager.service(PersistenceManagerService.class);

        final JSLJob job = new JSLJob();
        PropertyResolverFactory.createStepPropertyResolver(false).substituteProperties(step, jobParams);
        job.getExecutionElements().add(step);

        // contextual data
        final JobInstance jobInstance = persistenceManagerService.createJobInstance(step.getId(), null);
        manager.service(JobStatusManagerService.class).createJobStatus(jobInstance.getInstanceId());

        final JobContextImpl jobContext = new JobContextImpl(new JobNavigator(job), EMPTY_JSL_PROPERTIES);
        final StepContextImpl stepContext = new StepContextImpl(step.getId());

        final RuntimeJobExecution runtimeJobExecution = persistenceManagerService.createJobExecution(jobInstance, jobParams, BatchStatus.STARTED);

        // execute it!
        runtimeJobExecution.prepareForExecution(jobContext, null);
        new JobController(runtimeJobExecution, manager).originateExecutionOnThread();
        return new Result(stepContext, runtimeJobExecution, persistenceManagerService);
    }

    /**
     * @param step the step to execute, Note: it can be modified by this method
     * @param jobParams the job parameters properties
     * @return the step execution
     */
    public static StepExecution execute(final Step step, final Properties jobParams) {
        final Result execution = exec(step, jobParams);
        return execution.stepExecution();
    }

    public static class Result {
        private final StepContextImpl stepContext;
        private final RuntimeJobExecution jobExecution;
        private final PersistenceManagerService persistenceManagerService;

        public Result(final StepContextImpl stepContext, final RuntimeJobExecution jobExecution, final PersistenceManagerService persistenceManagerService) {
            this.stepContext = stepContext;
            this.jobExecution = jobExecution;
            this.persistenceManagerService = persistenceManagerService;
        }

        public StepExecution stepExecution() {
            return persistenceManagerService.getStepExecutionByStepExecutionId(stepContext.getStepInternalExecID());
        }

        public JobExecution jobExecution() {
            return jobExecution.getJobOperatorJobExecution();
        }
    }

    private StepLauncher() {
        // no-op
    }
}
