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
import org.apache.batchee.container.impl.controller.batchlet.BatchletStepController;
import org.apache.batchee.container.impl.controller.chunk.ChunkStepController;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.modelresolver.PropertyResolverFactory;
import org.apache.batchee.container.navigator.JobNavigator;
import org.apache.batchee.container.proxy.InjectionReferences;
import org.apache.batchee.container.proxy.ListenerFactory;
import org.apache.batchee.container.services.JobStatusManagerService;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.container.services.persistence.MemoryPersistenceManagerService;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.BatchArtifactFactory;
import org.apache.batchee.spi.PersistenceManagerService;
import org.apache.batchee.spi.SecurityService;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.StepExecution;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class StepLauncher {
    private static final Properties EMPTY_PROPERTIES = new Properties();
    private static final JSLProperties EMPTY_JSL_PROPERTIES = new JSLProperties();
    private static final JSLJob EMPTY_JOB = new JSLJob();

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
     * @return the step execution
     */
    public static StepExecution execute(final Step step, final Properties jobParams) {
        // services
        final ServicesManager manager = new ServicesManager();
        manager.init(TEST_PROPERTIES);

        final PersistenceManagerService persistenceManagerService = manager.service(PersistenceManagerService.class);
        final BatchArtifactFactory factory = manager.service(BatchArtifactFactory.class);

        // contextual data
        final JobInstance jobInstance = persistenceManagerService.createJobInstance(step.getId(), manager.service(SecurityService.class).getLoggedUser(), null);
        manager.service(JobStatusManagerService.class).createJobStatus(jobInstance.getInstanceId());

        final JobContextImpl jobContext = new JobContextImpl(new JobNavigator(EMPTY_JOB), EMPTY_JSL_PROPERTIES);
        final StepContextImpl stepContext = new StepContextImpl(step.getId());

        final RuntimeJobExecution runtimeJobExecution = persistenceManagerService.createJobExecution(jobInstance, EMPTY_PROPERTIES, BatchStatus.STARTED);
        final InjectionReferences injectionRefs = new InjectionReferences(jobContext, stepContext, EMPTY_JSL_PROPERTIES.getPropertyList());
        final ListenerFactory listenerFactory = new ListenerFactory(factory, EMPTY_JOB, injectionRefs, runtimeJobExecution);

        // execute it!
        runtimeJobExecution.setListenerFactory(listenerFactory);
        runtimeJobExecution.prepareForExecution(jobContext, null);

        if (step.getChunk() != null) {
            step.setChunk(PropertyResolverFactory.createChunkPropertyResolver(false).substituteProperties(step.getChunk(), jobParams));
            new ChunkStepController(runtimeJobExecution, step, stepContext, jobInstance.getInstanceId(),
                new ArrayBlockingQueue<PartitionDataWrapper>(1), manager)
                .execute();
        } else { // batchlet
            step.setBatchlet(PropertyResolverFactory.createBatchletPropertyResolver(false).substituteProperties(step.getBatchlet(), jobParams));
            new BatchletStepController(
                runtimeJobExecution, step, stepContext, jobInstance.getInstanceId(),
                new ArrayBlockingQueue<PartitionDataWrapper>(1), manager)
                .execute();
        }

        return persistenceManagerService.getStepExecutionByStepExecutionId(stepContext.getStepInternalExecID());
    }

    private StepLauncher() {
        // no-op
    }
}
