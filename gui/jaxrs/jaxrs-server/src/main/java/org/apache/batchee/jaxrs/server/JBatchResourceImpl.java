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
package org.apache.batchee.jaxrs.server;

import org.apache.batchee.jaxrs.common.JBatchResource;
import org.apache.batchee.jaxrs.common.RestJobExecution;
import org.apache.batchee.jaxrs.common.RestJobInstance;
import org.apache.batchee.jaxrs.common.RestProperties;
import org.apache.batchee.jaxrs.common.RestStepExecution;

import javax.annotation.PostConstruct;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.JobInstance;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Set;

@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
@Path("batchee")
public class JBatchResourceImpl implements JBatchResource {
    private JobOperator operator;

    @PostConstruct
    public void findOperator() { // avoid to ServiceLoader.load everytimes
        operator = BatchRuntime.getJobOperator();
    }

    @GET
    @Path("job-names")
    public String[] getJobNames() {
        final Set<String> jobNames = operator.getJobNames();
        return jobNames.toArray(new String[jobNames.size()]);
    }

    @GET
    @Path("job-instance/count/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    public int getJobInstanceCount(final @PathParam("name") String jobName) {
        return operator.getJobInstanceCount(jobName);
    }

    @GET
    @Path("job-instances/{name}")
    public RestJobInstance[] getJobInstances(final @PathParam("name") String jobName, final @QueryParam("start") int start, final @QueryParam("count")  int count) {
        final List<RestJobInstance> restJobInstances = RestJobInstance.wrap(operator.getJobInstances(jobName, start, count));
        return restJobInstances.toArray(new RestJobInstance[restJobInstances.size()]);
    }

    @GET
    @Path("executions/running/{name}")
    public Long[] getRunningExecutions(final @PathParam("name") String jobName) {
        final List<Long> runningExecutions = operator.getRunningExecutions(jobName);
        return runningExecutions.toArray(runningExecutions.toArray(new Long[runningExecutions.size()]));
    }

    @GET
    @Path("execution/parameter/{id}")
    public RestProperties getParameters(final @PathParam("id") long executionId) {
        return RestProperties.wrap(operator.getParameters(executionId));
    }

    @GET
    @Path("job-instance/{id}")
    public RestJobInstance getJobInstance(final @PathParam("id") long executionId) {
        return RestJobInstance.wrap(operator.getJobInstance(executionId));
    }

    @GET
    @Path("job-executions/{id}/{name}")
    public RestJobExecution[] getJobExecutions(final @PathParam("id") long id, final @PathParam("name") String name) {
        final List<RestJobExecution> restJobExecutions = RestJobExecution.wrap(operator.getJobExecutions(new JobInstanceImpl(name, id)));
        return restJobExecutions.toArray(new RestJobExecution[restJobExecutions.size()]);
    }

    @GET
    @Path("job-execution/{id}")
    public RestJobExecution getJobExecution(final @PathParam("id") long executionId) {
        return RestJobExecution.wrap(operator.getJobExecution(executionId));
    }

    @GET
    @Path("step-executions/{id}")
    public RestStepExecution[] getStepExecutions(final @PathParam("id") long jobExecutionId) {
        final List<RestStepExecution> restStepExecutions = RestStepExecution.wrap(operator.getStepExecutions(jobExecutionId));
        return restStepExecutions.toArray(new RestStepExecution[restStepExecutions.size()]);
    }

    @POST
    @Path("execution/start/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    public long start(final @PathParam("name") String jobXMLName, final RestProperties jobParameters) {
        return operator.start(jobXMLName, RestProperties.unwrap(jobParameters));
    }

    @POST
    @Path("execution/restart/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public long restart(final @PathParam("id") long executionId, final RestProperties restartParameters) {
        return operator.restart(executionId, RestProperties.unwrap(restartParameters));
    }

    @HEAD
    @Path("execution/stop/{id}")
    public void stop(final @PathParam("id") long executionId) {
        operator.stop(executionId);
    }

    @HEAD
    @Path("execution/abandon/{id}")
    public void abandon(final @PathParam("id") long executionId) {
        operator.abandon(executionId);
    }

    /* // needs to import jbatch impl and get the org.apache.batchee.spi.PersistenceManagerService + same note as start/restart methods ^^
    @HEAD
    @Path("clean-up/{id}")
    public void cleanUp(final @PathParam("id") long instanceId) {
        throw new UnsupportedOperationException();
    }
    */

    private static class JobInstanceImpl implements JobInstance {
        private final String name;
        private final long id;

        public JobInstanceImpl(final String name, final long id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public long getInstanceId() {
            return id;
        }

        @Override
        public String getJobName() {
            return name;
        }
    }
}
