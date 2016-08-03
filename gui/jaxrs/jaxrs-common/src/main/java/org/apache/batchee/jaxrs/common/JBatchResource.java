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
package org.apache.batchee.jaxrs.common;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("batchee")
public interface JBatchResource {
    @GET
    @Path("job-names")
    String[] getJobNames();

    @GET
    @Path("job-instance/count/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    int getJobInstanceCount(final @PathParam("name") String jobName);

    @GET
    @Path("job-instances/{name}")
    RestJobInstance[] getJobInstances(final @PathParam("name") String jobName, final @QueryParam("start") int start, final @QueryParam("count") int count);

    @GET
    @Path("executions/running/{name}")
    Long[] getRunningExecutions(final @PathParam("name") String jobName);

    @GET
    @Path("execution/parameter/{id}")
    RestProperties getParameters(final @PathParam("id") long executionId);

    @GET
    @Path("job-instance/{id}")
    RestJobInstance getJobInstance(final @PathParam("id") long executionId);

    @GET
    @Path("job-executions/{id}/{name}")
    RestJobExecution[] getJobExecutions(final @PathParam("id") long id, final @PathParam("name") String name);

    @GET
    @Path("job-execution/{id}")
    RestJobExecution getJobExecution(final @PathParam("id") long executionId);

    @GET
    @Path("step-executions/{id}")
    RestStepExecution[] getStepExecutions(final @PathParam("id") long jobExecutionId);

    @POST
    @Path("execution/start/{name}")
    @Produces(MediaType.TEXT_PLAIN)
    long start(final @PathParam("name") String jobXMLName, final RestProperties jobParameters);

    @POST
    @Path("execution/restart/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    long restart(final @PathParam("id") long executionId, final RestProperties restartParameters);

    @HEAD
    @Path("execution/stop/{id}")
    void stop(final @PathParam("id") long executionId);

    @HEAD
    @Path("execution/abandon/{id}")
    void abandon(final @PathParam("id") long executionId);
}
