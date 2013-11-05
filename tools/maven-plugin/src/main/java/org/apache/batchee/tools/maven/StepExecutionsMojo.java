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
package org.apache.batchee.tools.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import java.util.List;
import java.util.Locale;

/**
 * Print step executions of a job execution.
 */
@Mojo(name = "step-executions")
public class StepExecutionsMojo extends BatchEEMojoBase {
    /**
     * the executionId used to find step executions.
     */
    @Parameter(required = true, property = "batchee.executionId")
    protected long executionId;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        final List<StepExecution> executions = getOrCreateOperator().getStepExecutions(executionId);
        getLog().info("Step executions for job execution #" + executionId + ":");
        for (final StepExecution exec : executions) {
            getLog().info(" - id = " + exec.getStepExecutionId());
            getLog().info("   + step = " + exec.getStepName());
            getLog().info("   + batch status = " + exec.getBatchStatus());
            getLog().info("   + exit status = " + exec.getExitStatus());
            getLog().info("   + start time = " + exec.getStartTime());
            getLog().info("   + end time = " + exec.getEndTime());
            getLog().info("   + metrics");
            if (exec.getMetrics() != null) {
                for (final Metric m : exec.getMetrics()) {
                    getLog().info("     > " + m.getType().name().replace("COUNT", "").replace("_", " ").toLowerCase(Locale.ENGLISH) + " = " + m.getValue());
                }
            }
        }
    }
}
