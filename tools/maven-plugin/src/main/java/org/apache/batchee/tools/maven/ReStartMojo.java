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

/**
 * Restart a job.
 */
@Mojo(name = "restart")
public class ReStartMojo extends JobActionMojoBase {
    /**
     * the executionId representing the execution to restart
     */
    @Parameter(required = true, property = "batchee.executionId")
    protected long executionId;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        final long restartId = getOrCreateOperator().restart(executionId, toProperties(jobParameters));
        getLog().info("Restarted #" + executionId + ", new id is #" + restartId);
        if (wait) {
            waitEnd(getOrCreateOperator(), restartId);
        }
    }
}
