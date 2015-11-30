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
package org.apache.batchee.cli.command;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.commons.lang3.StringUtils;

import javax.batch.runtime.JobExecution;
import java.util.List;

@Command(name = "executions", description = "list executions")
public class Executions extends JobOperatorCommand {
    @Option(name = "-id", description = "instance id", required = true)
    private long id;

    @Override
    public void doRun() {
        final List<JobExecution> executions = operator().getJobExecutions(new JobInstanceImpl(id));
        if (!executions.isEmpty()) {
            info("Executions of " + executions.iterator().next().getJobName() + " for instance " + id);
        }

        info("execution id\t|\tbatch status\t|\texit status\t|\tstart time\t|\tend time");
        for (final JobExecution exec : executions) {
            info(String.format("%12d\t|\t%s\t|\t%s\t|\t%tc\t|\t%tc",
                    exec.getExecutionId(),
                    StringUtils.leftPad(exec.getBatchStatus() != null ? exec.getBatchStatus().toString() : "null", 12),
                    StringUtils.leftPad(exec.getExitStatus(), 11), exec.getStartTime(), exec.getEndTime()));
        }
    }
}
