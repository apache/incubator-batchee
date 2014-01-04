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

import io.airlift.command.Arguments;
import io.airlift.command.Option;
import org.apache.batchee.util.Batches;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.StepExecution;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public abstract class StartableCommand extends JobOperatorCommand {
    private static final String LINE = "=========================";

    @Option(name = "-wait", description = "should wait the end of the batch")
    protected boolean wait = true;

    @Arguments(description = "properties to pass to the batch")
    protected List<String> properties;

    @Override
    public void doRun() {
        final JobOperator operator = operator();
        final long id = doStart(operator);
        if (wait) {
            Batches.waitForEnd(operator, id);
            report(operator, id);
        }
    }

    protected abstract long doStart(JobOperator operator);

    private void report(final JobOperator operator, final long id) {
        final JobExecution execution = operator.getJobExecution(id);

        info("");
        info(LINE);

        info("Batch status: " + statusToString(execution.getBatchStatus()));
        info("Exit status:  " + execution.getExitStatus());
        if (execution.getEndTime() != null && execution.getStartTime() != null) {
            info("Duration:     " + TimeUnit.MILLISECONDS.toSeconds(execution.getEndTime().getTime() - execution.getStartTime().getTime()) + "s");
        }

        if (BatchStatus.FAILED.equals(execution.getBatchStatus())) {
            final Collection<StepExecution> stepExecutions = operator.getStepExecutions(id);
            for (final StepExecution stepExecution : stepExecutions) {
                if (BatchStatus.FAILED.equals(stepExecution.getBatchStatus())) {
                    info("");
                    info("Step name       : " + stepExecution.getStepName());
                    info("Step status     : " + statusToString(stepExecution.getBatchStatus()));
                    info("Step exit status: " + stepExecution.getExitStatus());
                    break;
                }
            }
        }

        info(LINE);
    }

    private static String statusToString(final BatchStatus status) {
        return (status != null ? status.name() : "null");
    }

    protected static Properties toProperties(final List<String> properties) {
        final Properties props = new Properties();
        if (properties != null) {
            for (final String kv : properties) {
                final String[] split = kv.split("=");
                if (split.length > 1) {
                    props.setProperty(split[0], split[1]);
                } else {
                    props.setProperty(split[0], "");
                }
            }
        }
        return props;
    }
}
