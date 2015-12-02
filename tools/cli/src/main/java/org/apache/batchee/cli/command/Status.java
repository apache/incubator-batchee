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

import org.apache.batchee.cli.command.api.Command;
import org.apache.commons.lang3.StringUtils;

import javax.batch.operations.JobOperator;
import javax.batch.operations.NoSuchJobException;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import java.util.LinkedList;
import java.util.Set;

@Command(name = "status", description = "list last batches statuses")
public class Status extends JobOperatorCommand {
    @Override
    public void doRun() {
        final JobOperator operator = operator();
        final Set<String> names = operator.getJobNames();
        if (names == null || names.isEmpty()) {
            info("No job");
        } else {
            info("     Name   \t|\texecution id\t|\tbatch status\t|\texit status\t|\tstart time\t|\tend time");
            for (final String name : names) {
                try {
                    final JobExecution exec = new LinkedList<JobExecution>(
                            operator.getJobExecutions(
                                new LinkedList<JobInstance>(
                                    operator.getJobInstances(name, operator.getJobInstanceCount(name) - 1, 2)).getLast())).getLast();
                    info(String.format("%s\t|\t%12d\t|\t%s\t|\t%s\t|\t%tc\t|\t%tc",
                            StringUtils.leftPad(name, 12),
                            exec.getExecutionId(),
                            StringUtils.leftPad(exec.getBatchStatus() != null ? exec.getBatchStatus().toString() : "null", 12),
                            StringUtils.leftPad(exec.getExitStatus(), 11), exec.getStartTime(), exec.getEndTime()));
                } catch (final NoSuchJobException nsje) {
                    // no-op
                }
            }
        }
    }
}
