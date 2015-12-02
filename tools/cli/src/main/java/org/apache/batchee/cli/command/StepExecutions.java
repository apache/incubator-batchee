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
import org.apache.batchee.cli.command.api.Option;
import org.apache.commons.lang3.StringUtils;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Command(name = "stepExecutions", description = "list step executions for a particular execution")
public class StepExecutions extends JobOperatorCommand {
    @Option(name = "id", description = "execution id", required = true)
    private long id;

    public StepExecutions withId(final long id) {
        this.id = id;
        return this;
    }
    public StepExecutions withOperator(final JobOperator operator) {
        this.operator = operator;
        return this;
    }

    @Override
    public void doRun() {
        final JobOperator operator = operator();
        final List<StepExecution> executions = operator.getStepExecutions(id);
        if (executions == null || executions.isEmpty()) {
            info("Executions of " + id + " not found");
            return;
        }

        info("Step executions of " + id);

        final List<Metric.MetricType> metricsOrder = new ArrayList<Metric.MetricType>();
        final StringBuilder metrics = new StringBuilder();
        for (final Metric.MetricType type : Metric.MetricType.values()) {
            metrics.append("\t|\t").append(type.name());
            metricsOrder.add(type);
        }

        final DateFormat format = new SimpleDateFormat("YYYYMMdd hh:mm:ss");
        info("   step id\t|\t step name\t|\t    start time   \t|\t     end time    \t|\texit status\t|\tbatch status" + metrics.toString());
        for (final StepExecution exec : executions) {
            final StringBuilder builder = new StringBuilder(String.format("%10d\t|\t%s\t|\t%s\t|\t%s\t|\t%s\t|\t%s",
                exec.getStepExecutionId(),
                StringUtils.center(exec.getStepName(), 10),
                format.format(exec.getStartTime()),
                exec.getEndTime() != null ? format.format(exec.getEndTime()) : "-",
                StringUtils.center(exec.getExitStatus() == null ? "-" : exec.getExitStatus(), 11),
                StringUtils.center(String.valueOf(exec.getBatchStatus()), 12)));
            final Map<Metric.MetricType, Long> stepMetrics = new HashMap<Metric.MetricType, Long>();
            if (exec.getMetrics() != null) {
                for (final Metric m : exec.getMetrics()) {
                    stepMetrics.put(m.getType(), m.getValue());
                }
            }
            for (final Metric.MetricType type : metricsOrder) {
                final Long value = stepMetrics.get(type);
                builder.append("\t|\t");
                if (value != null) {
                    builder.append(StringUtils.center(Long.toString(value), type.name().length()));
                } else {
                    builder.append("-");
                }
            }
            info(builder.toString());
        }
    }
}
