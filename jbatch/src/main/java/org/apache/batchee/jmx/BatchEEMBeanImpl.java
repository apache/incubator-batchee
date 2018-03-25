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
package org.apache.batchee.jmx;

import org.apache.batchee.container.impl.JobInstanceImpl;

import javax.batch.operations.JobOperator;
import javax.batch.operations.NoSuchJobException;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class BatchEEMBeanImpl implements BatchEEMBean {
    public static final BatchEEMBeanImpl INSTANCE = new BatchEEMBeanImpl();

    private static final String[] JOB_INSTANCES_ATTRIBUTES = { "jobName", "instanceId" };
    private static final TabularType JOB_INSTANCES_TABULAR_TYPE;
    private static final CompositeType JOB_INSTANCES_COMPOSITE_TYPE;

    private static final String[] PROPERTIES_ATTRIBUTES = { "key", "value" };
    private static final TabularType PROPERTIES_TABULAR_TYPE;
    private static final CompositeType PROPERTIES_COMPOSITE_TYPE;

    private static final String[] JOB_EXECUTION_ATTRIBUTES =
            { "executionId", "jobName", "Batch status", "Exit status", "Create time", "Last updated time", "Start time", "End time" };

    private static final TabularType JOB_EXECUTION_TABULAR_TYPE;
    private static final CompositeType JOB_EXECUTION_COMPOSITE_TYPE;

    private static final String[] STEP_EXECUTION_ATTRIBUTES =
            { "stepExecutionId", "stepName", "Batch status", "Exit status", "Start time", "End time", "Read", "Write",
              "Commit", "Rollback", "Read skip", "Process skip", "Write skip", "Filter" };
    private static final TabularType STEP_EXECUTION_TABULAR_TYPE;
    private static final CompositeType STEP_EXECUTION_COMPOSITE_TYPE;

    static {
        try {
            JOB_INSTANCES_COMPOSITE_TYPE = new CompositeType("JobInstance", "Job Instance", JOB_INSTANCES_ATTRIBUTES, JOB_INSTANCES_ATTRIBUTES,
                new OpenType[] { SimpleType.STRING, SimpleType.LONG });
            JOB_INSTANCES_TABULAR_TYPE = new TabularType("JobInstances", "Job Instances",
                JOB_INSTANCES_COMPOSITE_TYPE,
                JOB_INSTANCES_ATTRIBUTES);

            PROPERTIES_COMPOSITE_TYPE = new CompositeType("Properties", "Properties", PROPERTIES_ATTRIBUTES, PROPERTIES_ATTRIBUTES,
                new OpenType[] { SimpleType.STRING, SimpleType.STRING });
            PROPERTIES_TABULAR_TYPE = new TabularType("Properties", "Properties",
                PROPERTIES_COMPOSITE_TYPE,
                PROPERTIES_ATTRIBUTES);

            JOB_EXECUTION_COMPOSITE_TYPE = new CompositeType("JobExecution", "Job Execution", JOB_EXECUTION_ATTRIBUTES, JOB_EXECUTION_ATTRIBUTES,
                new OpenType[] { SimpleType.LONG, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
                                 SimpleType.STRING, SimpleType.STRING, SimpleType.STRING });
            JOB_EXECUTION_TABULAR_TYPE = new TabularType("JobExecutions", "Job Executions",
                JOB_EXECUTION_COMPOSITE_TYPE, JOB_EXECUTION_ATTRIBUTES);

            STEP_EXECUTION_COMPOSITE_TYPE = new CompositeType("StepExecution", "Step Execution", STEP_EXECUTION_ATTRIBUTES, STEP_EXECUTION_ATTRIBUTES,
                new OpenType[] { SimpleType.LONG, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
                                 SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG });
            STEP_EXECUTION_TABULAR_TYPE = new TabularType("StepExecutions", "Step Executions",
                STEP_EXECUTION_COMPOSITE_TYPE,
                STEP_EXECUTION_ATTRIBUTES);
        } catch (final OpenDataException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Properties toProperties(final String params) {
        final Properties properties = new Properties();
        if (params != null && !params.isEmpty()) {
            for (final String kv : params.split("|")) {
                final String[] split = kv.split("=");
                if (split.length == 1) {
                    continue;
                }

                properties.setProperty(split[0], split[1]);
            }
        }
        return properties;
    }

    private static Object[] asArray(final JobExecution n) {
        return new Object[] {
            n.getExecutionId(),
            n.getJobName(),
            n.getBatchStatus().name(),
            n.getExitStatus(),
            n.getCreateTime().toString(),
            n.getLastUpdatedTime().toString(),
            n.getStartTime() != null ? n.getStartTime().toString() : "",
            n.getEndTime() != null ? n.getEndTime().toString() : "",
        };
    }

    private static Object[] asArray(final StepExecution n) {
        return new Object[] {
            n.getStepExecutionId(),
            n.getStepName(),
            n.getBatchStatus().name(),
            n.getExitStatus(),
            n.getStartTime() != null ? n.getStartTime().toString() : "",
            n.getEndTime() != null ? n.getEndTime().toString() : "",
            metric(n.getMetrics(), Metric.MetricType.READ_COUNT),
            metric(n.getMetrics(), Metric.MetricType.WRITE_COUNT),
            metric(n.getMetrics(), Metric.MetricType.COMMIT_COUNT),
            metric(n.getMetrics(), Metric.MetricType.ROLLBACK_COUNT),
            metric(n.getMetrics(), Metric.MetricType.READ_SKIP_COUNT),
            metric(n.getMetrics(), Metric.MetricType.PROCESS_SKIP_COUNT),
            metric(n.getMetrics(), Metric.MetricType.WRITE_SKIP_COUNT),
            metric(n.getMetrics(), Metric.MetricType.FILTER_COUNT)
        };
    }

    private static long metric(final Metric[] metrics, final Metric.MetricType type) {
        for (final Metric m : metrics) {
            if (type.equals(m.getType())) {
                return m.getValue();
            }
        }
        return -1;
    }

    public BatchEEMBeanImpl() {
        // no-op
    }

    private JobOperator operator() { // lazy since we register jmx with the servicesmanager init
        return BatchRuntime.getJobOperator();
    }

    @Override
    public String[] getJobNames() {
        final Set<String> jobNames = operator().getJobNames();
        return jobNames.toArray(new String[jobNames.size()]);
    }

    @Override
    public int getJobInstanceCount(final String jobName) {
        return operator().getJobInstanceCount(jobName);
    }

    @Override
    public TabularData getJobInstances(final String jobName, final int start, final int count) {
        final List<JobInstance> instances = operator().getJobInstances(jobName, start, count);

        try {
            final TabularDataSupport data = new TabularDataSupport(JOB_INSTANCES_TABULAR_TYPE);
            for (final JobInstance n : instances) {
                data.put(new CompositeDataSupport(JOB_INSTANCES_COMPOSITE_TYPE, JOB_INSTANCES_ATTRIBUTES, new Object[] { n.getJobName(), n.getInstanceId() }));
            }
            return data;
        } catch (final OpenDataException e) {
            return null;
        }
    }

    @Override
    public Long[] getRunningExecutions(final String jobName) {
        try {
            final List<Long> runningExecutions = operator().getRunningExecutions(jobName);
            return runningExecutions.toArray(new Long[runningExecutions.size()]);
        } catch (final NoSuchJobException nsje) {
            return new Long[0];
        }
    }

    @Override
    public TabularData getParameters(final long executionId) {
        final Properties parameters = operator().getParameters(executionId);
        try {
            final TabularDataSupport data = new TabularDataSupport(PROPERTIES_TABULAR_TYPE);
            for (final Map.Entry<Object, Object> entry : parameters.entrySet()) {
                data.put(new CompositeDataSupport(PROPERTIES_COMPOSITE_TYPE, PROPERTIES_ATTRIBUTES, new Object[] { entry.getKey(), entry.getValue() }));
            }
            return data;
        } catch (final OpenDataException e) {
            return null;
        }
    }

    @Override
    public TabularData getJobInstance(final long executionId) {
        final JobInstance instance = operator().getJobInstance(executionId);
        try {
            final TabularDataSupport data = new TabularDataSupport(JOB_INSTANCES_TABULAR_TYPE);
            data.put(new CompositeDataSupport(JOB_INSTANCES_COMPOSITE_TYPE, JOB_INSTANCES_ATTRIBUTES, new Object[] { instance.getJobName(), instance.getInstanceId() }));
            return data;
        } catch (final OpenDataException e) {
            return null;
        }
    }

    @Override
    public void stop(final long executionId) {
        operator().stop(executionId);
    }

    @Override
    public void abandon(final long executionId) {
        operator().abandon(executionId);
    }

    @Override
    public long start(final String jobXMLName, final String jobParameters) {
        return operator().start(jobXMLName, toProperties(jobParameters));
    }

    @Override
    public long restart(final long executionId, final String restartParameters) {
        return operator().restart(executionId, toProperties(restartParameters));
    }

    @Override
    public TabularData getJobExecutions(final long id, final String name) {
        final List<JobExecution> executions = operator().getJobExecutions(new JobInstanceImpl(id, name));
        try {
            final TabularDataSupport data = new TabularDataSupport(JOB_EXECUTION_TABULAR_TYPE);
            for (final JobExecution n : executions) {
                data.put(new CompositeDataSupport(JOB_EXECUTION_COMPOSITE_TYPE, JOB_EXECUTION_ATTRIBUTES, asArray(n)));
            }
            return data;
        } catch (final OpenDataException e) {
            return null;
        }
    }

    @Override
    public TabularData getJobExecution(final long executionId) {
        final JobExecution execution = operator().getJobExecution(executionId);
        try {
            final TabularDataSupport data = new TabularDataSupport(JOB_EXECUTION_TABULAR_TYPE);
            data.put(new CompositeDataSupport(JOB_EXECUTION_COMPOSITE_TYPE, JOB_EXECUTION_ATTRIBUTES, asArray(execution)));
            return data;
        } catch (final OpenDataException e) {
            return null;
        }
    }

    @Override
    public TabularData getStepExecutions(final long jobExecutionId) {
        final List<StepExecution> executions = operator().getStepExecutions(jobExecutionId);
        try {
            final TabularDataSupport data = new TabularDataSupport(STEP_EXECUTION_TABULAR_TYPE);
            for (final StepExecution n : executions) {
                data.put(new CompositeDataSupport(STEP_EXECUTION_COMPOSITE_TYPE, STEP_EXECUTION_ATTRIBUTES, asArray(n)));
            }
            return data;
        } catch (final OpenDataException e) {
            return null;
        }
    }
}
