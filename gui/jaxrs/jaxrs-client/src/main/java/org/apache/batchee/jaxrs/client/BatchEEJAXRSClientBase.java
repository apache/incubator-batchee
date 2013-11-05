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
package org.apache.batchee.jaxrs.client;

import org.apache.batchee.jaxrs.client.impl.JobExecutionImpl;
import org.apache.batchee.jaxrs.client.impl.JobInstanceImpl;
import org.apache.batchee.jaxrs.client.impl.MetricImpl;
import org.apache.batchee.jaxrs.client.impl.StepExecutionImpl;
import org.apache.batchee.jaxrs.common.JBatchResource;
import org.apache.batchee.jaxrs.common.RestJobExecution;
import org.apache.batchee.jaxrs.common.RestJobInstance;
import org.apache.batchee.jaxrs.common.RestMetric;
import org.apache.batchee.jaxrs.common.RestProperties;
import org.apache.batchee.jaxrs.common.RestStepExecution;

import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.batchee.jaxrs.common.RestProperties.toProperties;

abstract class BatchEEJAXRSClientBase<RESPONSE> implements InvocationHandler {
    private static final Map<String, Method> RESOURCE_MAPPINGS = new HashMap<String, Method>();

    static {
        for (final Method m : JBatchResource.class.getMethods()) {
            if (!Object.class.equals(m.getDeclaringClass())) {
                RESOURCE_MAPPINGS.put(m.getName(), m);
            }
        }
    }

    protected abstract Object extractEntity(RESPONSE response, Type genericReturnType) throws Throwable;
    protected abstract RESPONSE doInvoke(Method jaxrsMethod, Method method, Object[] args) throws Throwable;
    protected abstract void close();

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (Closeable.class.equals(method.getDeclaringClass())) {
            close();
        }

        final Method jaxrsMethod = RESOURCE_MAPPINGS.get(method.getName());
        if (jaxrsMethod == null) {
            throw new IllegalArgumentException("Can't find mapping for " + method);
        }

        final RESPONSE response = doInvoke(jaxrsMethod, method, args);
        return convertReturnedValue(extractEntity(response, jaxrsMethod.getGenericReturnType()), method.getGenericReturnType());
    }

    protected static Object convertParam(final Object arg, final Type type) {
        if (arg == null) {
            return null;
        }

        // simple types are mapped 1-1 between JobOperator and JBatchResource
        if (String.class.equals(type) || int.class.equals(type) || long.class.equals(type)) {
            return arg;
        }

        if (Properties.class.equals(type)) {
            final Properties props = Properties.class.cast(arg);
            return RestProperties.wrap(props);
        }
        if (JobInstance.class.equals(type)) {
            final JobInstance instance = JobInstance.class.cast(arg);
            return new RestJobInstance(instance.getJobName(), instance.getInstanceId());
        }

        throw new IllegalArgumentException("Unexpected type " + type + " for a parameter");
    }

    protected static Object convertReturnedValue(final Object arg, final Type type) {
        if (arg == null || void.class.equals(type)) {
            return null;
        }

        // simple types are mapped 1-1 between JobOperator and JBatchResource
        if (String.class.equals(type) || int.class.equals(type) || long.class.equals(type)) {
            return arg;
        }

        if (Properties.class.equals(type)) {
            final RestProperties props = RestProperties.class.cast(arg);
            return toProperties(props);
        }
        if (JobInstance.class.equals(type)) {
            final RestJobInstance rij = RestJobInstance.class.cast(arg);
            return new JobInstanceImpl(rij.getName(), rij.getId());
        }
        if (JobExecution.class.equals(type)) {
            final RestJobExecution jobExec = RestJobExecution.class.cast(arg);
            return new JobExecutionImpl(
                jobExec.getId(),
                jobExec.getName(),
                toProperties(jobExec.getJobParameters()),
                jobExec.getBatchStatus(),
                jobExec.getExitStatus(),
                jobExec.getCreateTime(),
                jobExec.getStartTime(),
                jobExec.getEndTime(),
                jobExec.getLastUpdatedTime()
            );
        }
        if (ParameterizedType.class.isInstance(type)) {
            final ParameterizedType pt = ParameterizedType.class.cast(type);
            if (Set.class.equals(pt.getRawType())) {
                if (String.class.equals(pt.getActualTypeArguments()[0])) {
                    final String[] names = String[].class.cast(arg);
                    final Set<String> converted = new HashSet<String>(names.length);
                    Collections.addAll(converted, names);
                    return converted;
                }
            }
            if (List.class.equals(pt.getRawType())) {
                if (JobInstance.class.equals(pt.getActualTypeArguments()[0])) {
                    final RestJobInstance[] instances = RestJobInstance[].class.cast(arg);
                    final List<JobInstance> converted = new ArrayList<JobInstance>(instances.length);
                    for (final RestJobInstance rij : instances) {
                        converted.add(new JobInstanceImpl(rij.getName(), rij.getId()));
                    }
                    return converted;
                }
                if (Long.class.equals(pt.getActualTypeArguments()[0])) {
                    final Long[] ids = Long[].class.cast(arg);
                    final List<Long> converted = new ArrayList<Long>(ids.length);
                    Collections.addAll(converted, ids);
                    return converted;
                }
                if (JobExecution.class.equals(pt.getActualTypeArguments()[0])) {
                    final RestJobExecution[] executions = RestJobExecution[].class.cast(arg);
                    final List<JobExecution> converted = new ArrayList<JobExecution>(executions.length);
                    for (final RestJobExecution exec : executions) {
                        converted.add(new JobExecutionImpl(
                            exec.getId(),
                            exec.getName(),
                            toProperties(exec.getJobParameters()),
                            exec.getBatchStatus(),
                            exec.getExitStatus(),
                            exec.getCreateTime(),
                            exec.getStartTime(),
                            exec.getEndTime(),
                            exec.getLastUpdatedTime()
                        ));
                    }
                    return converted;
                }
                if (StepExecution.class.equals(pt.getActualTypeArguments()[0])) {
                    final RestStepExecution[] executions = RestStepExecution[].class.cast(arg);
                    final List<StepExecution> converted = new ArrayList<StepExecution>(executions.length);
                    for (final RestStepExecution exec : executions) {
                        converted.add(new StepExecutionImpl(
                            exec.getId(),
                            exec.getName(),
                            exec.getBatchStatus(),
                            exec.getExitStatus(),
                            exec.getStartTime(),
                            exec.getEndTime(),
                            null,
                            toMetrics(exec.getMetrics())
                        ));
                    }
                    return converted;
                }
            }
        }

        throw new IllegalArgumentException("Unexpected type " + type + " for a returned type");
    }

    private static Metric[] toMetrics(final List<RestMetric> metrics) {
        if (metrics == null) {
            return null;
        }
        final Metric[] array = new Metric[metrics.size()];
        int i = 0;
        for (final RestMetric m : metrics) {
            array[i++] = new MetricImpl(m.getType(), m.getValue());
        }
        return array;
    }
}
