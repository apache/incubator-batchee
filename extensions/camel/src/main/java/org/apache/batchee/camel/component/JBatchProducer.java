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
package org.apache.batchee.camel.component;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchStatus;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class JBatchProducer extends DefaultProducer {
    public static final String JBATCH_EXECUTION_ID = "JBatchExecutionId";
    public static final String JBATCH_OPERATOR = "JBatchOperator";

    private final JobOperator operator;
    private final String jobName;
    private final int synchronous;
    private final boolean restart;
    private final boolean stop;
    private final boolean abandon;

    public JBatchProducer(final Endpoint jBatchEndpoint, final JobOperator operator, final String job, final int synchronous,
                          final boolean restart, final boolean stop, final boolean abandon) {
        super(jBatchEndpoint);
        this.operator = operator;
        this.jobName = job;
        this.synchronous = synchronous;
        this.restart = restart;
        this.stop = stop;
        this.abandon = abandon;
    }

    @Override
    public void process(final Exchange exchange) throws Exception {
        final long id;
        if (stop) {
            final long stopId = exchange.getIn().getHeader(JBATCH_EXECUTION_ID, Long.class);
            operator.stop(stopId);
            return;
        } else if (abandon) {
            final long abandonId = exchange.getIn().getHeader(JBATCH_EXECUTION_ID, Long.class);
            operator.abandon(abandonId);
            return;
        } else if (restart) {
            final long restartId = exchange.getIn().getHeader(JBATCH_EXECUTION_ID, Long.class);
            id = operator.restart(restartId, toProperties(exchange.getIn().getHeaders()));
        } else {
            id = operator.start(jobName, toProperties(exchange.getIn().getHeaders()));
        }

        exchange.getIn().setHeader(JBATCH_EXECUTION_ID, id);
        exchange.getIn().setHeader(JBATCH_OPERATOR, operator);
        if (synchronous > 0) {
            final Collection<BatchStatus> endStatuses = Arrays.asList(BatchStatus.COMPLETED, BatchStatus.FAILED);
            do {
                try {
                    Thread.sleep(synchronous);
                } catch (final InterruptedException e) {
                    return;
                }
            } while (!endStatuses.contains(operator.getJobExecution(id).getBatchStatus()));
        }
    }

    private static Properties toProperties(final Map<String, Object> headers) {
        final Properties parametersBuilder = new Properties();
        for (final Map.Entry<String, Object> headerEntry : headers.entrySet()) {
            final String headerKey = headerEntry.getKey();
            final Object headerValue = headerEntry.getValue();
            if (headerValue != null) {
                parametersBuilder.setProperty(headerKey, headerValue.toString());
            } else {
                parametersBuilder.setProperty(headerKey, "");
            }
        }
        return parametersBuilder;
    }
}
