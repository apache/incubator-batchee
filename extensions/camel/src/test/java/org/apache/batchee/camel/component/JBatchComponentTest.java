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

import org.apache.batchee.util.Batches;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

import javax.batch.api.Batchlet;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.context.JobContext;
import javax.inject.Inject;

public class JBatchComponentTest extends CamelTestSupport {
    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Produce(uri = "direct:start-synch")
    protected ProducerTemplate templateSynch;

    @Test
    public void checkJobWasExecuted() throws Exception {
        final Object[] result = Object[].class.cast(template.requestBody(null));
        final long id = Number.class.cast(result[0]).longValue();
        final JobOperator operator = JobOperator.class.cast(result[1]);
        Batches.waitForEnd(operator, id);
        assertEquals("JBatch-Camel", operator.getJobExecution(id).getExitStatus());
    }

    @Test
    public void checkJobWasExecutedSynchonously() throws Exception {
        final Object[] result = Object[].class.cast(templateSynch.requestBody(null));
        final long id = Number.class.cast(result[0]).longValue();
        final JobOperator operator = JobOperator.class.cast(result[1]);
        assertEquals("JBatch-Camel", operator.getJobExecution(id).getExitStatus());
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from("direct:start").to("jbatch:component")
                    .process(new Processor() {
                        @Override
                        public void process(final Exchange exchange) throws Exception {
                            exchange.getIn().setBody(new Object[]{exchange.getIn().getHeader(JBatchProducer.JBATCH_EXECUTION_ID), exchange.getIn().getHeader(JBatchProducer.JBATCH_OPERATOR)});
                        }
                    }).to("mock:result");

                from("direct:start-synch").to("jbatch:component?synchronous=100")
                    .process(new Processor() {
                        @Override
                        public void process(final Exchange exchange) throws Exception {
                            exchange.getIn().setBody(new Object[]{exchange.getIn().getHeader(JBatchProducer.JBATCH_EXECUTION_ID), exchange.getIn().getHeader(JBatchProducer.JBATCH_OPERATOR)});
                        }
                    }).to("mock:result");
            }
        };
    }

    public static class ABatchlet implements Batchlet {
        @Inject
        private JobContext jobContext;

        @Override
        public String process() throws Exception {
            Thread.sleep(1000);
            jobContext.setExitStatus("JBatch-Camel");
            return null;
        }

        @Override
        public void stop() throws Exception {
            // no-op
        }
    }
}
