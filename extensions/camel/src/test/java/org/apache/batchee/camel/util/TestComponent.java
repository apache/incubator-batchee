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
package org.apache.batchee.camel.util;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.impl.DefaultProducer;

import java.util.Map;

public class TestComponent extends DefaultComponent {
    @Override
    protected Endpoint createEndpoint(final String uri, final String remaining, final Map<String, Object> parameters) throws Exception {
        final String value = String.class.cast(parameters.remove("value"));
        return new DefaultEndpoint() {
            @Override
            protected String createEndpointUri() {
                return uri;
            }

            @Override
            public Producer createProducer() throws Exception {
                return new DefaultProducer(this) {
                    @Override
                    public void process(final Exchange exchange) throws Exception {
                        exchange.getIn().setBody(exchange.getIn().getBody(String.class) + value);
                    }
                };
            }

            @Override
            public Consumer createConsumer(final Processor processor) throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isSingleton() {
                return true;
            }
        };
    }
}
