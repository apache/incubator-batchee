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
package org.apache.batchee.camel;

import org.apache.batchee.extras.locator.BeanLocator;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;

public class CamelBridge {
    static final DefaultCamelContext CONTEXT = new DefaultCamelContext();
    static final ProducerTemplate PRODUCER_TEMPLATE = CONTEXT.createProducerTemplate();
    static final ConsumerTemplate CONSUMER_TEMPLATE = CONTEXT.createConsumerTemplate();

    private CamelBridge() {
        // private utility class ct
    }

    protected static Object process(final String locator, final String endpoint, final Object body) throws Exception {
        final BeanLocator.LocatorInstance<CamelTemplateLocator> locatorInstance = locator(locator);
        try {
            return locatorInstance.getValue().findProducerTemplate().requestBody(endpoint, body);
        } finally {
            locatorInstance.release();
        }
    }

    public static Object receive(final String locator, final String endpoint, final long timeout, final Class<?> expected) {
        final BeanLocator.LocatorInstance<CamelTemplateLocator> locatorInstance = locator(locator);
        try {
            final ConsumerTemplate consumerTemplate = locatorInstance.getValue().findConsumerTemplate();
            if (timeout > 0) {
                if (expected != null) {
                    return consumerTemplate.receiveBody(endpoint, expected);
                }
                return consumerTemplate.receiveBody(endpoint);
            }

            if (expected != null) {
                return consumerTemplate.receiveBody(endpoint, timeout, expected);
            }
            return consumerTemplate.receiveBody(endpoint, timeout);
        } finally {
            locatorInstance.release();
        }
    }

    private static BeanLocator.LocatorInstance<CamelTemplateLocator> locator(final String locator) {
        if (locator == null) {
            return new BeanLocator.LocatorInstance<CamelTemplateLocator>(DefaultCamelTemplateLocator.INSTANCE, null);
        }
        return BeanLocator.Finder.get(locator).newInstance(CamelTemplateLocator.class, locator);
    }

    public static class DefaultCamelTemplateLocator implements CamelTemplateLocator {
        public static final CamelTemplateLocator INSTANCE = new DefaultCamelTemplateLocator();


        @Override
        public ProducerTemplate findProducerTemplate() {
            return PRODUCER_TEMPLATE;
        }

        @Override
        public ConsumerTemplate findConsumerTemplate() {
            return CONSUMER_TEMPLATE;
        }
    }
}

