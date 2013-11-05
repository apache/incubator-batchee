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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import javax.batch.operations.JobOperator;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;

public class JAXRS2ClientTest extends ClientTestBase {
    private static ClassLoader oldLoader;

    @BeforeClass
    public static void setEnvironment() { // forcing to use jersey, can't be done through system properties cause of FactoryFinder order
        oldLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[0], oldLoader) {
            @Override
            public InputStream getResourceAsStream(String name) {
                if ("META-INF/services/javax.ws.rs.ext.RuntimeDelegate".equals(name)) {
                    return new ByteArrayInputStream("org.glassfish.jersey.internal.RuntimeDelegateImpl".getBytes());
                }
                if ("META-INF/services/javax.ws.rs.client.ClientBuilder".equals(name)) {
                    return new ByteArrayInputStream("org.glassfish.jersey.client.JerseyClientBuilder".getBytes());
                }
                return super.getResourceAsStream(name);
            }

            @Override
            protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.apache.cxf")) {
                    throw new ClassNotFoundException();
                }
                return super.loadClass(name, resolve);
            }
        });
    }
    @AfterClass
    public static void resetEnvironment() {
        Thread.currentThread().setContextClassLoader(oldLoader);
    }

    @Override // this client hacks the class loader to be able to use jaxrs 2 api even if we use jaxrs 1 by default
    protected JobOperator newJobOperator(final int port) {
        final ClientConfiguration configuration = new ClientConfiguration();
        configuration.setBaseUrl("http://localhost:" + port + "/");
        return BatchEEJAXRSClientFactory.newClient(configuration, BatchEEJAXRSClientFactory.API.JAXRS2);
    }
}
