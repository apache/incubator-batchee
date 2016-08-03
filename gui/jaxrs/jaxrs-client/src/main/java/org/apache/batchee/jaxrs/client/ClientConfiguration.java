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

import static java.util.Arrays.asList;

public class ClientConfiguration {
    private static final String JACKSON_PROVIDER = "com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider";
    private static final String JOHNZON_PROVIDER = "org.apache.batchee.jaxrs.common.johnzon.JohnzonBatcheeProvider";

    private String baseUrl = null;
    private String jsonProvider = null;
    private ClientSecurity security = null;
    private ClientSslConfiguration ssl = null;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(final String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public Class<?> getJsonProvider() {
        if (jsonProvider == null) { // try to force jackson
            for (final String p : asList(JOHNZON_PROVIDER, JACKSON_PROVIDER)) {
                try {
                    Thread.currentThread().getContextClassLoader().loadClass(p);
                    setJsonProvider(p);
                } catch (final ClassNotFoundException cnfe) {
                    // no-op
                }
            }
            if (jsonProvider == null) {
                throw new IllegalStateException("No JSon provider found, please set it on the client configuration");
            }
        }
        try {
            return Class.class.cast(Thread.currentThread().getContextClassLoader().loadClass(jsonProvider));
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public void setJsonProvider(final String jsonProvider) {
        this.jsonProvider = jsonProvider;
    }

    public ClientSecurity getSecurity() {
        return security;
    }

    public void setSecurity(final ClientSecurity security) {
        this.security = security;
    }

    public ClientSslConfiguration getSsl() {
        return ssl;
    }

    public void setSsl(final ClientSslConfiguration ssl) {
        this.ssl = ssl;
    }
}
