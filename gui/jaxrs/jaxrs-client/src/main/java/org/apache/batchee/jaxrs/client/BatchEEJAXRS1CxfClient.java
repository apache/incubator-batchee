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

import org.apache.batchee.jaxrs.client.http.Base64s;
import org.apache.batchee.jaxrs.common.JBatchResource;
import org.apache.batchee.jaxrs.common.RestProperties;
import org.apache.cxf.configuration.jsse.TLSClientParameters;
import org.apache.cxf.configuration.security.AuthorizationPolicy;
import org.apache.cxf.jaxrs.client.Client;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.ConnectionType;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import javax.batch.runtime.JobInstance;
import javax.net.ssl.KeyManagerFactory;

class BatchEEJAXRS1CxfClient extends BatchEEJAXRSClientBase<Object> implements Base64s {
    private final JBatchResource client;

    public BatchEEJAXRS1CxfClient(final ClientConfiguration configuration) {
        try {
            final List<Object> providers = new LinkedList<Object>();
            if (configuration.getJsonProvider() != null) {
                providers.add(configuration.getJsonProvider().newInstance());
            }
            client = JAXRSClientFactory.create(configuration.getBaseUrl(), JBatchResource.class, providers);

            final HTTPConduit conduit = WebClient.getConfig(client).getHttpConduit();
            if (CxfClientConfiguration.class.isInstance(conduit)) {
                final HTTPClientPolicy policy = CxfClientConfiguration.class.cast(configuration).getPolicy();
                if (policy != null) {
                    conduit.setClient(policy);
                }
            } else {
                conduit.setClient(defaultClientPolicy());
            }

            final ClientSslConfiguration ssl = configuration.getSsl();
            if (ssl != null) {
                final TLSClientParameters params;
                if (conduit.getTlsClientParameters() == null) {
                    params = new TLSClientParameters();
                    conduit.setTlsClientParameters(params);
                } else {
                    params = conduit.getTlsClientParameters();
                }

                if (ssl.getHostnameVerifier() != null) { // not really supported in CXF 2.6
                    params.setUseHttpsURLConnectionDefaultHostnameVerifier(false);
                }
                if (ssl.getSslContext() != null) {
                    params.setSSLSocketFactory(ssl.getSslContext().getSocketFactory());
                }
                if (ssl.getKeystore() != null) {
                    try {
                        final KeyManagerFactory tmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        tmf.init(ssl.getKeystore(), ssl.getKeystorePassword().toCharArray());
                        params.setKeyManagers(tmf.getKeyManagers());
                    } catch (final Exception ex) {
                        throw new IllegalArgumentException(ex);
                    }
                }
            }

            final ClientSecurity security = configuration.getSecurity();
            if (security != null) {
                final AuthorizationPolicy authorization = new AuthorizationPolicy();
                authorization.setUserName(security.getUsername());
                authorization.setPassword(security.getPassword());
                authorization.setAuthorizationType(security.getType());
                conduit.setAuthorization(authorization);
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static HTTPClientPolicy defaultClientPolicy() {
        final HTTPClientPolicy client = new HTTPClientPolicy();
        client.setConnection(ConnectionType.CLOSE);
        client.setAllowChunking(false);
        client.setConnectionTimeout(0);
        client.setReceiveTimeout(0);
        return client;
    }

    @Override
    protected Object extractEntity(final Object o, final Type genericReturnType) {
        return o;
    }

    @Override
    protected Object doInvoke(final Method jaxrsMethod, final Method method, final Object[] args) throws Throwable {
        Object[] usedArgs = args;

        final Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 1 && JobInstance.class.equals(parameterTypes[0])) {
            if (args[0] == null) {
                usedArgs = new Object[2];
            } else {
                final JobInstance ji = JobInstance.class.cast(args[0]);
                usedArgs = new Object[] { ji.getInstanceId(), ji.getJobName() };
            }
        }
        if (args != null && args.length == 2 && Properties.class.isInstance(args[1])) {
            usedArgs[1] = RestProperties.wrap(Properties.class.cast(args[1]));
        }

        try {
            return jaxrsMethod.invoke(client, usedArgs);
        } catch (final InvocationTargetException ite) {
            throw ite.getCause();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected void close() {
        try { // not in cxf 2.6 but in cxf 2.7
            Client.class.getDeclaredMethod("close").invoke(client);
        } catch (final Exception e) {
            // no-op
        }
    }
}
