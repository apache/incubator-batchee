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

import org.apache.batchee.jaxrs.client.provider.Base64Filter;
import org.apache.batchee.jaxrs.common.JBatchResource;

import javax.batch.runtime.JobInstance;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

class BatchEEJAXRS2Client extends BatchEEJAXRSClientBase<Response> {
    private static final String BATCHEE_PATH = JBatchResource.class.getAnnotation(Path.class).value();

    private final WebTarget target;

    public BatchEEJAXRS2Client(final ClientConfiguration configuration) {
        final String url;
        if (configuration.getBaseUrl().endsWith("/")) {
            url = configuration.getBaseUrl() + BATCHEE_PATH;
        } else {
            url = configuration.getBaseUrl() + "/" + BATCHEE_PATH;
        }

        ClientBuilder builder = ClientBuilder.newBuilder();
        final ClientSslConfiguration ssl = configuration.getSsl();
        if (ssl != null) {
            if (ssl.getHostnameVerifier() != null) {
                builder = builder.hostnameVerifier(ssl.getHostnameVerifier());
            }
            if (ssl.getSslContext() != null) {
                builder = builder.sslContext(ssl.getSslContext());
            }
            if (ssl.getKeystore() != null) {
                builder.keyStore(ssl.getKeystore(), ssl.getKeystorePassword());
            }
        }

        WebTarget target = builder.build().target(url);
        if (configuration.getJsonProvider() != null) {
            target = target.register(configuration.getJsonProvider());
        }

        final ClientSecurity security = configuration.getSecurity();
        if (security != null) {
            if ("Basic".equalsIgnoreCase(security.getType())) {
                target = target.register(new Base64Filter(security.getUsername(), security.getPassword()));
            } else {
                throw new IllegalArgumentException("Security not supported: " + security.getType());
            }
        }

        this.target = target;
    }

    @Override
    protected Object extractEntity(final Response response, final Type type) throws Throwable {
        try {
            return Response.class.getMethod("readEntity", GenericType.class).invoke(response, new GenericType<Object>(type));
        } catch (final RuntimeException re) {
            throw re;
        } catch (final InvocationTargetException ite) {
            throw ite.getCause();
        } catch (final Exception e) {
            throw new IllegalStateException("Not using a JAXRS *2* API", e);
        }
    }

    @Override
    protected Response doInvoke(final Method jaxrsMethod, final Method method, final Object[] args) {
        final Map<String, Object> templates = new HashMap<String, Object>();
        final Map<String, Object> queryParams = new HashMap<String, Object>();

        Object body = null;
        if (args != null) {
            final Class<?>[] parameterTypes = method.getParameterTypes();
            if (parameterTypes.length == 1 && JobInstance.class.equals(parameterTypes[0])) { // getJobExecutions
                final JobInstance ji = JobInstance.class.cast(args[0]);
                templates.put("id", ji.getInstanceId());
                templates.put("name", ji.getJobName());
            } else {
                final Annotation[][] parameterAnnotations = jaxrsMethod.getParameterAnnotations();
                for (int i = 0; i < parameterAnnotations.length; i++) {
                    final Class<?> type = method.getParameterTypes()[i];

                    boolean found = false;
                    for (final Annotation a : parameterAnnotations[i]) {
                        final Class<? extends Annotation> at = a.annotationType();
                        if (PathParam.class.equals(at)) {
                            templates.put(PathParam.class.cast(a).value(), convertParam(args[i], type));
                            found = true;
                        } else if (QueryParam.class.equals(at)) {
                            queryParams.put(QueryParam.class.cast(a).value(), convertParam(args[i], type));
                            found = true;
                        }
                    }

                    if (!found) {
                        body = convertParam(args[i], type);
                    }
                }
            }
        }

        WebTarget resolvedTarget = target.path(jaxrsMethod.getAnnotation(Path.class).value());
        for (final Map.Entry<String, Object> qp : queryParams.entrySet()) {
            resolvedTarget = resolvedTarget.queryParam(qp.getKey(), qp.getValue());
        }

        final Invocation.Builder request = resolvedTarget.resolveTemplates(templates).request(findType(method.getReturnType()));
        if (jaxrsMethod.getAnnotation(GET.class) != null) {
            return request.get();
        } else if (jaxrsMethod.getAnnotation(HEAD.class) != null) {
            return request.head();
        } else if (jaxrsMethod.getAnnotation(POST.class) != null) {
            return request.post(Entity.entity(body, MediaType.APPLICATION_JSON_TYPE));
        }
        throw new IllegalArgumentException("Unexpected http method");
    }

    private MediaType findType(final Class<?> returnType) {
        return returnType.isPrimitive() ? MediaType.TEXT_PLAIN_TYPE : MediaType.APPLICATION_JSON_TYPE;
    }

    @Override
    protected void close() {
        // no-op
    }
}
