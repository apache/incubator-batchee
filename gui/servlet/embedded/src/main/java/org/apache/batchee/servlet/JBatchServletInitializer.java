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
package org.apache.batchee.servlet;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.Set;

public class JBatchServletInitializer implements ServletContainerInitializer {
    public static final String ACTIVE = "org.apache.batchee.servlet.active";
    public static final String CONTROLLER_MAPPING = "org.apache.batchee.servlet.mapping";
    public static final String DEFAULT_SCANNING = "org.apache.batchee.servlet.scan";
    public static final String ACTIVE_PRIVATE_FILTER = "org.apache.batchee.servlet.filter.private";
    public static final String BY_PAGE = "org.apache.batchee.servlet.pagination";

    private static final String DEFAULT_MAPPING = "/jbatch/*";

    @Override
    public void onStartup(final Set<Class<?>> classes, final ServletContext ctx) throws ServletException {
        final String active = ctx.getInitParameter(ACTIVE);
        if (active != null && !Boolean.parseBoolean(active)) {
            return;
        }

        String mapping = ctx.getInitParameter(CONTROLLER_MAPPING);
        if (mapping == null) {
            mapping = DEFAULT_MAPPING;
        } else if (!mapping.endsWith("/*")) { // needed for the controller
            mapping += "/*";
        }

        String byPage = ctx.getInitParameter(BY_PAGE);
        if (byPage == null) {
            byPage = "30";
        } else {
            byPage += byPage;
        }

        ctx.addServlet("JBatch Servlet", new JBatchController()
                                            .readOnly(false)
                                            .defaultScan(Boolean.parseBoolean(ctx.getInitParameter(DEFAULT_SCANNING)))
                                            .mapping(mapping)
                                            .executionByPage(Integer.parseInt(byPage)))
                                            .addMapping(mapping);

        final String activePrivateFilter = ctx.getInitParameter(ACTIVE_PRIVATE_FILTER);
        if (activePrivateFilter == null || Boolean.parseBoolean(activePrivateFilter)) {
            ctx.addFilter("JBatch Private Filter", PrivateFilter.class)
                .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, "/*");
        }
    }


    public static class PrivateFilter implements Filter {
        @Override
        public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
            if (HttpServletRequest.class.isInstance(request) && HttpServletResponse.class.isInstance(response)) {
                final HttpServletRequest httpServletRequest = HttpServletRequest.class.cast(request);
                final String requestURI = httpServletRequest.getRequestURI();
                if (requestURI.contains("/internal/batchee") && requestURI.endsWith("jsp")) {
                    HttpServletResponse.class.cast(response).sendError(HttpURLConnection.HTTP_NOT_FOUND);
                    return;
                }
            }

            chain.doFilter(request, response);
        }

        @Override
        public void init(final FilterConfig filterConfig) throws ServletException {
            //no-op
        }

        @Override
        public void destroy() {
            // no-op
        }
    }
}
