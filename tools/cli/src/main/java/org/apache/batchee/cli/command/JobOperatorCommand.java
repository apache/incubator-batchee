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
package org.apache.batchee.cli.command;

import io.airlift.command.Option;
import org.apache.batchee.jaxrs.client.BatchEEJAXRSClientFactory;
import org.apache.batchee.jaxrs.client.ClientConfiguration;
import org.apache.batchee.jaxrs.client.ClientSecurity;
import org.apache.batchee.jaxrs.client.ClientSslConfiguration;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

public abstract class JobOperatorCommand implements Runnable {
    @Option(name = "-url", description = "when using JAXRS the batchee resource url")
    private String baseUrl = null;

    @Option(name = "-json", description = "when using JAXRS the json provider")
    private String jsonProvider = null;

    @Option(name = "-user", description = "when using JAXRS the username")
    private String username = null;

    @Option(name = "-password", description = "when using JAXRS the password")
    private String password = null;

    @Option(name = "-auth", description = "when using JAXRS the authentication type (Basic)")
    private String type = "Basic";

    @Option(name = "-hostnameVerifier", description = "when using JAXRS the hostname verifier")
    private String hostnameVerifier = null;

    @Option(name = "-keystorePassword", description = "when using JAXRS the keystorePassword")
    private String keystorePassword = null;

    @Option(name = "-keystoreType", description = "when using JAXRS the keystoreType (JKS)")
    private String keystoreType = "JKS";

    @Option(name = "-keystorePath", description = "when using JAXRS the keystorePath")
    private String keystorePath = null;

    @Option(name = "-sslContextType", description = "when using JAXRS the sslContextType (TLS)")
    private String sslContextType = "TLS";

    @Option(name = "-keyManagerType", description = "when using JAXRS the keyManagerType (SunX509)")
    private String keyManagerType = "SunX509";

    @Option(name = "-keyManagerPath", description = "when using JAXRS the keyManagerPath")
    private String keyManagerPath = null;

    @Option(name = "-trustManagerAlgorithm", description = "when using JAXRS the trustManagerAlgorithm")
    private String trustManagerAlgorithm = null;

    @Option(name = "-trustManagerProvider", description = "when using JAXRS the trustManagerProvider")
    private String trustManagerProvider = null;

    protected JobOperator operator() {
        if (baseUrl == null) {
            return BatchRuntime.getJobOperator();
        }

        final ClientConfiguration configuration = new ClientConfiguration();
        configuration.setBaseUrl(baseUrl);
        configuration.setJsonProvider(jsonProvider);
        if (hostnameVerifier != null || keystorePath != null || keyManagerPath != null) {
            final ClientSslConfiguration ssl = new ClientSslConfiguration();
            configuration.setSsl(ssl);
            ssl.setHostnameVerifier(hostnameVerifier);
            ssl.setKeystorePassword(keystorePassword);
            ssl.setKeyManagerPath(keyManagerPath);
            ssl.setKeyManagerType(keyManagerType);
            ssl.setKeystorePath(keystorePath);
            ssl.setKeystoreType(keystoreType);
            ssl.setSslContextType(sslContextType);
            ssl.setTrustManagerAlgorithm(trustManagerAlgorithm);
            ssl.setTrustManagerProvider(trustManagerProvider);
        }
        final ClientSecurity security = new ClientSecurity();
        configuration.setSecurity(security);
        security.setUsername(username);
        security.setPassword(password);
        security.setType(type);

        return BatchEEJAXRSClientFactory.newClient(configuration);
    }

    protected void info(final String text) {
        System.out.println(text);
    }
}
