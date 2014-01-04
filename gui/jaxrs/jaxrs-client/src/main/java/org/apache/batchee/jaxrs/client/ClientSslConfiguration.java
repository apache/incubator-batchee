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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;

public class ClientSslConfiguration {
    private String hostnameVerifier = null;
    private String keystorePassword = null;
    private String keystoreType = "JKS";
    private String keystorePath = null;
    private String sslContextType = "TLS";
    private String keyManagerType = "SunX509";
    private String keyManagerPath = null;
    private String trustManagerAlgorithm = null;
    private String trustManagerProvider = null;

    public HostnameVerifier getHostnameVerifier() {
        return HostnameVerifier.class.cast(load(hostnameVerifier));
    }

    private static Class<?> load(final String name) {
        try {
            return Thread.currentThread().getContextClassLoader().loadClass(name);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public void setHostnameVerifier(final String hostnameVerifier) {
        this.hostnameVerifier = hostnameVerifier;
    }

    public SSLContext getSslContext() {
        final SSLContext context;
        try {
            context = SSLContext.getInstance(sslContextType);

            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerType);

            final KeyManager[] km;
            if (keyManagerPath != null) {
                final InputStream fin =findInputStream(keyManagerPath);
                final KeyStore ks = KeyStore.getInstance(keystoreType);
                ks.load(fin, keystorePassword.toCharArray());
                km = kmf.getKeyManagers();
            } else {
                km = null;
            }

            final TrustManager[] tm;
            if (trustManagerAlgorithm != null) {
                if (trustManagerProvider != null) {
                    tm = TrustManagerFactory.getInstance(trustManagerAlgorithm, trustManagerProvider).getTrustManagers();
                } else {
                    tm = TrustManagerFactory.getInstance(trustManagerAlgorithm).getTrustManagers();
                }
            } else {
                tm = null;
            }

            context.init(km, tm, null);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
        return context;
    }

    public KeyStore getKeystore() {
        try {
            final KeyStore keystore = KeyStore.getInstance(keystoreType);

            final InputStream is = findInputStream(keystorePath);
            if (is != null) {
                try {
                    keystore.load(is, keystorePassword.toCharArray());
                } finally {
                    is.close();
                }
            }

            return keystore;
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(final String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    private static InputStream findInputStream(final String path) throws FileNotFoundException {
        final File file = new File(path);
        if (file.isFile()) {
            return new FileInputStream(file);
        }
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    }

    public void setKeystoreType(final String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public void setKeystorePath(final String keystorePath) {
        this.keystorePath = keystorePath;
    }

    public void setSslContextType(final String sslContextType) {
        this.sslContextType = sslContextType;
    }

    public void setKeyManagerType(final String keyManagerType) {
        this.keyManagerType = keyManagerType;
    }

    public void setKeyManagerPath(final String keyManagerPath) {
        this.keyManagerPath = keyManagerPath;
    }

    public void setTrustManagerAlgorithm(final String trustManagerAlgorithm) {
        this.trustManagerAlgorithm = trustManagerAlgorithm;
    }

    public void setTrustManagerProvider(final String trustManagerProvider) {
        this.trustManagerProvider = trustManagerProvider;
    }
}
