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
package org.apache.batchee.cdi.testng;

import org.apache.deltaspike.cdise.api.CdiContainer;
import org.apache.deltaspike.cdise.api.CdiContainerLoader;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class CdiContainerLifecycle implements ITestListener {
    private CdiContainer container;

    @Override
    public void onTestStart(final ITestResult iTestResult) {
        // no-op
    }

    @Override
    public void onTestSuccess(final ITestResult iTestResult) {
        // no-op
    }

    @Override
    public void onTestFailure(final ITestResult iTestResult) {
        // no-op
    }

    @Override
    public void onTestSkipped(final ITestResult iTestResult) {
        // no-op
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(final ITestResult iTestResult) {
        // no-op
    }

    @Override
    public void onStart(final ITestContext iTestContext) {
        container = CdiContainerLoader.getCdiContainer();
        try {
            container.boot();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onFinish(final ITestContext iTestContext) {
        try {
            container.shutdown();
        } catch (final Exception e) {
            // no-op
        }
    }
}
