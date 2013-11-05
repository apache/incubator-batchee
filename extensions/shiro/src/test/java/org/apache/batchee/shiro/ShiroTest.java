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
package org.apache.batchee.shiro;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.batch.api.Batchlet;
import javax.batch.operations.JobOperator;
import javax.batch.operations.JobSecurityException;
import javax.batch.runtime.BatchRuntime;
import java.util.Properties;

public class ShiroTest {
    @BeforeClass
    public static void initShiro() {
        SecurityUtils.setSecurityManager(new IniSecurityManagerFactory("classpath:shiro.ini").getInstance());
    }

    @AfterClass
    public static void resetShiro() {
        ThreadContext.unbindSubject();
        ThreadContext.unbindSecurityManager();
        ThreadContext.remove();
    }

    @Test
    public void authorized() {
        run("user1");
    }

    @Test(expectedExceptions = JobSecurityException.class)
    public void notAuthorized() {
        run("user2");
    }

    @Test(expectedExceptions = JobSecurityException.class)
    public void notLogged() {
        runJob();
    }

    private static void run(final String user) {
        final Subject subject = SecurityUtils.getSubject();
        subject.login(new UsernamePasswordToken(user, "userpwd"));
        try {
            runJob();
        } finally {
            subject.logout();
        }
    }

    private static void runJob() {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        jobOperator.start("job", new Properties());
    }

    public static class ABatchlet implements Batchlet {
        @Override
        public String process() throws Exception {
            return null;
        }

        @Override
        public void stop() throws Exception {
            // no-op
        }
    }
}
