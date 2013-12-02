/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */


package com.netflix.test;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 *
 * @author dweeks
 */
public class TestCaseRunner {
    public static void main(String[] args) throws ClassNotFoundException {
        Request testCase = Request.method(Class.forName(args[0]), args[1]);
        
        JUnitCore core = new JUnitCore();
        
        Result result = core.run(testCase);
        
        for(Failure f: result.getFailures()) {
            System.out.println(f.getMessage());
            f.getException().printStackTrace();
        }
        System.exit(result.wasSuccessful() ? 0 : 1);
    }
}
