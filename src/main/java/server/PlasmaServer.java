/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//This class is derived from the Apache Plasma JNI test code:
//https://github.com/apache/arrow/blob/4ef95eb89f9202dfcd9017633cf55671d56e337f/java/plasma/src/test/java/org/apache/arrow/plasma/PlasmaClientTest.java#L76
package server;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class PlasmaServer {
    private static final String storeSuffix = "/tmp/plasma";
    private static int storePort;
    private static Process storeProcess;

    private static Process startProcess(final String[] cmd) {
        final ProcessBuilder builder;
        final List<String> newCmd = Arrays.stream(cmd).filter(s -> s.length() > 0).collect(Collectors.toList());
        builder = new ProcessBuilder(newCmd);
        builder.inheritIO();
        final Process p;
        try {
            p = builder.start();
        } catch (final IOException e) {
            log.error(e.getMessage());
            return null;
        }
        log.info("Start process " + p.hashCode() + " OK, cmd = " + Arrays.toString(cmd).replace(',', ' '));
        return p;
    }

    public static void startPlasmaStore(int plasmaStoreSize) {
        final int occupiedMemoryMB = plasmaStoreSize;
        final long memoryBytes = occupiedMemoryMB * 1000000;
        int numRetries = 10;
        Process p = null;
        while (numRetries-- > 0) {
            final int currentPort = java.util.concurrent.ThreadLocalRandom.current().nextInt(0, 100000);
            final String name = storeSuffix + currentPort;
            final String cmd = "plasma_store" + " -s " + name + " -m " + memoryBytes;

            p = startProcess(cmd.split(" "));

            if (p != null && p.isAlive()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(300);
                } catch (final InterruptedException e) {
                    log.error(e.getMessage());
                }
                if (p.isAlive()) {
                    storePort = currentPort;
                    break;
                }
            }
        }
        if (p == null || !p.isAlive()) {
            throw new RuntimeException("Start object store failed ...");
        } else {
            storeProcess = p;
            log.info("Start object store success");
        }
    }

    public static void cleanup() {
        log.info("Shutting down plasma store");
        if (storeProcess != null && killProcess(storeProcess)) {
            log.info("Kill plasma store process forcibly");
        }
    }

    private static boolean killProcess(final Process p) {
        if (p.isAlive()) {
            p.destroyForcibly();
            return true;
        } else {
            return false;
        }
    }

    public static String getStoreAddress() {
        return storeSuffix + storePort;
    }
}
