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
package com.alipay.remoting.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.remoting.config.BoltClientOption;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;

public class BenchmarkClient {

    private static final byte[] BYTES = new byte[128];

    static {
        ThreadLocalRandom.current().nextBytes(BYTES);
    }

    public static void main(String[] args) throws InterruptedException {
        // 设置 Netty 高低水位线，防止在高并发压力下发送缓冲区溢出导致 OOM 或死锁
        System.setProperty("bolt.netty.buffer.high.watermark", String.valueOf(64 * 1024 * 1024));
        System.setProperty("bolt.netty.buffer.low.watermark", String.valueOf(32 * 1024 * 1024));

        RpcClient rpcClient = new RpcClient();
        // 开启 Netty Flush 整合优化：合并小的写操作以减少系统调用，显著提升高吞吐量场景下的性能
        rpcClient.option(BoltClientOption.NETTY_FLUSH_CONSOLIDATION, true);
        rpcClient.startup();
        runBenchmark(rpcClient, "127.0.0.1:18090");
    }

    /**
     * 执行压力测试
     */
    private static void runBenchmark(final RpcClient rpcClient, final String address) throws InterruptedException {
        final int requestsPerThread = 80; // 每个线程计划发送的请求总数
        final int pipelineBatchSize = 80;    // 流水线深度：一次性发出 80 个异步请求后再统一等待结果，能极大提升吞吐量
        int threadCount = Runtime.getRuntime().availableProcessors() * 16; // 并发线程数设为 CPU 核心数的 16 倍
        final CountDownLatch completionLatch = new CountDownLatch(threadCount);
        final AtomicLong totalRequestCount = new AtomicLong();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            new Thread(new Runnable() {
                final List<RpcResponseFuture> pendingFutures = new ArrayList<>(pipelineBatchSize);

                @Override
                public void run() {
                    for (int j = 0; j < requestsPerThread; j++) {
                        try {
                            // 异步调用：只管发出请求，不立即阻塞等待结果
                            pendingFutures.add(rpcClient.invokeWithFuture(address, new Request<>(BYTES), 5000));

                            // 当流水线中的请求达到设定的阈值时，开始批量获取结果 (Pipelining 核心逻辑)
                            if (pendingFutures.size() == pipelineBatchSize) {
                                waitAndClearFutures(pendingFutures);
                            }

                            // 每隔 10000 个请求打印一次全局进度
                            if (totalRequestCount.getAndIncrement() % 10000 == 0) {
                                System.out.println("Current processed count = " + totalRequestCount.get());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    // 处理线程收尾阶段：确保最后一批不满 pipelineBatchSize 的请求也被正确等待
                    if (!pendingFutures.isEmpty()) {
                        waitAndClearFutures(pendingFutures);
                    }
                    completionLatch.countDown();
                }

                // 批量等待 Future 结果并清空列表
                private void waitAndClearFutures(List<RpcResponseFuture> futures) {
                    for (RpcResponseFuture f : futures) {
                        try {
                            f.get();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                    futures.clear();
                }
            }, "benchmark_thread_" + i).start();
        }

        completionLatch.await();
        System.out.println("Final total count = " + totalRequestCount.get());


        // 3. 计算压测结果 (QPS)
        long durationInSeconds = (System.currentTimeMillis() - startTime) / 1000;
        if (durationInSeconds > 0) {
            long qps = totalRequestCount.get() / durationInSeconds;
            System.out.println("压测结果,总请求：" + totalRequestCount.get() + ", Time cost: " + durationInSeconds + "s, QPS: " + qps);
        }
    }
}
