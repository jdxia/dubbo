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
package org.apache.dubbo.springboot.demo.provider.serive;


import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.rpc.AsyncContext;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.springboot.demo.AsyncDemoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@DubboService
public class AsyncDemoServiceImpl implements AsyncDemoService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncDemoServiceImpl.class);

    private ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 4, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    @Override
    public String sayHello(String name) {
        logger.info("============> Hello " + name + ", request from consumer: " + RpcContext.getContext().getRemoteAddress());

        try {
            TimeUnit.SECONDS.sleep(6L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return "Hello " + name;

    }

    /**
     * 服务端异步执行
     */
    @Override
    public String asyncContextSayHello(String name) {

        String resultMsg = "===> provider async context: " + name;

        // DubboServerHandler-192.168.243.2:20881-thread-4
        logger.info("===> provider async context start thread: {}", Thread.currentThread().getName());

        AsyncContext asyncContext = RpcContext.startAsync();
        poolExecutor.execute(() -> {
            asyncContext.signalContextSwitch();

            // 自己的线程池 pool-6-thread-1
            logger.info("===> provider async context handler thread: {}", Thread.currentThread().getName());

            try {
                TimeUnit.SECONDS.sleep(2L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            asyncContext.write(resultMsg);

        });

        return null;
    }

    @Override
    public CompletableFuture<String> queryUserName(String name) {
        // DubboServerHandler-192.168.243.2:20881-thread-2
        logger.info("===> provider thread: {}", Thread.currentThread().getName());

        // 获取客户端隐式传入的参数，用于框架集成，不建议常规业务使用
        String index = RpcContext.getServerAttachment().getAttachment("index");
        logger.info("===> provider得到的隐式传参index: {}", index);

        // 在服务提供方写入回传参数
        RpcContext.getServerContext().setAttachment("result", "provider: " + index);

        String resultStr = "===> async provider, name: " + name;
        /**
         * 如果不传入线程池，默认使用ForkJoinPool的commonPool，其线程数量默认是CPU的核心数量-1，推荐传入自定义的业务线程池
         *
         * CompletableFuture.supplyAsync(() -> predictQuestionNew(request), dubboAsyncBizExecutor)
         */
        return CompletableFuture.supplyAsync(() -> {

            try {
                // ForkJoinPool.commonPool-worker-1
                logger.info("===> provider async thread: {}", Thread.currentThread().getName());

                TimeUnit.SECONDS.sleep(2L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return resultStr;
        });
    }



}
