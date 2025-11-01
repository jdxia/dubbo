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
package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.common.constants.CommonConstants;

import org.apache.dubbo.config.*;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.service.GenericService;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Consumer {

    private static final String REGISTRY_URL = "zookeeper://127.0.0.1:2181";

//    private static final String REGISTRY_URL = "zookeeper://192.168.33.20:2181";

//    private static final String REGISTRY_URL = "nacos://192.168.33.20:8848";



    public static void main(String[] args) {
        cleanProxy();

        runWithBootstrap();
    }

    private static void runWithBootstrap() {
        // 消费者和生产者是不一样的, 记得改下
        String appName = "consumerDemo";

        String filePath = System.getProperty("user.home") + File.separator + ".dubbo" + File.separator + appName;
        // 修改dubbo的本地缓存路径，避免缓存冲突 cd ~/.dubbo
        System.setProperty("dubbo.meta.cache.filePath", filePath);
        System.setProperty("dubbo.mapping.cache.filePath", filePath);

        List<ReferenceConfig> referenceConfigList = new ArrayList<>();

        // 一个应用内可以有 0 到多个 demoServiceReferenceConfig
        // 一个provider服务实例的一个引用, ReferenceConfig 代表调用其他服务实例的引用配置
        ReferenceConfig<DemoService> demoServiceReferenceConfig = new ReferenceConfig<>();
        demoServiceReferenceConfig.setInterface(DemoService.class);
        demoServiceReferenceConfig.setGeneric("true");
        // 开启异步化
//        demoServiceReferenceConfig.setAsync(Boolean.TRUE);
        // 关闭依赖检查
        demoServiceReferenceConfig.setCheck(Boolean.FALSE);
        // 请求重试次数
        demoServiceReferenceConfig.setRetries(2);
        // 失败自动切换
        demoServiceReferenceConfig.setCluster("failover");
        //设置超时时间
        demoServiceReferenceConfig.setTimeout(5000);
        demoServiceReferenceConfig.setGroup("demo");
        demoServiceReferenceConfig.setVersion("1.0.0");

        NotifyCallback notify = new NotifyCallback();
        MethodConfig sayHelloMethod = new MethodConfig();
        sayHelloMethod.setName("sayHello");

        // 绑定调用前后/异常的回调“对象 + 方法名”
        sayHelloMethod.setOninvoke(notify);
        sayHelloMethod.setOninvokeMethod("onInvoke");
        sayHelloMethod.setOnreturn(notify);
        sayHelloMethod.setOnreturnMethod("onReturn");
        sayHelloMethod.setOnthrow(notify);
        sayHelloMethod.setOnthrowMethod("onThrow");

        demoServiceReferenceConfig.setMethods(Collections.singletonList(sayHelloMethod));

        referenceConfigList.add(demoServiceReferenceConfig);

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        ApplicationConfig applicationConfig = new ApplicationConfig("dubbo-demo-api-consumer");
        applicationConfig.setQosEnable(false);
        applicationConfig.setRegisterConsumer(false);

        bootstrap.application(applicationConfig)
            .registry(new RegistryConfig(REGISTRY_URL))
            .protocol(new ProtocolConfig(CommonConstants.DUBBO, -1))
            .references(referenceConfigList)
            .start();


        // 直接通过 ReferenceConfig 拿到一个 DemoService 接口类型, 底层是用了动态代理
//        DemoService demoService1 = demoServiceReferenceConfig.get();

        DemoService demoService = bootstrap.getCache().get(demoServiceReferenceConfig);
        String message = demoService.sayHello("dubbo");
        System.out.println("=========> provider: " + message);

        // 调用 sayHelloAsyncContext - 使用 AsyncContext
        // setAsync(Boolean.TRUE); 要放开
        demoService.sayHelloAsyncContext("异步上下文");
        CompletableFuture<String> contextFuture = RpcContext.getContext().getCompletableFuture();
        contextFuture.thenAccept(result ->
            System.out.println("=========> sayHelloAsyncContext 结果: " + result)
        );

        // 等待异步结果完成
        try {
            Thread.sleep(1000); // 等待异步调用完成
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // generic invoke
//        GenericService genericService = (GenericService) demoService;
//        Object genericInvokeResult = genericService.$invoke("sayHello", new String[]{String.class.getName()},
//            new Object[]{"dubbo generic invoke"});
//        System.out.println(genericInvokeResult.toString());
        
    }

    private static void cleanProxy() {
        System.clearProperty("socksProxyHost");
        System.clearProperty("socksProxyPort");

        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");

        System.clearProperty("https.proxyHost");
        System.clearProperty("https.proxyPort");

        System.clearProperty("frp.proxyHost");
        System.clearProperty("frp.proxyPort");
    }

}
