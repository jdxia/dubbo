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

import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.service.GenericService;

import java.io.File;

public class Application {

//    private static final String REGISTRY_URL = "zookeeper://127.0.0.1:2181";

//    private static final String REGISTRY_URL = "zookeeper://172.30.10.72:2181";

    private static final String REGISTRY_URL = "nacos://172.30.10.72:8848";



    public static void main(String[] args) {
            runWithBootstrap();
    }

    private static void runWithBootstrap() {
        // 消费者和生产者是不一样的, 记得改下
        String appName = "consumerDemo";
        String filePath = System.getProperty("user.home") + File.separator + ".dubbo" + File.separator + appName;
        // 修改dubbo的本地缓存路径，避免缓存冲突 cd ~/.dubbo
        System.setProperty("dubbo.meta.cache.filePath", filePath);
        System.setProperty("dubbo.mapping.cache.filePath", filePath);

        ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setInterface(DemoService.class);
        reference.setGeneric("true");
        // 开启异步化
//        reference.setAsync(Boolean.TRUE);
        // 关闭依赖检查
        reference.setCheck(Boolean.FALSE);
        // 请求重试次数
        reference.setRetries(2);
        // 失败自动切换
        reference.setCluster("failover");
        //设置超时时间
        reference.setTimeout(5000);

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(new ApplicationConfig("dubbo-demo-api-consumer"))
            .registry(new RegistryConfig(REGISTRY_URL))
            .protocol(new ProtocolConfig(CommonConstants.DUBBO, -1))
            .reference(reference)
            .start();

        DemoService demoService = bootstrap.getCache().get(reference);
        String message = demoService.sayHello("dubbo");
        System.out.println("=========> provider: " + message);

        // generic invoke
        GenericService genericService = (GenericService) demoService;
        Object genericInvokeResult = genericService.$invoke("sayHello", new String[]{String.class.getName()},
            new Object[]{"dubbo generic invoke"});
        System.out.println(genericInvokeResult.toString());
    }

}
