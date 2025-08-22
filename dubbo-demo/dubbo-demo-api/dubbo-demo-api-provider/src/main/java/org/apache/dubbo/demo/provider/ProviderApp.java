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
package org.apache.dubbo.demo.provider;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.config.*;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.demo.DemoService;
import org.apache.dubbo.demo.UserService;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ProviderApp {

        private static final String REGISTRY_URL = "zookeeper://127.0.0.1:2181";

//    private static final String REGISTRY_URL = "zookeeper://192.168.33.20:2181";

    // http://192.168.33.20:8848/nacos
//    private static final String REGISTRY_URL = "nacos://192.168.33.20:8848";

    public static void main(String[] args) {
        cleanProxy();

        startWithBootstrap();
    }

    private static void startWithBootstrap() {
        // 消费者和生产者是不一样的, 记得改下
        String appName = "providerDemo";

        String filePath = System.getProperty("user.home") + File.separator + ".dubbo" + File.separator + appName;
        // 修改dubbo的本地缓存路径，避免缓存冲突 cd ~/.dubbo
        System.setProperty("dubbo.meta.cache.filePath", filePath);
        System.setProperty("dubbo.mapping.cache.filePath", filePath);

        List<ServiceConfig> list = new ArrayList<>();

        // 服务配置, 一个应用内可以有 0 到多个 service
        // 泛型里面包含的是 服务的接口必须有的东西
        ServiceConfig<DemoServiceImpl> demoServiceServiceConfig = new ServiceConfig<>();
        // 设置你服务暴露出去的接口
        demoServiceServiceConfig.setInterface(DemoService.class);
        demoServiceServiceConfig.setRef(new DemoServiceImpl());
        demoServiceServiceConfig.setVersion("1.0.0");
        demoServiceServiceConfig.setGroup("demo");
        // 开启异步化支持
//        demoServiceServiceConfig.setAsync(Boolean.TRUE);
        // 设置超时
        demoServiceServiceConfig.setTimeout(5000);

        // 设置线程池
        demoServiceServiceConfig.setExecutes(null);

        // ======================================================================================================

        // 一个 ServiceConfig 实例代表一个 RPC 服务
        ServiceConfig<UserServiceImpl> userServiceConfig = new ServiceConfig<>();
        userServiceConfig.setInterface(UserService.class);
        userServiceConfig.setRef(new UserServiceImpl());
        // 开启异步化支持
//        demoServiceServiceConfig.setAsync(Boolean.TRUE);
        // 设置超时
        userServiceConfig.setTimeout(5000);

        list.add(demoServiceServiceConfig);
        list.add(userServiceConfig);

        // ======================================================================================================

        /**
         * 整个 Dubbo 应用的启动入口
         *
         * 往下看
         */
        DubboBootstrap bootstrap = DubboBootstrap.getInstance();

        /**
         * 一个应用内只允许出现一个 application
         * 应用名及应用级别的一些全局配置
         */
        ApplicationConfig applicationConfig = new ApplicationConfig("dubbo-demo-api-provider");

        bootstrap.application(applicationConfig)
            /**
             * RegistryConfig
             * 注册中心实现、地址、订阅等配置
             */
            .registry(new RegistryConfig(REGISTRY_URL))
            // 元数据上报
            .metadataReport(new MetadataReportConfig(REGISTRY_URL))
            .protocol(new ProtocolConfig(CommonConstants.DUBBO, -1))
            .services(list)
            // 官方推荐使用 DubboBootstrap.start() 作为应用的集中启动入口
            // 运行中 也允许直接调用 ServiceConfig.export() 或 ReferenceConfig.refer() 方法发布单个服务
            .start()
            .await();
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
