package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.utils.SimpleReferenceCache;
import org.apache.dubbo.rpc.service.GenericService;

import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_DEFAULT;

public class ConsumerGenericService {

    // 方法一：通过URL参数配置Nacos认证信息
    private static final String REGISTRY_URL = "nacos://172.30.98.62:8848?username=pxb7_test&password=pxb7_test&namespace=pxb7_test";

    // 服务接口全限定名
    private static final String SERVICE_INTERFACE = "com.pxb7.mall.trade.order.client.api.OrderInfoDubboServiceI";

    // 方法名
    private static final String METHOD_NAME = "getOrderInfo";

    public static ApplicationConfig application = new ApplicationConfig("DubboSample");


    public static void main(String[] args) {

        // 清理之前的代理设置
        cleanProxy();

        Map<String, String> appConfigMap = application.getApplicationModel().getModelEnvironment().getAppConfigMap();

        appConfigMap.put("socksProxyHost", "172.30.98.64");
        appConfigMap.put("socksProxyPort", "18082");

        // 配置注册中心
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setAddress(REGISTRY_URL);
        registryConfig.setTimeout(10000);
        // 禁用注册中心作为元数据中心，解决元数据中心缺失问题
        registryConfig.setUseAsMetadataCenter(false);
        // 禁用注册中心作为配置中心（可选，根据需要）
        registryConfig.setUseAsConfigCenter(false);
        // 只订阅不注册
        registryConfig.setSubscribe(true);
        registryConfig.setRegister(false);


        // 配置应用程序设置
        application.setQosEnable(false);
        application.setEnableFileCache(false);
        application.setRegisterConsumer(false);

        // 创建并配置 ReferenceConfig
        ReferenceConfig<GenericService> referenceConfig = new ReferenceConfig<>();
        // 设置应用程序配置
        referenceConfig.setApplication(application);
        // 直接设置注册中心配置（关键步骤）
        referenceConfig.setRegistry(registryConfig);
        // 设置泛化调用
        referenceConfig.setGeneric(GENERIC_SERIALIZATION_DEFAULT);
        // 设置服务接口
        referenceConfig.setInterface(SERVICE_INTERFACE);
        // 设置超时时间
        referenceConfig.setTimeout(5000);
        // 禁用启动时检查
        referenceConfig.setCheck(false);
        // 设置服务提供者标识
        referenceConfig.setProvidedBy("order");

        // 使用 SimpleReferenceCache 替代 DubboBootstrap
        SimpleReferenceCache cache = SimpleReferenceCache.getCache();
        GenericService genericService = cache.get(referenceConfig);

        Object result = genericService.$invoke(
            METHOD_NAME,
            new String[]{String.class.getName()},
            new Object[]{"ZH16532927006316524240"}
        );

        System.out.println("========> " + result);

    }


    /**
     * 清理所有代理相关的系统属性
     * 确保在设置新的代理配置之前，清除可能存在的旧配置
     */
    private static void cleanProxy() {
        // 清理SOCKS代理配置
        System.clearProperty("socksProxyHost");
        System.clearProperty("socksProxyPort");

        // 清理HTTP代理配置
        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");

        // 清理HTTPS代理配置
        System.clearProperty("https.proxyHost");
        System.clearProperty("https.proxyPort");

        // 清理自定义FRP代理配置
        System.clearProperty("frp.proxyHost");
        System.clearProperty("frp.proxyPort");
    }
}
