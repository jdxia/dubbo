package org.apache.dubbo.springboot.demo.provider;


import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.io.File;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
@EnableDubbo(scanBasePackages = {"org.apache.dubbo.springboot.demo.provider"})
public class ProviderApplication {
    public static void main(String[] args) throws Exception {

        cleanProxy();

        SpringApplication application = new SpringApplication(ProviderApplication.class);

        // 添加自定义的ApplicationContextInitializer
        application.addInitializers(context -> {
            // 获取Environment对象
            Environment env = context.getEnvironment();
            // 从Environment中读取"spring.application.name"属性值
            String appName = env.getProperty("spring.application.name");
            String filePath = System.getProperty("user.home") + File.separator + ".dubbo" + File.separator + appName;
            // 修改dubbo的本地缓存路径，避免缓存冲突
            System.setProperty("dubbo.meta.cache.filePath", filePath);
            System.setProperty("dubbo.mapping.cache.filePath", filePath);
        });

        //启动应用
        application.run(args);

        System.out.println("============> dubbo service started");
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
