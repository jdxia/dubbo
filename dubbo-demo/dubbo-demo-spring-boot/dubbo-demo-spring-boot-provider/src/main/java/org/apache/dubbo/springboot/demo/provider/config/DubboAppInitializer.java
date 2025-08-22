package org.apache.dubbo.springboot.demo.provider.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.Map;

public class DubboAppInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext>, Ordered {

    private final static Logger log = LoggerFactory.getLogger(DubboAppInitializer.class);

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();

        MutablePropertySources propertySources = environment.getPropertySources();

        Map<String, Object> myDubboWarmUpEnvironment = new HashMap<>(1, 1);
//        myDubboWarmUpEnvironment.put("dubbo.provider.delay", -1);
//        myDubboWarmUpEnvironment.put("dubbo.application.manual-register", true);

        myDubboWarmUpEnvironment.put("dubbo.registry.register", false);

        propertySources.addFirst(new MapPropertySource("myDubboWarmUpEnvironment", myDubboWarmUpEnvironment));

        log.info("========> dubbo 手动注册");
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
