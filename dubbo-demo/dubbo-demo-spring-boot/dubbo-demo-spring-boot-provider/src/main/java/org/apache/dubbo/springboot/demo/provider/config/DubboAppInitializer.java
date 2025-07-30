package org.apache.dubbo.springboot.demo.provider.config;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.Map;

public class DubboAppInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();

        MutablePropertySources propertySources = environment.getPropertySources();

        Map<String, Object> dubboWarmUpEnvironment = new HashMap<>();
        dubboWarmUpEnvironment.put("dubbo.provider.delay", -1);
        dubboWarmUpEnvironment.put("dubbo.provider.export", false);
        dubboWarmUpEnvironment.put("dubbo.application.manual-register", true);
        dubboWarmUpEnvironment.put("dubbo.application.auto-start", false);
        propertySources.addFirst(new MapPropertySource("dubboWarmUpEnvironment", dubboWarmUpEnvironment));

    }
}
