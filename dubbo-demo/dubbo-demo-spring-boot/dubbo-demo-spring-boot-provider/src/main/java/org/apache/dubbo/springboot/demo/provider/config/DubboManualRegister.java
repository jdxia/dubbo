package org.apache.dubbo.springboot.demo.provider.config;

import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.qos.command.impl.Online;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class DubboManualRegister implements ApplicationListener<ApplicationStartedEvent>, ApplicationContextAware, Ordered {

    private final static Logger log = LoggerFactory.getLogger(DubboManualRegister.class);


    private ConfigurableApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {

//        dubboOnline();

    }

    private void dubboOnline() {
        log.info("开始手动上线Dubbo服务...");
        // 获取DubboBootstrap实例
        DubboBootstrap dubboBootstrap = DubboBootstrap.getInstance();
        if (!dubboBootstrap.isInitialized()) {
            log.error("DubboBootstrap未初始化，无法手动上线服务");
            return;
        }

        FrameworkModel frameworkModel = ApplicationModel.defaultModel().getFrameworkModel();
        Online online = new Online(frameworkModel);

        /**
         * 直接调用 online 方法，".*" 表示所有服务
         * 参考
         * {@link BaseOnline#execute(CommandContext, String[])}
         */
        boolean result = online.online(".*");
        if (!result) {
            log.error("Dubbo服务手动上线失败");
        } else {
            log.info("====> dubbo 手动上线成功");
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
