package org.apache.dubbo.springboot.demo.provider;

import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.springboot.demo.AsyncDemoService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class AppTest {

    /**
     * 启动后看日志, provider那 [DUBBO] Export dubbo service 后面url是啥
     *
     * ps -l 也可以
     */
    @DubboReference(url = "dubbo://192.168.243.2:20880/")
    private AsyncDemoService asyncDemoService;

//    @DubboReference(url = "injvm://127.0.0.1/org.apache.dubbo.springboot.demo.AsyncDemoService?anyhost=true&background=false&check.serializable=false&deprecated=false")
//    private AsyncDemoService asyncDemoService;

    @Test
    void contextLoads() {

    }

    @Test
    public void test1() {
        String s = asyncDemoService.asyncContextSayHello("=>hello<=");
        System.out.println("===============> " + s);

        assert StringUtils.isNotBlank(s);

    }

}
