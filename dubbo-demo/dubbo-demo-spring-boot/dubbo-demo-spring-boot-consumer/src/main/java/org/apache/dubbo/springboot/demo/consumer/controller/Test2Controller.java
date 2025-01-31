package org.apache.dubbo.springboot.demo.consumer.controller;

import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.springboot.demo.AsyncDemoService;
import org.apache.dubbo.springboot.demo.TestDubboService;
import org.apache.dubbo.springboot.demo.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/test2")
public class Test2Controller {
    private static final Logger log = LoggerFactory.getLogger(Test2Controller.class);

    @DubboReference(timeout = 30000)
    private TestDubboService testDubboService;

    // 127.0.0.1:8283/test2/hello
    @RequestMapping("/hello")
    public List<User> getDubboResult() {
        log.info("==========> hello dubbo");
        return testDubboService.getUserList(new User(1, "hello"));
    }

}
