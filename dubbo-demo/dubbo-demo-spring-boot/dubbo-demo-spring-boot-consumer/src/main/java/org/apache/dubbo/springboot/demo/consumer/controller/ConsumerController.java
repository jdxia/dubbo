package org.apache.dubbo.springboot.demo.consumer.controller;


import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.springboot.demo.DemoService;
import org.apache.dubbo.springboot.demo.config.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@RestController
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

//    @DubboReference(async = true)
    @DubboReference()
    private DemoService demoService;

    @RequestMapping("/hello")
    public String getDubboResult() {
        String res = demoService.sayHello("=>hello<=");

        // 会是null
        System.out.println("==============> " + res);
        return res;
    }

    @RequestMapping("/asyncHello")
    public String getDubboAsyncResult() {
        log.info("===> asyncHello start");

        // 隐式传参，后面的远程调用都会隐式将这些参数发送到服务器端，类似cookie，用于框架集成，不建议常规业务使用
        RpcContext.getClientAttachment().setAttachment("index", "1");

        demoService.queryUserName("=>hello<=").whenComplete((result, e) -> {
            // DubboClientHandler-thread-1
            log.info("===> future thread: {}", Thread.currentThread().getName());

            String attachment = RpcContext.getServerContext().getAttachment("result");
            log.info("===> consumer得到的隐式传参: {}", attachment);

            if (e != null) {
               log.error("asyncHello error: " , e);
           }

           log.info("asyncHello result: {}", result);
        });


        return "hello world";
    }

    @RequestMapping("/asyncContextHello")
    public String getDubboAsyncContextResult() {
        return demoService.asyncContextSayHello("=>hello<=");
    }

}
