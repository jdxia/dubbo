package org.apache.dubbo.springboot.demo.consumer.controller;


import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.springboot.demo.AsyncDemoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@RestController
@RequestMapping("/test1")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

//    @DubboReference(async = true)
    @DubboReference()
    private AsyncDemoService asyncDemoService;

    // 127.0.0.1:8283/test1/hello
    @RequestMapping("/hello")
    public String getDubboResult() {
        // 如果开启了异步, 这边会是null
        String res = asyncDemoService.sayHello("=>hello<=");

        // 如果开启了异步
        CompletableFuture<String> helloFuture = RpcContext.getServerContext().getCompletableFuture();
        // 为Future添加回调
        helloFuture.whenComplete((retValue, exception) -> {
            if (exception == null) {
                log.info("=====> 异步回调结果: {}", retValue);
            } else {
                log.info("=====> 发生了异常: {}", exception.getMessage(), exception);
            }
        });


        System.out.println("==============> " + res);
        return res;
    }

    // 127.0.0.1:8283/test1/asyncHello
    @RequestMapping("/asyncHello")
    public String getDubboAsyncResult() {
        log.info("===> asyncHello start");

        // 隐式传参，后面的远程调用都会隐式将这些参数发送到服务器端，类似cookie，用于框架集成，不建议常规业务使用
        RpcContext.getClientAttachment().setAttachment("index", "1");

        asyncDemoService.queryUserName("=>hello<=").whenComplete((result, e) -> {
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

    // 127.0.0.1:8283/test1/asyncContextHello
    @RequestMapping("/asyncContextHello")
    public String getDubboAsyncContextResult() {
        return asyncDemoService.asyncContextSayHello("=>hello<=");
    }

}
