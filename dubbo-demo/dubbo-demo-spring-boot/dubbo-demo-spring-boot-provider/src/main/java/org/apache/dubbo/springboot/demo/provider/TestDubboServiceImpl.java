package org.apache.dubbo.springboot.demo.provider;

import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.springboot.demo.TestDubboService;
import org.apache.dubbo.springboot.demo.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@DubboService
public class TestDubboServiceImpl implements TestDubboService {

    private static final Logger log = LoggerFactory.getLogger(TestDubboServiceImpl.class);

    @Override
    public List<User> getUserList(User user) {

        log.info("==========> getUserList方法 开始执行");

//        try {
//            TimeUnit.SECONDS.sleep(10);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        List<User> list = new ArrayList<>();

        User res = new User(1, "xjd");
        User res2 = new User(2, "jdxia", "123");
        list.add(res);
        list.add(res2);

        log.info("==========> getUserList方法 执行结束");

        return list;
    }

    @Override
    public User queryUserInfo(User user) {
        if (Objects.isNull(user)) {
            return new User(1, "xjd");
        }
        user.setName("res");

        return user;
    }
}
