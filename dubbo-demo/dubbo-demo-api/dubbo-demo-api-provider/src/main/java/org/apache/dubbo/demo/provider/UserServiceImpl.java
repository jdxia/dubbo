package org.apache.dubbo.demo.provider;

import org.apache.dubbo.demo.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import po.User;

public class UserServiceImpl implements UserService {
    private static final Logger logger = LoggerFactory.getLogger(UserServiceImpl.class);

    @Override
    public User queryUserInfo(User user) {
        System.out.println("============> 有调用过来, 入参是: " + user);
        user.setName("result");
        return user;
    }
}
