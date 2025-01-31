package org.apache.dubbo.springboot.demo;

import java.util.List;

public interface TestDubboService {

    List<User> getUserList(User user);

    User queryUserInfo(User user);

}
