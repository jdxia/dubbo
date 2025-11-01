package org.apache.dubbo.demo.consumer;

// 回调实现：线程安全，尽量无阻塞
public class NotifyCallback {
    // 调用前：参数签名必须和被调方法一致（这里是 sayHello(String)）
    public void onInvoke(String name) {
        System.out.println("[oninvoke] about to call sayHello, arg=" + name);
    }

    // 正常返回后：第一个参数是返回值，其后是原始入参
    public void onReturn(String result, String name) {
        System.out.println("[onreturn] result=" + result + ", arg=" + name);
    }

    // 出现异常后：第一个参数是 Throwable，其后是原始入参
    public void onThrow(Throwable ex, String name) {
        System.out.println("[onthrow] ex=" + ex.getClass().getName() + ":" + ex.getMessage()
            + ", arg=" + name);
    }
}

