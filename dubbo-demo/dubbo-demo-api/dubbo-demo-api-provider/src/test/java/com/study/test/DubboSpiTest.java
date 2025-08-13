package com.study.test;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.util.List;
import java.util.Set;

public class DubboSpiTest {

    public static void main(String[] args) {
        FrameworkModel frameworkModel = FrameworkModel.defaultModel();

        // 扩展点加载器
        ExtensionLoader<Protocol> extensionLoader = frameworkModel.getExtensionLoader(Protocol.class);

        // 自适应实例
        Protocol adaptiveExtension = extensionLoader.getAdaptiveExtension();
        System.out.println(adaptiveExtension);

        System.out.println("====================支持的扩展点的实例==================");

        // 支持的扩展点的实例
        Set<String> supportedExtensions = extensionLoader.getSupportedExtensions();
        for (String supportedExtension : supportedExtensions) {
            System.out.println(supportedExtension);
        }

        System.out.println("====================激活的扩展点实例====================");

        // 激活的扩展点实例
        List<Protocol> activateExtensions = extensionLoader.getActivateExtensions();
        for (Protocol protocol : activateExtensions) {
            System.out.println(protocol);
        }

    }

}
