/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.extension;

import org.apache.dubbo.rpc.model.ScopeModel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 扩展点加载器的管理器
 * ExtensionDirector is a scoped extension loader manager.
 *
 * 是有多层级的
 * <p></p>
 * <p>ExtensionDirector supports multiple levels, and the child can inherit the parent's extension instances. </p>
 * <p>The way to find and create an extension instance is similar to Java classloader.</p>
 * 查找和创建扩展点的方式类似classloader
 */
public class ExtensionDirector implements ExtensionAccessor {

    // ExtensionLoader 的映射
    private final ConcurrentMap<Class<?>, ExtensionLoader<?>> extensionLoadersMap = new ConcurrentHashMap<>(64);


    private final ConcurrentMap<Class<?>, ExtensionScope> extensionScopeMap = new ConcurrentHashMap<>(64);

    // 父级, 组成多层级
    private final ExtensionDirector parent;
    private final ExtensionScope scope;
    private final List<ExtensionPostProcessor> extensionPostProcessors = new ArrayList<>();
    private final ScopeModel scopeModel;
    private final AtomicBoolean destroyed = new AtomicBoolean();

    public ExtensionDirector(ExtensionDirector parent, ExtensionScope scope, ScopeModel scopeModel) {
        this.parent = parent;
        this.scope = scope;
        this.scopeModel = scopeModel;
    }

    public void addExtensionPostProcessor(ExtensionPostProcessor processor) {
        if (!this.extensionPostProcessors.contains(processor)) {
            // 放到集合里面
            this.extensionPostProcessors.add(processor);
        }
    }

    public List<ExtensionPostProcessor> getExtensionPostProcessors() {
        return extensionPostProcessors;
    }

    @Override
    public ExtensionDirector getExtensionDirector() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ExtensionLoader<T>  getExtensionLoader(Class<T> type) {
        checkDestroyed();
        // 类型不能是空
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        // 不是接口不行
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type (" + type + ") is not an interface!");
        }
        // 类型是不是有扩展点的注解, 有没有 @SPI 注解
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type (" + type +
                ") is not an extension, because it is NOT annotated with @" + SPI.class.getSimpleName() + "!");
        }

        // 1. find in local cache
        // 查找本地缓存
        ExtensionLoader<T> loader = (ExtensionLoader<T>) extensionLoadersMap.get(type);

        ExtensionScope scope = extensionScopeMap.get(type);
        if (scope == null) {
            /**
             * 这边可以用, rpc的这个协议来辅助看
             * frameworkModel.getExtensionLoader(Protocol.class);
             *
             * 这个
             * @SPI(value = "dubbo", scope = ExtensionScope.FRAMEWORK)
             * public interface Protocol
             */

            // 获取这个类型上面的spi注解
            SPI annotation = type.getAnnotation(SPI.class);
            // 获取注解的范围
            scope = annotation.scope();
            extensionScopeMap.put(type, scope);
        }

        if (loader == null && scope == ExtensionScope.SELF) {
            // create an instance in self scope
            loader = createExtensionLoader0(type);
        }

        // 2. find in parent
        // 从父类开始找
        if (loader == null) {
            // 如果当前扩展点访问器有父级别
            if (this.parent != null) {
                // 从父级别中获取, 看能不能获取到 扩展点加载器, 类似jdk 双亲委派
                loader = this.parent.getExtensionLoader(type);
            }
        }

        // 3. create it
        if (loader == null) {
            // 创建这个类型扩展点
            // type 类似 org.apache.dubbo.rpc.Protocol
            loader = createExtensionLoader(type);
        }

        return loader;
    }

    private <T> ExtensionLoader<T> createExtensionLoader(Class<T> type) {
        ExtensionLoader<T> loader = null;
        // 判断当前扩展访问器的 scope 范围是不是和 spi上面的一致 是的话才会是true
        if (isScopeMatched(type)) {
            // if scope is matched, just create it
            // 范围匹配进行加载
            loader = createExtensionLoader0(type);
        }
        return loader;
    }

    @SuppressWarnings("unchecked")
    private <T> ExtensionLoader<T> createExtensionLoader0(Class<T> type) {
        checkDestroyed();
        ExtensionLoader<T> loader;
        // 创建出扩展点加载器 直接new出来放到缓存里面
        extensionLoadersMap.putIfAbsent(type, new ExtensionLoader<T>(type, this, scopeModel));
        loader = (ExtensionLoader<T>) extensionLoadersMap.get(type);
        return loader;
    }

    private boolean isScopeMatched(Class<?> type) {
        // 从注解里面取出来的 scope
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        // 看当前 扩展点管理器的 scope 和 它是不是匹配, 比如是不是 FRAMEWORK 级别的
        return defaultAnnotation.scope().equals(scope);
    }

    private static boolean withExtensionAnnotation(Class<?> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    public ExtensionDirector getParent() {
        return parent;
    }

    public void removeAllCachedLoader() {
    }

    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            for (ExtensionLoader<?> extensionLoader : extensionLoadersMap.values()) {
                extensionLoader.destroy();
            }
            extensionLoadersMap.clear();
            extensionScopeMap.clear();
            extensionPostProcessors.clear();
        }
    }

    private void checkDestroyed() {
        if (destroyed.get()) {
            throw new IllegalStateException("ExtensionDirector is destroyed");
        }
    }
}
