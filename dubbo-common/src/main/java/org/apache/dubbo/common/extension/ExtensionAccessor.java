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

/**
 * Uniform accessor for extension
 * 扩展的统一访问器
 */
public interface ExtensionAccessor {
    /**
     * 用于获取扩展加载管理器 ExtensionDirector 对象
     * 获取扩展对象 ExtensionLoader
     * 根据扩展名字获取具体扩展对象
     * 获取自适应扩展对象
     * 获取默认扩展对象
     */

    // 用于获取扩展加载管理器 ExtensionDirector 对象
    ExtensionDirector getExtensionDirector();

    // 获取扩展对象 ExtensionLoader
    default <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        // ExtensionDirector 是 扩展加载管理器 是 ExtensionLoader 的管理器
        return this.getExtensionDirector().getExtensionLoader(type);
    }

    // 根据扩展名字获取具体扩展对象
    default <T> T getExtension(Class<T> type, String name) {
        ExtensionLoader<T> extensionLoader = getExtensionLoader(type);
        return extensionLoader != null ? extensionLoader.getExtension(name) : null;
    }

    // 获取自适应扩展对象
    default <T> T getAdaptiveExtension(Class<T> type) {
        ExtensionLoader<T> extensionLoader = getExtensionLoader(type);
        return extensionLoader != null ? extensionLoader.getAdaptiveExtension() : null;
    }

    // 获取默认扩展对象
    default <T> T getDefaultExtension(Class<T> type) {
        ExtensionLoader<T> extensionLoader = getExtensionLoader(type);
        return extensionLoader != null ? extensionLoader.getDefaultExtension() : null;
    }

}
