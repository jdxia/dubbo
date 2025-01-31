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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.beans.factory.ScopeBeanFactory;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.extension.ExtensionAccessor;
import org.apache.dubbo.common.extension.ExtensionDirector;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_UNABLE_DESTROY_MODEL;

/*
                      +----------------------+
                      |   Abstract ScopeModel |
                      |----------------------|
                      | <<interface>>         |
                      | ExtensionAccessor     |
                      +----------------------+
                                ▲
    ┌─────────────────────────┼─────────────────────────┐
    │                           │                           │
+--------------------+  +--------------------+  +--------------------+
|  ApplicationModel  |  |  FrameworkModel    |  |  ModuleModel        |
+--------------------+  +--------------------+  +--------------------+

关系说明：
1. Abstract ScopeModel 是一个抽象类，所有子模型类都继承它。
2. ApplicationModel、FrameworkModel 和 ModuleModel 是具体的子类，分别继承自 Abstract ScopeModel。
3. ExtensionAccessor 是一个接口，由 Abstract ScopeModel 实现。
4. 子类 (ApplicationModel、FrameworkModel、ModuleModel) 通过继承 ScopeModel，也间接实现了 ExtensionAccessor 的接口功能。

总结：
- Abstract ScopeModel 负责定义模型的基础结构，并提供一个接口扩展的能力。
- 每个子类代表不同的业务范围 (应用、框架和模块) 的模型。
*/
// 模型对象的公共抽象父类型
public abstract class ScopeModel implements ExtensionAccessor {
    /**
     * 内部id用于表示模型树的层次结构
     * 公共模型名称，可以被用户设置
     * 描述信息
     * 类加载器管理
     * 父模型管理parent
     * 当前模型的所属域ExtensionScope有:FRAMEWORK(框架)，APPLICATION(应用)，MODULE(模块)，SELF(自给自足，为每个作用域创建一个实例，用于特殊的SPI扩展，如ExtensionInjector)
     * 具体的扩展加载程序管理器对象的管理:ExtensionDirector
     * 域Bean工厂管理，一个内部共享的Bean工厂 ScopeBeanFactory
     */


    protected static final ErrorTypeAwareLogger LOGGER = LoggerFactory.getErrorTypeAwareLogger(ScopeModel.class);

    /**
     * The internal id is used to represent the hierarchy of the model tree, such as:
     * <ol>
     *     <li>1</li>
     *     FrameworkModel (index=1)
     *     <li>1.2</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2)
     *     <li>1.2.0</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2) -> ModuleModel (index=0, internal module)
     *     <li>1.2.1</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2) -> ModuleModel (index=1, first user module)
     * </ol>
     */
    // 模型id 代表模型树的层次结构, 框架模型的id就是1
    private String internalId;

    /**
     * Public Model Name, can be set from user
     * 公共模型名称，可以被用户设置
     */
    private String modelName;

    // 描述信息
    private String desc;

    // 当前域下的类加载器
    private final Set<ClassLoader> classLoaders = new ConcurrentHashSet<>();

    // 父模型管理parent
    private final ScopeModel parent;

    // 当前模型的所属域ExtensionScope有:FRAMEWORK(框架)，APPLICATION(应用)，MODULE(模块)，SELF(自给自足，为每个作用域创建一个实例，用于特殊的SPI扩展，如ExtensionInjector)
    private final ExtensionScope scope;

    // 具体扩展加载程序管理器对象的管理
    private volatile ExtensionDirector extensionDirector;

    // 域Bean工厂管理，一个内部共享的Bean工厂
    private volatile ScopeBeanFactory beanFactory;

    //使用数据结构链表，创建销毁监听器容器，一般用于关闭进程，重置应用程序对象等操作时候调用
    private final List<ScopeModelDestroyListener> destroyListeners = new CopyOnWriteArrayList<>();

    private final List<ScopeClassLoaderListener> classLoaderListeners = new CopyOnWriteArrayList<>();

    // 属性集合
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private final AtomicBoolean destroyed = new AtomicBoolean(false);
    private final boolean internalScope;

    protected final Object instLock = new Object();

    protected ScopeModel(ScopeModel parent, ExtensionScope scope, boolean isInternal) {
        this.parent = parent;
        this.scope = scope;  // 什么模型, 是框架模型还是什么
        this.internalScope = isInternal;
    }

    /**
     * NOTE:
     * <ol>
     *  <li>The initialize method only be called in subclass.</li>
     * <li>
     * In subclass, the extensionDirector and beanFactory are available in initialize but not available in constructor.
     * </li>
     * </ol>
     */
    protected void initialize() {
        synchronized (instLock) {
            /**
             * 扩展点加载器的管理器, 第一个参数是null, 第二个参数是什么模型框架模型还是什么, 第三个参数是当前模型
             * ExtensionDirector 支持多个级别, 子级可以继承父级的扩展实例
             * 查找和创建扩展方式类似java classloader
             */
            this.extensionDirector = new ExtensionDirector(parent != null ? parent.getExtensionDirector() : null, scope, this);
            // 给 扩展点加载器的管理器 添加一个扩展点的初始化后置处理器, 在扩展初始化之前或之后调用的后处理器，参数类型为ExtensionPostProcessor
            this.extensionDirector.addExtensionPostProcessor(new ScopeModelAwareExtensionProcessor(this));
            /**
             * beanFactory: ScopeBeanFactory
             * 创建一个内部共享的域工厂对象, 用于注册bean, 创建bean, 获取bean, 初始化bean等
             */
            this.beanFactory = new ScopeBeanFactory(parent != null ? parent.getBeanFactory() : null, extensionDirector);

            // Add Framework's ClassLoader by default
            //将当前类的加载器存入加载器集合classLoaders中
            ClassLoader dubboClassLoader = ScopeModel.class.getClassLoader();
            if (dubboClassLoader != null) {
                this.addClassLoader(dubboClassLoader);
            }
        }
    }

    protected abstract Lock acquireDestroyLock();

    public void destroy() {
        Lock lock = acquireDestroyLock();
        try {
            lock.lock();
            if (destroyed.compareAndSet(false, true)) {
                try {
                    onDestroy();
                    HashSet<ClassLoader> copyOfClassLoaders = new HashSet<>(classLoaders);
                    for (ClassLoader classLoader : copyOfClassLoaders) {
                        removeClassLoader(classLoader);
                    }
                    if (beanFactory != null) {
                        beanFactory.destroy();
                    }
                    if (extensionDirector != null) {
                        extensionDirector.destroy();
                    }
                } catch (Throwable t) {
                    LOGGER.error(CONFIG_UNABLE_DESTROY_MODEL, "", "", "Error happened when destroying ScopeModel.", t);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    protected void notifyDestroy() {
        for (ScopeModelDestroyListener destroyListener : destroyListeners) {
            destroyListener.onDestroy(this);
        }
    }

    protected void notifyProtocolDestroy() {
        for (ScopeModelDestroyListener destroyListener : destroyListeners) {
            if (destroyListener.isProtocol()) {
                destroyListener.onDestroy(this);
            }
        }
    }

    protected void notifyClassLoaderAdd(ClassLoader classLoader) {
        for (ScopeClassLoaderListener classLoaderListener : classLoaderListeners) {
            classLoaderListener.onAddClassLoader(this, classLoader);
        }
    }

    protected void notifyClassLoaderDestroy(ClassLoader classLoader) {
        for (ScopeClassLoaderListener classLoaderListener : classLoaderListeners) {
            classLoaderListener.onRemoveClassLoader(this, classLoader);
        }
    }

    protected abstract void onDestroy();

    public final void addDestroyListener(ScopeModelDestroyListener listener) {
        destroyListeners.add(listener);
    }

    public final void addClassLoaderListener(ScopeClassLoaderListener listener) {
        classLoaderListeners.add(listener);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public <T> T getAttribute(String key, Class<T> type) {
        return (T) attributes.get(key);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public ExtensionDirector getExtensionDirector() {
        return extensionDirector;
    }

    public ScopeBeanFactory getBeanFactory() {
        return beanFactory;
    }

    public ScopeModel getParent() {
        return parent;
    }

    public ExtensionScope getScope() {
        return scope;
    }

    public void addClassLoader(ClassLoader classLoader) {
        synchronized (instLock) {
            this.classLoaders.add(classLoader);
            if (parent != null) {
                parent.addClassLoader(classLoader);
            }
            extensionDirector.removeAllCachedLoader();
            // 通知, 会发布事件监听
            notifyClassLoaderAdd(classLoader);
        }
    }

    public void removeClassLoader(ClassLoader classLoader) {
        synchronized (instLock) {
            if (checkIfClassLoaderCanRemoved(classLoader)) {
                this.classLoaders.remove(classLoader);
                if (parent != null) {
                    parent.removeClassLoader(classLoader);
                }
                extensionDirector.removeAllCachedLoader();
                notifyClassLoaderDestroy(classLoader);
            }
        }
    }

    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return classLoader != null && !classLoader.equals(ScopeModel.class.getClassLoader());
    }

    public Set<ClassLoader> getClassLoaders() {
        return Collections.unmodifiableSet(classLoaders);
    }

    public abstract Environment getModelEnvironment();

    public String getInternalId() {
        return this.internalId;
    }

    void setInternalId(String internalId) {
        this.internalId = internalId;
    }

    protected String buildInternalId(String parentInternalId, long childIndex) {
        // FrameworkModel    1
        // ApplicationModel  1.1
        // ModuleModel       1.1.1
        if (StringUtils.hasText(parentInternalId)) {
            return parentInternalId + "." + childIndex;
        } else {
            return "" + childIndex;
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
        this.desc = buildDesc();
    }

    public boolean isInternal() {
        return internalScope;
    }

    /**
     * @return to describe string of this scope model
     */
    public String getDesc() {
        if (this.desc == null) {
            this.desc = buildDesc();
        }
        return this.desc;
    }

    private String buildDesc() {
        // Dubbo Framework[1]
        // Dubbo Application[1.1](appName)
        // Dubbo Module[1.1.1](appName/moduleName)
        String type = this.getClass().getSimpleName().replace("Model", "");
        String desc = "Dubbo " + type + "[" + this.getInternalId() + "]";

        // append model name path
        String modelNamePath = this.getModelNamePath();
        if (StringUtils.hasText(modelNamePath)) {
            desc += "(" + modelNamePath + ")";
        }
        return desc;
    }

    private String getModelNamePath() {
        if (this instanceof ApplicationModel) {
            return safeGetAppName((ApplicationModel) this);
        } else if (this instanceof ModuleModel) {
            String modelName = this.getModelName();
            if (StringUtils.hasText(modelName)) {
                // appName/moduleName
                return safeGetAppName(((ModuleModel) this).getApplicationModel()) + "/" + modelName;
            }
        }
        return null;
    }

    private static String safeGetAppName(ApplicationModel applicationModel) {
        String modelName = applicationModel.getModelName();
        if (StringUtils.isBlank(modelName)) {
            modelName = "unknown"; // unknown application
        }
        return modelName;
    }

    @Override
    public String toString() {
        return getDesc();
    }
}
