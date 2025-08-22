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

import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.resource.GlobalResourcesRepository;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.metadata.definition.TypeDefinitionBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Model of dubbo framework, it can be shared with multiple applications.
 * dubbo框架模型，可与多个应用程序共享
 */
public class FrameworkModel extends ScopeModel {
    /**
     * FrameworkModel实例对象集合，allInstances
     * 所有ApplicationModel实例对象集合，applicationModels
     * 发布的ApplicationModel实例对象集合 pubApplicationModels
     * 框架的服务存储库FrameworkServiceRepository类型对象(数据存储在内存中)
     * 内部的应用程序模型对象 internalApplicationModel
     */

    // ========================= Static Fields Start ===================================

    protected static final Logger LOGGER = LoggerFactory.getLogger(FrameworkModel.class);

    public static final String NAME = "FrameworkModel";
    private static final AtomicLong index = new AtomicLong(1);

    private static final Object globalLock = new Object();

    private volatile static FrameworkModel defaultInstance;

    // FrameworkModel实例对象集合
    private static final List<FrameworkModel> allInstances = new CopyOnWriteArrayList<>();

    // ========================= Static Fields End ===================================

    // internal app index is 0, default app index is 1
    private final AtomicLong appIndex = new AtomicLong(0);

    private volatile ApplicationModel defaultAppModel;

    //  所有ApplicationModel实例对象集合
    private final List<ApplicationModel> applicationModels = new CopyOnWriteArrayList<>();

    // 发布的ApplicationModel实例对象集合
    private final List<ApplicationModel> pubApplicationModels = new CopyOnWriteArrayList<>();

    // 框架的服务存储库FrameworkServiceRepository类型对象(数据存储在内存中)
    private final FrameworkServiceRepository serviceRepository;

    // 内部的应用程序模型对象
    private final ApplicationModel internalApplicationModel;

    private final ReentrantLock destroyLock = new ReentrantLock();

    /**
     * Use {@link FrameworkModel#newModel()} to create a new model
     */
    public FrameworkModel() {
        /**
         * parent: 没有父类, 框架模型是最顶层的
         * ExtensionScope 枚举, 模型的范围
         *
         * 调用父类型的ScopeModel传递参数, 第一个为空代表这是一个顶层的域模型, 第二个代表了这个是框架模型, 第三个参数为false, 代表不是内部域
         */
        super(null, ExtensionScope.FRAMEWORK, false);
        synchronized (globalLock) {
            synchronized (instLock) {
                // 框架模型的 setInternalId 是 1
                // 内部id用于表示模型树的层次结构，如层次结构: FrameworkModel(索引=1)->ApplicationModel(索引=2)->ModuleModel(索引=1，第一个用户模块)
                // 这个index变量是static类型的为静态全局变量默认值从1开始，如果有多个框架模型对象则 internalId 编号从1开始依次递增
                this.setInternalId(String.valueOf(index.getAndIncrement()));
                // register FrameworkModel instance early
                // 将当前新创建的框架实例对象添加到容器中
                allInstances.add(this);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(getDesc() + " is created");
                }

                // 往下
                // 父类的 ScopeModel , 初始化框架模型领域对象
                initialize();

                /**
                 * 类型定义构建器进行初始化
                 *
                 * 使用 TypeDefinitionBuilder 的静态方法 initBuilders 来初始化类型构建器 TypeDefinitionBuilder 类型集合
                 */
                TypeDefinitionBuilder.initBuilders(this);

                // 框架服务存储仓库对象，可以用于快速查询服务提供者信息
                serviceRepository = new FrameworkServiceRepository(this);

                //获取ScopeModelInitializer类型(域模型初始化器)的扩展加载器 ExtensionLoader，每个扩展类型都会创建一个扩展加载器缓存起来
                ExtensionLoader<ScopeModelInitializer> initializerExtensionLoader = this.getExtensionLoader(ScopeModelInitializer.class);
                // 获取 ScopeModelInitializer 类型支持的扩展集合，这里当前版本存在好几个扩展类型实现
                Set<ScopeModelInitializer> initializers = initializerExtensionLoader.getSupportedExtensionInstances();
                // 遍历这些扩展实现调用他们的 initializeFrameworkModel 方法类传递 FrameworkModel 对象, 10个
                for (ScopeModelInitializer initializer : initializers) {
                    initializer.initializeFrameworkModel(this);
                }

                // 创建一个内部的 ApplicationModel 类型
                internalApplicationModel = new ApplicationModel(this, true);
                // 创建 ApplicationConfig 类型对象同时传递应用程序模型对象 internalApplicationModel
                // 获取 ConfigManager 类型对象，然后设置添加当前应用配置对象
                internalApplicationModel.getApplicationConfigManager().setApplication(
                    new ApplicationConfig(internalApplicationModel, CommonConstants.DUBBO_INTERNAL_APPLICATION));
                // 设置公开的模块名称为常量 DUBBO_INTERNAL_APPLICATION
                internalApplicationModel.setModelName(CommonConstants.DUBBO_INTERNAL_APPLICATION);
            }
        }
    }

    @Override
    protected void onDestroy() {
        synchronized (instLock) {
            if (defaultInstance == this) {
                // NOTE: During destroying the default FrameworkModel, the FrameworkModel.defaultModel() or ApplicationModel.defaultModel()
                // will return a broken model, maybe cause unpredictable problem.
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Destroying default framework model: " + getDesc());
                }
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(getDesc() + " is destroying ...");
            }

            // destroy all application model
            for (ApplicationModel applicationModel : new ArrayList<>(applicationModels)) {
                applicationModel.destroy();
            }
            // check whether all application models are destroyed
            checkApplicationDestroy();

            // notify destroy and clean framework resources
            // see org.apache.dubbo.config.deploy.FrameworkModelCleaner
            notifyDestroy();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(getDesc() + " is destroyed");
            }

            // remove from allInstances and reset default FrameworkModel
            synchronized (globalLock) {
                allInstances.remove(this);
                resetDefaultFrameworkModel();
            }

            // if all FrameworkModels are destroyed, clean global static resources, shutdown dubbo completely
            destroyGlobalResources();
        }
    }

    private void checkApplicationDestroy() {
        synchronized (instLock) {
            if (applicationModels.size() > 0) {
                List<String> remainApplications = applicationModels.stream()
                    .map(ScopeModel::getDesc)
                    .collect(Collectors.toList());
                throw new IllegalStateException("Not all application models are completely destroyed, remaining " +
                    remainApplications.size() + " application models may be created during destruction: " + remainApplications);
            }
        }
    }

    private void destroyGlobalResources() {
        synchronized (globalLock) {
            if (allInstances.isEmpty()) {
                GlobalResourcesRepository.getInstance().destroy();
            }
        }
    }

    /**
     * 在销毁默认FrameworkModel的过程中，
     * FrameworkModel. defaultModel（）或ApplicationModel. defaultMode（）将返回一个损坏的模型，这可能会导致不可预测的问题。
     * 建议：尽量避免使用默认模型
     *
     * During destroying the default FrameworkModel, the FrameworkModel.defaultModel() or ApplicationModel.defaultModel()
     * will return a broken model, maybe cause unpredictable problem.
     * Recommendation: Avoid using the default model as much as possible.
     * @return the global default FrameworkModel
     */
    public static FrameworkModel defaultModel() {
        FrameworkModel instance = defaultInstance;
        // 框架模型默认一个就可以, 没有就创建并且加锁
        // 双重校验锁
        if (instance == null) {
            synchronized (globalLock) {
                // 重置默认框架模型
                resetDefaultFrameworkModel();
                if (defaultInstance == null) {
                    // 往下看
                    defaultInstance = new FrameworkModel();
                }
                instance = defaultInstance;
            }
        }
        Assert.notNull(instance, "Default FrameworkModel is null");
        return instance;
    }

    /**
     * Get all framework model instances
     * @return
     */
    public static List<FrameworkModel> getAllInstances() {
        synchronized (globalLock) {
            return Collections.unmodifiableList(new ArrayList<>(allInstances));
        }
    }

    /**
     * Destroy all framework model instances, shutdown dubbo engine completely.
     */
    public static void destroyAll() {
        synchronized (globalLock) {
            for (FrameworkModel frameworkModel : new ArrayList<>(allInstances)) {
                frameworkModel.destroy();
            }
        }
    }

    public ApplicationModel newApplication() {
        synchronized (instLock) {
            return new ApplicationModel(this);
        }
    }

    /**
     * Get or create default application model
     * @return
     */
    public ApplicationModel defaultApplication() {
        ApplicationModel appModel = this.defaultAppModel;
        if (appModel == null) {
            // check destroyed before acquire inst lock, avoid blocking during destroying
            checkDestroyed();
            // 重置默认的应用模型对象
            resetDefaultAppModel();
            if ((appModel = this.defaultAppModel) == null) {
                synchronized (instLock) {
                    if (this.defaultAppModel == null) {
                        this.defaultAppModel = newApplication();
                    }
                    appModel = this.defaultAppModel;
                }
            }
        }
        Assert.notNull(appModel, "Default ApplicationModel is null");
        return appModel;
    }

    ApplicationModel getDefaultAppModel() {
        return defaultAppModel;
    }

    void addApplication(ApplicationModel applicationModel) {
        // can not add new application if it's destroying
        //检查FrameworkModel对象是否已经被标记为销毁状态，如果已经被销毁了则抛出异常无需执行逻辑
        checkDestroyed();
        synchronized (instLock) {
            //如果还未添加过当前参数传递应用模型
            if (!this.applicationModels.contains(applicationModel)) {
                //为当前应用模型生成内部id
                applicationModel.setInternalId(buildInternalId(getInternalId(), appIndex.getAndIncrement()));
                //添加到成员变量集合applicationModels中
                this.applicationModels.add(applicationModel);
                //如果非内部的则也向公开应用模型集合pubApplicationModels中添加一下
                if (!applicationModel.isInternal()) {
                    this.pubApplicationModels.add(applicationModel);
                }
            }
        }
    }

    void removeApplication(ApplicationModel model) {
        synchronized (instLock) {
            this.applicationModels.remove(model);
            if (!model.isInternal()) {
                this.pubApplicationModels.remove(model);
            }
            resetDefaultAppModel();
        }
    }

    /**
     * Protocols are special resources that need to be destroyed as soon as possible.
     *
     * Since connections inside protocol are not classified by applications, trying to destroy protocols in advance might only work for singleton application scenario.
     */
    void tryDestroyProtocols() {
        synchronized (instLock) {
            if (pubApplicationModels.size() == 0) {
                notifyProtocolDestroy();
            }
        }
    }

    void tryDestroy() {
        synchronized (instLock) {
            if (pubApplicationModels.size() == 0) {
                destroy();
            }
        }
    }

    private void checkDestroyed() {
        if (isDestroyed()) {
            throw new IllegalStateException("FrameworkModel is destroyed");
        }
    }

    private void resetDefaultAppModel() {
        synchronized (instLock) {
            if (this.defaultAppModel != null && !this.defaultAppModel.isDestroyed()) {
                return;
            }
            //取第一个公开的应用模型做为默认应用模型
            ApplicationModel oldDefaultAppModel = this.defaultAppModel;
            if (pubApplicationModels.size() > 0) {
                this.defaultAppModel = pubApplicationModels.get(0);
            } else {
                this.defaultAppModel = null;
            }
            if (defaultInstance == this && oldDefaultAppModel != this.defaultAppModel) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Reset global default application from " + safeGetModelDesc(oldDefaultAppModel) + " to " + safeGetModelDesc(this.defaultAppModel));
                }
            }
        }
    }

    private static void resetDefaultFrameworkModel() {
        // 全局锁
        synchronized (globalLock) {
            // defaultInstance 成员变量代表默认的框架模型
            if (defaultInstance != null && !defaultInstance.isDestroyed()) {
                return;
            }

            // 存在实例模型列表则直接从内存缓存中查看, 后续不需要创建的
            // 一开始是null
            FrameworkModel oldDefaultFrameworkModel = defaultInstance;
            if (allInstances.size() > 0) {
                // 当前存在的有 FrameworkModel 框架实例多个列表则取第一个默认的
                defaultInstance = allInstances.get(0);
            } else {
                defaultInstance = null;
            }
            if (oldDefaultFrameworkModel != defaultInstance) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Reset global default framework from " + safeGetModelDesc(oldDefaultFrameworkModel) + " to " + safeGetModelDesc(defaultInstance));
                }
            }
        }
    }

    private static String safeGetModelDesc(ScopeModel scopeModel) {
        return scopeModel != null ? scopeModel.getDesc() : null;
    }

    /**
     * Get all application models except for the internal application model.
     */
    public List<ApplicationModel> getApplicationModels() {
        synchronized (globalLock) {
            return Collections.unmodifiableList(pubApplicationModels);
        }
    }

    /**
     * Get all application models including the internal application model.
     */
    public List<ApplicationModel> getAllApplicationModels() {
        synchronized (globalLock) {
            return Collections.unmodifiableList(applicationModels);
        }
    }

    public ApplicationModel getInternalApplicationModel() {
        return internalApplicationModel;
    }

    public FrameworkServiceRepository getServiceRepository() {
        return serviceRepository;
    }


    @Override
    protected Lock acquireDestroyLock() {
        return destroyLock;
    }

    @Override
    public Environment getModelEnvironment() {
        throw new UnsupportedOperationException("Environment is inaccessible for FrameworkModel");
    }

    @Override
    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return super.checkIfClassLoaderCanRemoved(classLoader) &&
            applicationModels.stream().noneMatch(applicationModel -> applicationModel.containsClassLoader(classLoader));
    }
}
