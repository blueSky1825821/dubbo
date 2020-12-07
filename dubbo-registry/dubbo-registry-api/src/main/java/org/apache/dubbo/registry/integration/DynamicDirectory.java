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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.SIMPLIFIED_KEY;
import static org.apache.dubbo.registry.integration.InterfaceCompatibleRegistryProtocol.DEFAULT_REGISTER_CONSUMER_KEYS;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;


/**
 * RegistryDirectory
 */
public abstract class DynamicDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(DynamicDirectory.class);

    protected static final Cluster CLUSTER = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    protected static final RouterFactory ROUTER_FACTORY = ExtensionLoader.getExtensionLoader(RouterFactory.class)
            .getAdaptiveExtension();
    //服务对应的 ServiceKey，默认是 {interface}:[group]:[version] 三部分构成。
    protected final String serviceKey; // Initialization at construction time, assertion not null
    //服务接口类型，例如，org.apache.dubbo.demo.DemoService
    protected final Class<T> serviceType; // Initialization at construction time, assertion not null
    //是否引用多个服务组
    protected final boolean multiGroup;
    //使用的 Protocol 实现
    protected Protocol protocol; // Initialization at the time of injection, the assertion is not null
    //使用的注册中心实现
    protected Registry registry; // Initialization at the time of injection, the assertion is not null
    protected volatile boolean forbidden = false;
    protected boolean shouldRegister;
    protected boolean shouldSimplified;

    protected volatile URL overrideConsumerUrl; // Initialization at construction time, assertion not null, and always assign non null value

    protected volatile URL registeredConsumerUrl;

    /**
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    //动态更新的配置信息
    protected volatile List<Configurator> configurators; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    // Map<url, Invoker> cache service url to invoker mapping.
    //Provider URL 与对应 Invoker 之间的映射，该集合会与 invokers 字段同时动态更新
    protected volatile Map<URL, Invoker<T>> urlInvokerMap; // The initial value is null and the midway may be assigned to null, please use the local variable reference
    //动态更新的 Invoker 集合
    protected volatile List<Invoker<T>> invokers;

    // Set<invokerUrls> cache invokeUrls to invokers mapping.
    //当前缓存的所有 Provider 的 URL，该集合会与 invokers 字段同时动态更新
    protected volatile Set<URL> cachedInvokerUrls; // The initial value is null and the midway may be assigned to null, please use the local variable reference

    protected ServiceInstancesChangedListener serviceListener;

    public DynamicDirectory(Class<T> serviceType, URL url) {
        // 传入的url参数是注册中心的URL，例如，
        // zookeeper://127.0.0.1:2181/org.apache.dubbo.registry.RegistryService?...，
        // 其中refer参数包含了Consumer信息，例如，refer=
        // application=dubbo-demo-api-consumer&dubbo=2.0.2&interface=org.apache.dubbo.demo.DemoService&pid=13423&register.ip=192.168.124.3&side=consumer(URLDecode之后的值)
        super(url);
        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }

        shouldRegister = !ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(REGISTER_KEY, true);
        shouldSimplified = url.getParameter(SIMPLIFIED_KEY, false);
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }
        this.serviceType = serviceType;
        this.serviceKey = super.getConsumerUrl().getServiceKey();
        // 解析refer参数值，得到其中Consumer的属性信息
        String group = queryMap.get(GROUP_KEY) != null ? queryMap.get(GROUP_KEY) : "";
        this.multiGroup = group != null && (ANY_VALUE.equals(group) || group.contains(","));
    }

    @Override
    public void addServiceListener(ServiceInstancesChangedListener instanceListener) {
        this.serviceListener = instanceListener;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public Registry getRegistry() {
        return registry;
    }

    public boolean isShouldRegister() {
        return shouldRegister;
    }

    public void subscribe(URL url) {
        setConsumerUrl(url);
        registry.subscribe(url, this);
    }

    public void unSubscribe(URL url) {
        setConsumerUrl(null);
        registry.unsubscribe(url, this);
    }

    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            // 1. No service provider 2. Service providers are disabled
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "No provider available from registry " +
                    getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +
                    NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() +
                    ", please check status of providers(disabled, not registered or in blacklist).");
        }

        if (multiGroup) {
            return this.invokers == null ? Collections.emptyList() : this.invokers;
        }

        List<Invoker<T>> invokers = null;
        try {
            // Get invokers from cache, only runtime routers will be executed.
            invokers = routerChain.route(getConsumerUrl(), invocation);
        } catch (Throwable t) {
            logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
        }

        return invokers == null ? Collections.emptyList() : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public List<Invoker<T>> getAllInvokers() {
        return invokers;
    }

    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    public void setRegisteredConsumerUrl(URL url) {
        if (!shouldSimplified) {
            this.registeredConsumerUrl = url.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY,
                    String.valueOf(false));
        } else {
            this.registeredConsumerUrl = URL.valueOf(url, DEFAULT_REGISTER_CONSUMER_KEYS, null).addParameters(
                    CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false));
        }
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<URL, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(url));
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<URL, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    @Override
    public void setConsumerUrl(URL consumerUrl) {
        this.consumerUrl = consumerUrl;
        this.overrideConsumerUrl = consumerUrl;
    }

}
