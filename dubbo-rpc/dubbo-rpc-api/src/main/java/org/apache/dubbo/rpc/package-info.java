package org.apache.dubbo.rpc;

/**
 * filter 包：在进行服务引用时会进行一系列的过滤，其中包括了很多过滤器。
 * listener 包：在服务发布和服务引用的过程中，我们可以添加一些 Listener 来监听相应的事件，与 Listener 相关的接口 Adapter、Wrapper 实现就在这个包内。
 * protocol 包：一些实现了 Protocol 接口以及 Invoker 接口的抽象类位于该包之中，它们主要是为 Protocol 接口的具体实现以及 Invoker 接口的具体实现提供一些公共逻辑。
 * proxy 包：提供了创建代理的能力，在这个包中支持 JDK 动态代理以及 Javassist 字节码两种方式生成本地代理类。
 * support 包：包括了 RpcUtils 工具类、Mock 相关的 Protocol 实现以及 Invoker 实现。
 */