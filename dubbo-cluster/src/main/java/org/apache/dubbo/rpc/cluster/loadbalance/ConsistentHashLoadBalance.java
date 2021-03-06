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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.support.RpcUtils;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;

/**
 * ConsistentHashLoadBalance
 * 一致性 Hash 算法的原理也是取模算法，与 Hash 取模的不同之处在于：Hash 取模是对 Provider 节点数量取模，
 * 而一致性 Hash 算法是对 2^32 取模
 * @link https://kaiwu.lagou.com/course/courseInfo.htm?courseId=393#/detail/pc?id=5363
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "consistenthash";

    /**
     * Hash nodes name
     */
    public static final String HASH_NODES = "hash.nodes";

    /**
     * Hash arguments name
     */
    public static final String HASH_ARGUMENTS = "hash.arguments";

    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 获取调用的方法名称
        String methodName = RpcUtils.getMethodName(invocation);
        // 将ServiceKey和方法拼接起来，构成一个key
        String key = invokers.get(0).getUrl().getServiceKey() + "." + methodName;
        // using the hashcode of list to compute the hash only pay attention to the elements in the list
        // 注意：这是为了在invokers列表发生变化时都会重新生成ConsistentHashSelector对象
        int invokersHashCode = invokers.hashCode();
        // 根据key获取对应的ConsistentHashSelector对象，selectors是一个ConcurrentMap<String, ConsistentHashSelector>集合
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        // 未查找到ConsistentHashSelector对象，则进行创建
        if (selector == null || selector.identityHashCode != invokersHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, methodName, invokersHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        // 通过ConsistentHashSelector对象选择一个Invoker对象
        return selector.select(invocation);
    }

    private static final class ConsistentHashSelector<T> {
        /**
         * 用于记录虚拟 Invoker 对象的 Hash 环。这里使用 TreeMap 实现 Hash 环，并将虚拟的 Invoker 对象分布在 Hash 环上。
         */
        private final TreeMap<Long, Invoker<T>> virtualInvokers;
        /**
         * 虚拟 Invoker 个数
         */
        private final int replicaNumber;
        /**
         * Invoker 集合的 HashCode 值
         */
        private final int identityHashCode;
        /**
         * 需要参与 Hash 计算的参数索引
         */
        private final int[] argumentIndex;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            // 初始化virtualInvokers字段，也就是虚拟Hash槽
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            // 记录Invoker集合的hashCode，用该hashCode值来判断Provider列表是否发生了变化
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            // 从hash.nodes参数中获取虚拟节点的个数
            this.replicaNumber = url.getMethodParameter(methodName, HASH_NODES, 160);
            // 获取参与Hash计算的参数下标值，默认对第一个参数进行Hash运算
            String[] index = COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, HASH_ARGUMENTS, "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            // 构建虚拟Hash槽，默认replicaNumber=160，相当于在Hash槽上放160个槽位
            // 外层轮询40次，内层轮询4次，共40*4=160次，也就是同一节点虚拟出160个槽位
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 对address + i进行md5运算，得到一个长度为16的字节数组
                    byte[] digest = Bytes.getMD5(address + i);
                    // 对digest部分字节进行4次Hash运算，得到4个不同的long型正整数
                    for (int h = 0; h < 4; h++) {
                        // h = 0 时，取 digest 中下标为 0~3 的 4 个字节进行位运算
                        // h = 1 时，取 digest 中下标为 4~7 的 4 个字节进行位运算
                        // h = 2 和 h = 3时，过程同上
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        public Invoker<T> select(Invocation invocation) {
            // 将参与一致性Hash的参数拼接到一起
            String key = toKey(invocation.getArguments());
            // 计算key的Hash值
            byte[] digest = Bytes.getMD5(key);
            // 匹配Invoker对象
            return selectForKey(hash(digest, 0));
        }

        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        private Invoker<T> selectForKey(long hash) {
            // 从virtualInvokers集合（TreeMap是按照Key排序的）中查找第一个节点值大于或等于传入Hash值的Invoker对象
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.ceilingEntry(hash);
            // 如果Hash值大于Hash环中的所有Invoker，则回到Hash环的开头，返回第一个Invoker对象
            if (entry == null) {
                entry = virtualInvokers.firstEntry();
            }
            return entry.getValue();
        }

        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }
    }

}
