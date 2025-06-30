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
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    // 最少活跃调用数策略，在加权随机的基础上，增加正在处理请求数的判断，获取每个 invoker 的正在处理请求数，仅对最小请求数的一个或多个 invoker 进行RandomLoadBalance策略
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        // 集群invoker个数
        int length = invokers.size();
        // The least active value of all invokers
        // 最少调用数
        int leastActive = -1;
        // The number of invokers having the same least active value (leastActive)
        // 最少调用数的invoker数量
        int leastCount = 0;
        // The index of invokers having the same least active value (leastActive)
        // 最少调用数的invoker数组
        int[] leastIndexes = new int[length];
        // the weight of every invokers
        // 每个invoker的权重
        int[] weights = new int[length];
        // The sum of the warmup weights of all the least active invokers
        // 权重总和，和之前不同的是，这里只求和最少调用数的invoker的权重
        int totalWeight = 0;
        // The weight of the first least active invoker
        // 最少调用数起始权重
        int firstWeight = 0;
        // Every least active invoker has the same weight value?
        // 是否最少调用数的invoker都是一样的权重
        boolean sameWeight = true;


        // Filter out all the least active invokers
        // 遍历invokers，找出最少调用数的所有invoker
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // Get the active number of the invoker
            // 获取方法的调用数
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            // Get the weight of the invoker's configuration. The default value is 100.
            // AbstractLoadBalance中获取invoker权重的逻辑
            int afterWarmup = getWeight(invoker, invocation);
            // save for later use
            // 保存权重
            weights[i] = afterWarmup;
            // If it is the first invoker or the active number of the invoker is less than the current least active number
            // 首个invoker或者小于之前的调用数
            if (leastActive == -1 || active < leastActive) {
                // Reset the active number of the current invoker to the least active number
                // 因为是首个，或者发现了更小调用数，下面都是重置类型的操作
                // 重置最少调用数
                leastActive = active;
                // Reset the number of least active invokers
                // 重置最少调用数的invoker数量
                leastCount = 1;
                // Put the first least active invoker first in leastIndexes
                // 把当前invoker放入最少调用数数组第一位
                leastIndexes[0] = i;
                // Reset totalWeight
                // 重置最少调用数求和权重
                totalWeight = afterWarmup;
                // Record the weight the first least active invoker
                // 重置起始权重
                firstWeight = afterWarmup;
                // Each invoke has the same weight (only one invoker here)
                // 因为只有一个，所以重置为true
                sameWeight = true;
                // If current invoker's active value equals with leaseActive, then accumulating.
                // 下面是发现了相同调用数时候发生的事情
            } else if (active == leastActive) {
                // Record the index of the least active invoker in leastIndexes order
                // 在最少调用数数组中记录invoker
                leastIndexes[leastCount++] = i;
                // Accumulate the total weight of the least active invoker
                // 求和最少调用数权重
                totalWeight += afterWarmup;
                // If every invoker has the same weight?
                // 对比之前的权重，如果不一样，sameWeight置为false，这里的sameWeight和RandomLoadBalance用处一样
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // Choose an invoker from all the least active invokers
        // 如果最少调用数的invoker只有一个，那么选择这个
        if (leastCount == 1) {
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexes[0]);
        }

        // 下面的逻辑和RandomLoadBalance基本一致
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on 
            // totalWeight.
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
