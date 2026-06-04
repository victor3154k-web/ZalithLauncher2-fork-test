/*
 * Zalith Launcher 2
 * Copyright (C) 2025 MovTery <movtery228@qq.com> and contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/gpl-3.0.txt>.
 */

package com.movtery.zalithlauncher.utils.network

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import com.movtery.zalithlauncher.utils.logging.Logger
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

private const val TAG = "AdaptiveCoordinator"

/** 自适应并发的硬上限 */
private const val HARD_CONCURRENCY_CAP = 64

/**
 * 自适应下载协调器，根据网络质量和下载成功率动态调整并发数
 * @param maxConcurrency 最大并发数
 * @param minConcurrency 最小并发数
 * @param failureThreshold 连续失败阈值，超过此值后降级并发
 * @param successThreshold 连续成功阈值，超过此值后允许扩容
 */
class AdaptiveDownloadCoordinator(
    private val context: Context,
    maxConcurrency: Int = 64,
    private val minConcurrency: Int = 2,
    private val failureThreshold: Int = 3,
    private val successThreshold: Int = 5,
) {
    private val maxConcurrency: Int = maxConcurrency.coerceIn(minConcurrency, HARD_CONCURRENCY_CAP)

    /**
     * 并发阶段实际配置
     * (并发比例, 阈值倍数) 两者均相对于 [maxConcurrency]
     */
    private val concurrencyTemplates = listOf(
        0.125   to 0.3,
        0.1875  to 1.5,
        0.25    to 2.5,
        0.375   to 4.0,
        0.5625  to 6.0,
        0.75    to 8.0,
        1.0     to 10.0,
    )

    /** 阶段性并发配置(扩容阈值任务数, 并发数) */
    private val phaseConfig: List<Pair<Int, Int>> = buildPhaseConfig()

    @Volatile
    private var currentPhase = 0

    /** 当前阶段已完成的任务数 */
    private val tasksInPhase = AtomicInteger(0)

    /** 连续成功计数 */
    private val consecutiveSuccesses = AtomicInteger(0)

    /** 连续失败计数 */
    private val consecutiveFailures = AtomicInteger(0)

    /** 总下载字节数 */
    private val totalBytes = AtomicLong(0L)

    @Volatile
    private var semaphore = Semaphore(phaseConfig[0].second)

    /** 当前并发数 */
    @Volatile
    var currentConcurrency: Int = phaseConfig[0].second
        private set

    suspend fun withPermit(block: suspend () -> Unit) {
        //获取当前 Semaphore 的快照
        val s = semaphore
        s.withPermit {
            try {
                block()
            } finally {
                //无论成功失败都计为阶段完成一次
                tasksInPhase.incrementAndGet()
            }
        }
    }

    /**
     * 通知一个文件下载成功
     */
    fun onSuccess(bytes: Long) {
        totalBytes.addAndGet(bytes)
        consecutiveSuccesses.incrementAndGet()
        consecutiveFailures.set(0)
        tryUpgrade()
    }

    /**
     * 通知一个文件下载失败
     */
    fun onFailure() {
        consecutiveFailures.incrementAndGet()
        consecutiveSuccesses.set(0)
        tryDowngrade()
    }

    /**
     * 尝试扩容
     * 连续成功达到阈值且尚未到达最大并发时，进入下一阶段
     */
    private fun tryUpgrade() {
        if (consecutiveSuccesses.get() >= successThreshold) {
            val nextPhase = currentPhase + 1
            if (nextPhase < phaseConfig.size) {
                val (threshold, newConcurrency) = phaseConfig[nextPhase]
                val currentCompleted = tasksInPhase.get()
                if (currentCompleted >= threshold) {
                    //完成足够多的任务时进入下一阶段
                    upgradeTo(nextPhase, newConcurrency)
                }
            }
        }
    }

    /**
     * 尝试降级
     * 连续失败过多时回退到更小的并发数
     */
    private fun tryDowngrade() {
        if (consecutiveFailures.get() >= failureThreshold && currentPhase > 0) {
            val prevPhase = (currentPhase - 1).coerceAtLeast(0)
            val (_, newConcurrency) = phaseConfig[prevPhase]
            if (newConcurrency < currentConcurrency) {
                upgradeTo(prevPhase, newConcurrency)
                Logger.warning(TAG, "Downgrading concurrency: $currentConcurrency -> $newConcurrency (consecutive failures: ${consecutiveFailures.get()})")
            }
            consecutiveFailures.set(0)
        }
    }

    /**
     * 热替换 Semaphore，实现并发数调整。
     * 注意：不等待旧 Semaphore 的许可释放，新旧许可可能短暂叠加。
     */
    private fun upgradeTo(phase: Int, newConcurrency: Int) {
        currentPhase = phase
        currentConcurrency = newConcurrency
        tasksInPhase.set(0)
        consecutiveSuccesses.set(0)
        // 创建新的 Semaphore，旧许可随任务完成自动归还
        semaphore = Semaphore(newConcurrency)
        Logger.info(TAG, "Concurrency adjusted to $newConcurrency (phase $phase)")
    }

    /**
     * 根据当前网络类型决定初始并发数
     */
    private fun initialConcurrency(): Int {
        val caps = runCatching {
            val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
            cm?.getNetworkCapabilities(cm.activeNetwork)
        }.getOrNull()

        return when {
            caps == null -> minConcurrency
            caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) ||
            caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> 4
            //使用流量，从最小并发数起步
            caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> minConcurrency
            else -> minConcurrency
        }
    }

    /**
     * 根据 [maxConcurrency] 动态生成阶段配置：(扩容所需任务数, 并发数)
     */
    private fun buildPhaseConfig(): List<Pair<Int, Int>> {
        val init = initialConcurrency()
        val max = maxConcurrency

        val phases = mutableListOf(0 to init)
        var lastConcurrency = init

        for ((fraction, thresholdMultiplier) in concurrencyTemplates) {
            val concurrency = (max * fraction).toInt()
            //部分比例取整后与前阶段相同，直接跳过
            if (concurrency <= lastConcurrency) continue

            val threshold = (max * thresholdMultiplier).toInt()
            phases.add(threshold to concurrency)
            lastConcurrency = concurrency
        }

        return phases
    }
}
