package com.tqxd.jys.servicebus.service.collectors;

import com.tqxd.jys.servicebus.payload.CollectorStatusDto;
import com.tqxd.jys.servicebus.service.ServiceAddress;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.List;


@ProxyGen
public interface CollectorOpenApi {

    static CollectorOpenApi createProxy(Vertx vertx) {
        return new CollectorOpenApiVertxEBProxy(vertx, ServiceAddress.COLLECTOR.name());
    }

    /**
     * 获取所有收集器状态
     *
     * @param handler 处理器
     */
    void listCollector(Handler<AsyncResult<List<CollectorStatusDto>>> handler);

    /**
     * 部署一个收集器
     *
     * @param collectorName 收集器名称
     * @param configJson    收集器配置
     * @param handler       结果处理器
     */
    void deployCollectorEx(String collectorName, String configJson,
                           Handler<AsyncResult<Boolean>> handler);

    /**
     * 部署一个收集器
     *
     * @param collectorName 收集器名称
     * @param handler       结果处理器
     */
    void deployCollector(String collectorName,
                         Handler<AsyncResult<Boolean>> handler);

    /**
     * 取消部署一个收集器
     *
     * @param collectorName 收集器名称
     * @param handler       结果处理器
     */
    void unDeployCollector(String collectorName,
                           Handler<AsyncResult<Boolean>> handler);

    /**
     * 启动收集器
     *
     * @param collectorName 收集器名称
     * @param handler       结果处理器
     */
    void startCollector(String collectorName,
                        Handler<AsyncResult<Boolean>> handler);

    /**
     * 停止收集器
     *
     * @param collectorName 收集器名称
     * @param handler       结果处理器
     */
    void stopCollector(String collectorName,
                       Handler<AsyncResult<Boolean>> handler);

    /**
     * 订阅交易对
     *
     * @param collectorName 收集器名称
     * @param symbol        交易对
     * @param handler       结果处理器
     */
    void subscribe(String collectorName, String symbol,
                   Handler<AsyncResult<Boolean>> handler);

    /**
     * 取消订阅交易对
     *
     * @param collectorName 收集器名称
     * @param symbol        交易对
     * @param handler       结果处理器
     */
    void unsubscribe(String collectorName, String symbol,
                     Handler<AsyncResult<Boolean>> handler);
}

