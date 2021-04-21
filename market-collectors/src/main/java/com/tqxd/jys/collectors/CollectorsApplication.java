package com.tqxd.jys.collectors;

import com.hazelcast.config.Config;
import com.tqxd.jys.collectors.api.CollectorOpenApiImpl;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.servicebus.payload.CollectorStatusDto;
import com.tqxd.jys.servicebus.service.ServiceAddress;
import com.tqxd.jys.servicebus.service.collectors.CollectorOpenApi;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yjt
 * @since 2020/10/10 下午3:45
 */
public class CollectorsApplication extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(CollectorsApplication.class);

    /**
     * expose service
     */
    private ServiceBinder serviceBinder;

    /**
     * consumer
     */
    private MessageConsumer<JsonObject> serviceConsumer;

    /**
     * 开放服务
     */
    private CollectorOpenApiImpl openService;

    public CollectorsApplication() {
    }

    @Override
    public void start(Promise<Void> promise) throws Exception {
        // 暴露服务
        serviceBinder = new ServiceBinder(vertx).setAddress(ServiceAddress.COLLECTOR.name());
        openService = new CollectorOpenApiImpl(vertx, MessageBusFactory.bus());

        if (vertx.isClustered()) {
            serviceConsumer = serviceBinder
                    .register(CollectorOpenApi.class, openService);
        } else {
            serviceConsumer = serviceBinder
                    .registerLocal(CollectorOpenApi.class, openService);
        }
        JsonObject config = config();
        String collectorName = VertxUtil.jsonGetValue(config, "market.collector.name", String.class);
        List<String> subscribe = VertxUtil.jsonListValue(config, "market.collector.subscribe", String.class);
        if (collectorName != null && !collectorName.isEmpty()) {
            Future<Boolean> future = openService.deployCollector(collectorName)
                                                 .compose(ignored -> openService.startCollector(collectorName));
            for (String subscribeSymbol : subscribe) {
                future = future.compose(ignored -> openService.subscribe(collectorName,subscribeSymbol));
            }
            future.onFailure(promise::fail);
            future.onSuccess(ignored -> {
                log.info("[Market-KlineCollector]: start success!");
                log.info("[Market-KlineCollector]: deploy collector: " + collectorName);
                log.info("[Market-KlineCollector]: subscribe: " + subscribe);
                promise.complete();
            });
        }
    }

    @Override
    public void stop() throws Exception {
        serviceBinder.unregister(serviceConsumer);
        // 停止所有收集器
        if (openService != null) {
            openService.listCollector(cr -> {
                if (cr.succeeded()) {
                    for (CollectorStatusDto collector : cr.result()) {
                        openService.stopCollector(collector.getName(), stopRs -> {
                            if (stopRs.failed()) {
                                stopRs.cause().printStackTrace();
                            }
                        });
                    }
                }
            });
        }
    }

    public static void main(String[] args) {
        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", "localhost:9092");
        kafkaConfig.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.put("group.id", "collector");
        kafkaConfig.put("auto.offset.reset", "earliest");
        kafkaConfig.put("enable.auto.commit", "true");

        Config hazelcastConfig = new Config();
        HazelcastClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
        VertxOptions options = new VertxOptions().setClusterManager(mgr);
        options.setClusterManager(mgr);
        Vertx.clusteredVertx(options, ar -> {
            if (ar.succeeded()) {
                MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS,ar.result(), kafkaConfig);
                ar.result().deployVerticle(new CollectorsApplication())
                        .onFailure(Throwable::printStackTrace);
            }else {
                ar.cause().printStackTrace();
            }
        });
    }
}
