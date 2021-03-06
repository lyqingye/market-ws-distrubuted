package com.tqxd.jys.websocket;

import com.tqxd.jys.bootstrap.Bootstrap;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.timeline.InMemKLineRepository;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.KLineRepositoryAdapter;
import com.tqxd.jys.timeline.sync.MBKLineRepositoryAppendedSyncer;
import com.tqxd.jys.websocket.cache.CacheManager;
import com.tqxd.jys.websocket.processor.impl.KLineChannelProcessor;
import com.tqxd.jys.websocket.processor.impl.MarketDepthChannelProcessor;
import com.tqxd.jys.websocket.processor.impl.MarketDetailChannelProcessor;
import com.tqxd.jys.websocket.processor.impl.TradeDetailChannelProcessor;
import com.tqxd.jys.websocket.session.SessionManager;
import com.tqxd.jys.websocket.transport.RequestDispatcher;
import com.tqxd.jys.websocket.transport.ServerEndpointVerticle;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class PushingApplication extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(PushingApplication.class);
  private KLineRepository kLineRepository;
  private CacheManager cacheManager;
  private String serverEndpointDeployId;

  public static void main(String[] args) {
    Bootstrap.run(new PushingApplication(), new DeploymentOptions().setWorker(true));
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    long start = System.currentTimeMillis();
    CompositeFuture.join(initKLineRepository(),initCacheManager(),initTransport())
      .onSuccess(none -> {
        log.info("[PushingApplication]: start success! using {}ms",System.currentTimeMillis() - start);
      })
      .onFailure(throwable -> {
        log.error("[PushingApplication]: start fail! system will be exit! cause by: {}", throwable.getMessage());
        throwable.printStackTrace();
        System.exit(-1);
      }
    );
  }

  private Future<Void> initKLineRepository () {
    KLineRepositoryAdapter remote = new KLineRepositoryAdapter();
    kLineRepository = new InMemKLineRepository();
    MBKLineRepositoryAppendedSyncer syncer = new MBKLineRepositoryAppendedSyncer();
    return remote.open(vertx, config())
      .compose(none -> kLineRepository.open(vertx, config()))
      .compose(none -> kLineRepository.importFrom(remote))
      // ???k??????????????????k??????????????????
      .compose(none -> MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC, syncer))
      // ???????????????k?????????
      .compose(none -> syncer.syncAppendTo(kLineRepository));
  }

  private Future<Void> initCacheManager () {
    cacheManager = new CacheManager(kLineRepository);
    return MessageBusFactory.bus().subscribe(Topic.TRADE_DETAIL_TOPIC, cacheManager)
      .compose(none -> MessageBusFactory.bus().subscribe(Topic.DEPTH_CHART_TOPIC, cacheManager))
      .map(toVoid -> null);
  }

  private Future<Void> initTransport () {
    RequestDispatcher dispatcher = RequestDispatcher.getInstance();
    SessionManager sessionMgr = SessionManager.getInstance(config());
    // k??????????????????
    KLineChannelProcessor kLineChannelProcessor = new KLineChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addWatcher(kLineChannelProcessor);
    dispatcher.addProcessor(kLineChannelProcessor);

    // ???????????????????????????
    TradeDetailChannelProcessor tradeDetailChannelProcessor = new TradeDetailChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addWatcher(tradeDetailChannelProcessor);
    dispatcher.addProcessor(tradeDetailChannelProcessor);

    // ???????????????????????????
    MarketDetailChannelProcessor marketDetailChannelProcessor = new MarketDetailChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addWatcher(marketDetailChannelProcessor);
    dispatcher.addProcessor(marketDetailChannelProcessor);

    // ???????????????????????????
    MarketDepthChannelProcessor marketDepthChannelProcessor = new MarketDepthChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addWatcher(marketDepthChannelProcessor);
    dispatcher.addProcessor(marketDepthChannelProcessor);

    return vertx.deployVerticle(ServerEndpointVerticle.class, new DeploymentOptions().setWorker(true).setConfig(config()).setInstances(Runtime.getRuntime().availableProcessors()))
        .onSuccess(deploymentId -> {
          this.serverEndpointDeployId = deploymentId;
        })
        .map(toVoid -> null);
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    vertx.undeploy(this.serverEndpointDeployId, stopPromise);
  }
}
