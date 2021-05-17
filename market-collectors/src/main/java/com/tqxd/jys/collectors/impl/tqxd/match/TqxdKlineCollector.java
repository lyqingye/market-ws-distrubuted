package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdRequestUtils;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.utils.ChannelUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public class TqxdKlineCollector extends GenericWsCollector {
  public static final String PERIOD_CONFIG = "period";
  private static final int TIME_INDEX = 0;
  private static final int OPEN_INDEX = 1;
  private static final int CLOSE_INDEX = 1;
  private static final int HIGH_INDEX = 1;
  private static final int LOW_INDEX = 1;
  private static final int VOL_INDEX = 1;
  private static final int AMOUNT_INDEX = 1;

  private static final Logger log = LoggerFactory.getLogger(TqxdKlineCollector.class);
  private JsonObject config;

  public TqxdKlineCollector(JsonObject config) {
    this.config = config;
  }

  @Override
  public String name() {
    return TqxdKlineCollector.class.getSimpleName();
  }

  @Override
  public String desc() {
    return "天启旭达k线收集器";
  }

  @Override
  public JsonObject config() {
    return config;
  }

  @Override
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    Promise<Void> promise = Promise.promise();
    super.start(promise);
    promise.future()
        .compose(none -> this.subscribe(DataType.KLINE, config.getString(TqxdCollector.SYMBOL_CONFIG)))
        .onComplete(startPromise);
  }

  @Override
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          Period period = Period.valueOfName(config.getString(PERIOD_CONFIG));
          if (period != null) {
            super.writeText(TqxdRequestUtils.buildSubscribeKLineReq(System.currentTimeMillis() / 1000, TqxdRequestUtils.toTqxdSymbol(symbol), period));
          }
          handler.handle(Future.succeededFuture());
        })
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    if (frame.isText() && frame.isFinal()) {
      JsonObject obj = (JsonObject) Json.decodeValue(frame.textData());
      if (TqxdRequestUtils.KLINE_UPDATE_RESP.equalsIgnoreCase(obj.getString(TqxdRequestUtils.METHOD))) {
        JsonArray params = obj.getJsonArray(TqxdRequestUtils.PARAMS);
        for (Object param : params.getList()) {
          if (param instanceof List) {
            List row = (List) param;
            KlineTick tick = new KlineTick();
            tick.setId(Long.valueOf((Integer) row.get(TIME_INDEX)));
            tick.setOpen(new BigDecimal((String) row.get(OPEN_INDEX)));
            tick.setClose(new BigDecimal((String) row.get(CLOSE_INDEX)));
            tick.setHigh(new BigDecimal((String) row.get(HIGH_INDEX)));
            tick.setLow(new BigDecimal((String) row.get(LOW_INDEX)));
            tick.setVol(new BigDecimal((String) row.get(VOL_INDEX)));
            tick.setAmount(new BigDecimal((String) row.get(AMOUNT_INDEX)));
            // TODO 数据问题
            tick.setCount(1);
            String ch = ChannelUtil.buildKLineTickChannel(config.getString(TqxdCollector.SYMBOL_CONFIG), Objects.requireNonNull(Period.valueOfName(config.getString(PERIOD_CONFIG))));
            TemplatePayload<KlineTick> payload = TemplatePayload.of(ch, tick);
            unParkReceives(DataType.KLINE, JsonObject.mapFrom(payload));
          }
        }
      } else {
        log.warn("[TqxdKLine]-{}-{}: receive ignored data is: {}", config.getString(TqxdCollector.SYMBOL_CONFIG), config.getString(PERIOD_CONFIG), frame.textData());
      }
    }
  }
}
