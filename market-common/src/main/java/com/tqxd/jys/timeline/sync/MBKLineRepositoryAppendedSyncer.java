package com.tqxd.jys.timeline.sync;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.messagebus.MessageListener;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.utils.ChannelUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.jackson.JacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 将消息总线数据同步到指定仓库
 *
 * @author lyqingye
 */
public class MBKLineRepositoryAppendedSyncer implements KLineRepositoryAppendedSyncer, MessageListener {
  private static final Logger log = LoggerFactory.getLogger(MBKLineRepositoryAppendedSyncer.class);
  private KLineRepository kLineRepository;

  @Override
  public void syncAppendedTo(KLineRepository target, Handler<AsyncResult<Void>> handler) {
    this.kLineRepository = Objects.requireNonNull(target);
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void onMessage(Message<?> msg) {
    if (msg.getType() == DataType.KLINE) {
      TemplatePayload<KlineTick> payload = JacksonCodec.decodeValue((String) msg.getPayload(), new TypeReference<TemplatePayload<KlineTick>>() {
      });
      ChannelUtil.KLineChannel ch = ChannelUtil.resolveKLineCh(payload.getCh());
      if (ch == null) {
        log.error("invalid message from Kline topic, invalid channel string! message: {}", msg);
      } else {
        kLineRepository.append(msg.getIndex(), ChannelUtil.getSymbol(payload.getCh()), ch.getPeriod(), payload.getTick())
            .onFailure(Throwable::printStackTrace);
      }
    } else {
      log.error("invalid message type from Kline topic! message: {}", msg);
    }
  }
}
