package com.tqxd.jys.core.kline.sync;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.core.kline.KLineRepository;
import com.tqxd.jys.core.message.TemplatePayload;
import com.tqxd.jys.core.message.channel.ChannelUtil;
import com.tqxd.jys.core.message.kline.KlineTick;
import com.tqxd.jys.core.message.kline.Period;
import com.tqxd.jys.core.spi.DataType;
import com.tqxd.jys.core.spi.Message;
import com.tqxd.jys.core.spi.MessageListener;
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
      kLineRepository.append(msg.getIndex(), ChannelUtil.getSymbol(payload.getCh()), Period._1_MIN, payload.getTick())
        .onFailure(Throwable::printStackTrace);
    } else {
      log.error("[RepositoryApplication]: invalid message type from Kline topic! message: {}", msg);
    }
  }
}
