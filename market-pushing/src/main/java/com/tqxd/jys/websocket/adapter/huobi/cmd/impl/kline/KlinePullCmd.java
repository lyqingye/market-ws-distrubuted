package com.tqxd.jys.websocket.adapter.huobi.cmd.impl.kline;


import com.tqxd.jys.utils.HuoBiUtils;
import com.tqxd.jys.websocket.adapter.Cmd;
import com.tqxd.jys.websocket.context.PushingContext;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.json.JsonObject;


/**
 * @author yjt
 * @since 2020/11/14 18:27
 */
public class KlinePullCmd implements Cmd {

  @Override
  public boolean canExecute(JsonObject json) {
    return HuoBiUtils.isPullHistoryReq(json);
  }

  @Override
  public void execute(JsonObject json, PushingContext ctx, WsSession curSession) {
//        HBPullKineReq req = json.mapTo(HBPullKineReq.class);
//        // 拉取历史消息也算是订阅 ps: 前端组件问题
//        curSession.subKline(req.getReq());
//        // 根据订阅内容从时间轮获取k线数据历史数据
//        KlineTimeLine timeWheel = ctx.getTickTimeWheelCache().get(req.getReq());
//        Collection<KlineTickResp> history = Collections.emptyList();
//        if (timeWheel != null) {
//            history = timeWheel.pull(req.getFrom() * 1000,
//                    req.getTo() * 1000,
//                    req.getPartIdx());
//        }
//        // 转换为json二进制
//        Buffer jsonBuffer = Json.encodeToBuffer(HBPullKlineTemplate.ok(req.getId(), req.getReq(), history));
//        // 异步压缩
//        GZIPUtils.compressAsync(ctx.getVertx(),jsonBuffer)
//            .onSuccess(compressed -> curSession.getSocket().write(compressed))
//            .onFailure(Throwable::printStackTrace);
  }
}
