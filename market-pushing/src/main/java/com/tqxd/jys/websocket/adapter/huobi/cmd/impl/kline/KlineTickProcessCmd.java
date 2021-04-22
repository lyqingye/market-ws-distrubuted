package com.tqxd.jys.websocket.adapter.huobi.cmd.impl.kline;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.utils.HuoBiUtils;
import com.tqxd.jys.utils.VertxUtil;
import com.tqxd.jys.websocket.adapter.Cmd;
import com.tqxd.jys.websocket.context.PushingContext;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;


public class KlineTickProcessCmd implements Cmd {
    @Override
    public boolean canExecute(JsonObject json) {
        String ch = json.getString("ch");
        if (ch == null) {
            return false;
        }
        return ch.contains(".kline.");
    }

    @Override
    public void execute(JsonObject json, PushingContext ctx, WsSession curSession) {
        String sub = json.getString("ch");
        String symbol = HuoBiUtils.getSymbolFromKlineSub(sub);
        if (symbol == null) {
            return;
        }
        KlineTick tick = json.getJsonObject("tick").mapTo(KlineTick.class);
        // 更新到缓存后并推送
//        this.broadcastTick(this.updateCache(ctx, symbol, tick), ctx);

        // 生成市场概要并且推送
//        this.broadcastDetail(ctx, symbol, ctx.updateMarketDetailTick(symbol));
    }

    /**
     * 更新时间轮并且返回更新后的数据
     *
     * @param ctx    上下文
     * @param symbol 交易对
     * @param tick   tick
     * @return 更新后的数据
     */
    private Map<String, Buffer> updateCache(PushingContext ctx, String symbol,
                                            KlineTick tick) {
        Map<String, Buffer> updated = new HashMap<>(Period.values().length);
//        for (Period period : Period.values()) {
//            TimeWheel<KlineTick> wheel = ctx.getOrCreateTimeWheel(symbol, period);
//            String key = HuoBiUtils.toKlineSub(symbol, period);
//            Buffer jsonBuffer = Json.encodeToBuffer(new KlineTick(key, wheel.updateNow(tick).clone()));
//            try {
//                updated.put(key, Buffer.buffer(GZIPUtils.compress(jsonBuffer.getBytes())));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        return updated;
    }

    /**
     * 广播k线数据
     *
     * @param updateData 需要广播的数据
     */
    private void broadcastTick(Map<String, Buffer> updateData,
                               PushingContext ctx) {
        VertxUtil.asyncFastCallIgnoreRs(ctx.getVertx(), () -> {
            ctx.getKlineSM().values().forEach(wrapper -> {
                String required = wrapper.getKlineSub();
                if (required != null) {
                    Buffer buffer = updateData.get(required);
                    if (buffer != null) {
                        // 只向已经订阅了该交易对的
                        wrapper.getSocket().write(buffer);
                    }
                }
            });
        });
    }

    /**
     * 广播市场详情
     *
     * @param ctx    上下文
     * @param detail 细节
     */
    private void broadcastDetail(PushingContext ctx, String symbol, Buffer detail) {
        String sub = HuoBiUtils.toDetailSub(symbol);
        VertxUtil.asyncFastCallIgnoreRs(ctx.getVertx(), () -> {
            ctx.getDetailSM().values().forEach(wrapper -> {
                if (wrapper.isSubMarketDetail(sub)) {
                    wrapper.getSocket().write(detail);
                }
            });
        });
    }
}
