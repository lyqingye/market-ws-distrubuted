package com.tqxd.jys.websocket.adapter;

import com.tqxd.jys.utils.HuoBiUtils;
import com.tqxd.jys.websocket.adapter.huobi.cmd.impl.depth.DepthProcessCmd;
import com.tqxd.jys.websocket.adapter.huobi.cmd.impl.depth.DepthSubCmd;
import com.tqxd.jys.websocket.adapter.huobi.cmd.impl.detail.MarketDetailSubCmd;
import com.tqxd.jys.websocket.adapter.huobi.cmd.impl.kline.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author yjt
 * @since 2020/11/14 18:21
 */
public class CmdFactory {

    private static Cmd[] WS_CMD_ARRAY;

    private static Cmd[] TOPIC_CMD_ARRAY;

    static {
        WS_CMD_ARRAY = new Cmd[]{
                new KlinePullCmd(),
                new KlineSubCmd(),
                new TradeDetailSubCmd(),
                new DepthSubCmd(),
                new MarketDetailSubCmd()
        };

        TOPIC_CMD_ARRAY = new Cmd[]{
                new KlineTickProcessCmd(),
                new TradeDetailProcessCmd(),
                new DepthProcessCmd()
        };
    }

    public static Cmd createForWs(JsonObject json) {
        // 忽略心跳
        if (!HuoBiUtils.isPongReq(json)) {
            for (Cmd cmd : WS_CMD_ARRAY) {
                if (cmd.canExecute(json)) {
                    return cmd;
                }
            }
        }
        return null;
    }

    public static Cmd createForWs(JsonArray jsonArray) {
        for (Cmd cmd : WS_CMD_ARRAY) {
            if (cmd.canExecute(jsonArray)) {
                return cmd;
            }
        }
        return null;
    }

    public static Cmd createForTopic(JsonObject json) {
        for (Cmd cmd : TOPIC_CMD_ARRAY) {
            if (cmd.canExecute(json)) {
                return cmd;
            }
        }
        return null;
    }

    public static Cmd createForTopic(JsonArray jsonArray) {
        for (Cmd cmd : TOPIC_CMD_ARRAY) {
            if (cmd.canExecute(jsonArray)) {
                return cmd;
            }
        }
        return null;
    }
}
