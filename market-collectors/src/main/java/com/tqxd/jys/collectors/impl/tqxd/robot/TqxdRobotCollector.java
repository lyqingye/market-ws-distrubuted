package com.tqxd.jys.collectors.impl.tqxd.robot;

import com.tqxd.jys.collectors.impl.tqxd.TqxdBaseCollector;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdMatchCollector;

public class TqxdRobotCollector extends TqxdBaseCollector {

    @Override
    public String name() {
        return TqxdRobotCollector.class.getSimpleName();
    }

    @Override
    public String desc() {
        return "tqxd机器人收集器";
    }
}
