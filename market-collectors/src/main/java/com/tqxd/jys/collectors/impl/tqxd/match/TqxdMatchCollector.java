package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.tqxd.TqxdBaseCollector;

public class TqxdMatchCollector extends TqxdBaseCollector {

    @Override
    public String name() {
        return TqxdMatchCollector.class.getSimpleName();
    }

    @Override
    public String desc() {
        return "tqxd撮合引擎数据采集";
    }
}
