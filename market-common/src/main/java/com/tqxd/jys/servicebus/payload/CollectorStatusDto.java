package com.tqxd.jys.servicebus.payload;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.List;

@DataObject
public class CollectorStatusDto {

    public CollectorStatusDto() {
    }

    public CollectorStatusDto(JsonObject object) {
        CollectorStatusDto t = object.mapTo(CollectorStatusDto.class);
        this.name = t.name;
        this.desc = t.desc;
        this.isDeployed = t.isDeployed;
        this.isRunning = t.isRunning;
        this.subscribedSymbols = t.subscribedSymbols;
    }

    /**
     * 收集器名称
     */
    private String name;

    /**
     * 收集器描述
     */
    private String desc;

    /**
     * 是否正在运行
     */
    private boolean isRunning;

    /**
     * 是否已经部署
     */
    private boolean isDeployed;

    /**
     * 已经订阅的交易对
     */
    private List<String> subscribedSymbols;

    public JsonObject toJson() {
        return JsonObject.mapFrom(this);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }

    public boolean isDeployed() {
        return isDeployed;
    }

    public void setDeployed(boolean deployed) {
        isDeployed = deployed;
    }

    public List<String> getSubscribedSymbols() {
        return subscribedSymbols;
    }

    public void setSubscribedSymbols(List<String> subscribedSymbols) {
        this.subscribedSymbols = subscribedSymbols;
    }
}
