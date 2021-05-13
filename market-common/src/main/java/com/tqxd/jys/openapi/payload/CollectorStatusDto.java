package com.tqxd.jys.openapi.payload;

import com.tqxd.jys.constance.DataType;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;

@DataObject
public class CollectorStatusDto {

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

  private List<String> klineSubscribes;
  private List<String> depthSubscribes;
  private List<String> tradeDetailSubscribes;


  public CollectorStatusDto() {
  }

  public CollectorStatusDto(JsonObject object) {
    CollectorStatusDto t = object.mapTo(CollectorStatusDto.class);
    this.name = t.name;
    this.desc = t.desc;
    this.isRunning = t.isRunning;
    this.klineSubscribes = t.klineSubscribes;
    this.depthSubscribes = t.depthSubscribes;
    this.tradeDetailSubscribes = t.tradeDetailSubscribes;
  }

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

  public List<String> getKlineSubscribes() {
    return klineSubscribes;
  }

  public void setKlineSubscribes(List<String> klineSubscribes) {
    this.klineSubscribes = klineSubscribes;
  }

  public List<String> getDepthSubscribes() {
    return depthSubscribes;
  }

  public void setDepthSubscribes(List<String> depthSubscribes) {
    this.depthSubscribes = depthSubscribes;
  }

  public List<String> getTradeDetailSubscribes() {
    return tradeDetailSubscribes;
  }

  public void setTradeDetailSubscribes(List<String> tradeDetailSubscribes) {
    this.tradeDetailSubscribes = tradeDetailSubscribes;
  }

  public void setSubscribedInfo(Map<DataType, List<String>> subscribedInfo) {
    klineSubscribes = subscribedInfo.get(DataType.KLINE);
    depthSubscribes = subscribedInfo.get(DataType.DEPTH);
    tradeDetailSubscribes = subscribedInfo.get(DataType.TRADE_DETAIL);
  }
}
