package com.tqxd.jys.websocket.adapter.huobi.payload;

public class HBPullKlineTemplate<T> {
    /**
     * OK状态
     */
    public static final String RESPONSE_STATUS_OK = "ok";

    private String id;

    private String status;

    private String rep;

    /**
     * 返回的数据
     */
    private T data;

    public static <E> HBPullKlineTemplate<E> ok(String id, String rep, E data) {
        HBPullKlineTemplate<E> t = new HBPullKlineTemplate<>();
        t.setStatus(RESPONSE_STATUS_OK);
        t.setId(id);
        t.setRep(rep);
        t.setData(data);
        return t;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRep() {
        return rep;
    }

    public void setRep(String rep) {
        this.rep = rep;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
