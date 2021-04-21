package com.tqxd.jys.messagebus.payload.trade;

import java.util.List;

/**
 * @author yjt
 * @since 2020/10/15 下午6:58
 */
public class TradeDetailTick {

    private final Long id = System.currentTimeMillis() / 1000;
    private final Long ts = System.currentTimeMillis() / 1000;

    private List<TradeDetailTickData> data;

    public TradeDetailTick() {
    }

    public Long getId() {
        return this.id;
    }

    public Long getTs() {
        return this.ts;
    }

    public List<TradeDetailTickData> getData() {
        return this.data;
    }

    public void setData(List<TradeDetailTickData> data) {
        this.data = data;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof TradeDetailTick)) return false;
        final TradeDetailTick other = (TradeDetailTick) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$ts = this.getTs();
        final Object other$ts = other.getTs();
        if (this$ts == null ? other$ts != null : !this$ts.equals(other$ts)) return false;
        final Object this$data = this.getData();
        final Object other$data = other.getData();
        if (this$data == null ? other$data != null : !this$data.equals(other$data)) return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof TradeDetailTick;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $ts = this.getTs();
        result = result * PRIME + ($ts == null ? 43 : $ts.hashCode());
        final Object $data = this.getData();
        result = result * PRIME + ($data == null ? 43 : $data.hashCode());
        return result;
    }

    public String toString() {
        return "TradeDetailTick(id=" + this.getId() + ", ts=" + this.getTs() + ", data=" + this.getData() + ")";
    }
}
