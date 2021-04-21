package com.tqxd.jys.messagebus.payload.trade;

/**
 * https://huobiapi.github.io/docs/spot/v1/cn/#56c6c47284-2
 *
 * @author yjt
 * @since 2020/10/15 下午6:52
 */
public class TradeDetailResp {
    private String ch;

    private final Long ts = System.currentTimeMillis();

    private TradeDetailTick tick;

    public TradeDetailResp() {
    }

    public String getCh() {
        return this.ch;
    }

    public Long getTs() {
        return this.ts;
    }

    public TradeDetailTick getTick() {
        return this.tick;
    }

    public void setCh(String ch) {
        this.ch = ch;
    }

    public void setTick(TradeDetailTick tick) {
        this.tick = tick;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof TradeDetailResp)) return false;
        final TradeDetailResp other = (TradeDetailResp) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$ch = this.getCh();
        final Object other$ch = other.getCh();
        if (this$ch == null ? other$ch != null : !this$ch.equals(other$ch)) return false;
        final Object this$ts = this.getTs();
        final Object other$ts = other.getTs();
        if (this$ts == null ? other$ts != null : !this$ts.equals(other$ts)) return false;
        final Object this$tick = this.getTick();
        final Object other$tick = other.getTick();
        if (this$tick == null ? other$tick != null : !this$tick.equals(other$tick)) return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof TradeDetailResp;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $ch = this.getCh();
        result = result * PRIME + ($ch == null ? 43 : $ch.hashCode());
        final Object $ts = this.getTs();
        result = result * PRIME + ($ts == null ? 43 : $ts.hashCode());
        final Object $tick = this.getTick();
        result = result * PRIME + ($tick == null ? 43 : $tick.hashCode());
        return result;
    }

    public String toString() {
        return "TradeDetailResp(ch=" + this.getCh() + ", ts=" + this.getTs() + ", tick=" + this.getTick() + ")";
    }
}
