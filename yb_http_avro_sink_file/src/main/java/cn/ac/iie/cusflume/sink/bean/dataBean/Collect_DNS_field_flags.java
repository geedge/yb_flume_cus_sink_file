package cn.ac.iie.cusflume.sink.bean.dataBean;

public class Collect_DNS_field_flags {

    private long qr;
    private long opcode;
    private long aa;
    private long tc;
    private long rd;
    private long ra;
    private long z;
    private long rcode;


    public long getQr() {
        return qr;
    }

    public void setQr(long qr) {
        this.qr = qr;
    }

    public long getOpcode() {
        return opcode;
    }

    public void setOpcode(long opcode) {
        this.opcode = opcode;
    }

    public long getAa() {
        return aa;
    }

    public void setAa(long aa) {
        this.aa = aa;
    }

    public long getTc() {
        return tc;
    }

    public void setTc(long tc) {
        this.tc = tc;
    }

    public long getRd() {
        return rd;
    }

    public void setRd(long rd) {
        this.rd = rd;
    }

    public long getRa() {
        return ra;
    }

    public void setRa(long ra) {
        this.ra = ra;
    }

    public long getZ() {
        return z;
    }

    public void setZ(long z) {
        this.z = z;
    }

    public long getRcode() {
        return rcode;
    }

    public void setRcode(long rcode) {
        this.rcode = rcode;
    }
}
