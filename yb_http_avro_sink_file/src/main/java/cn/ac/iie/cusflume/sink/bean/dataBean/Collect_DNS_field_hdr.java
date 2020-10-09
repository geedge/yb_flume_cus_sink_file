package cn.ac.iie.cusflume.sink.bean.dataBean;

public class Collect_DNS_field_hdr {

    private long id;
    private long qdcount;
    private long ancount;
    private long aucount;
    private long adcount;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getQdcount() {
        return qdcount;
    }

    public void setQdcount(long qdcount) {
        this.qdcount = qdcount;
    }

    public long getAncount() {
        return ancount;
    }

    public void setAncount(long ancount) {
        this.ancount = ancount;
    }

    public long getAucount() {
        return aucount;
    }

    public void setAucount(long aucount) {
        this.aucount = aucount;
    }

    public long getAdcount() {
        return adcount;
    }

    public void setAdcount(long adcount) {
        this.adcount = adcount;
    }
}
