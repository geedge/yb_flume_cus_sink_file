package cn.ac.iie.cusflume.sink.bean.dataBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

public class NTC_COLLECT_DNS_LOG extends CommonLog {

    /**
     * 旧
     */
//    private int dns_id;
//    private int qr;
//    private int rd;
//    private int ra;
//    private int tc;
//    private int aa;
//    private int rcode;
//    private String rr;
//    private int qtype;
//    private int qclass;
//    private int opcode;
//    private String qname;
//    private String cname;
//    private int dns_sub;

    /**
     * 新
     */
    private long dns_sub;

    /**
     * hdr json
     */
//    private Collect_DNS_field_hdr hdr;//旧-废弃-20200507
    private String hdr;//新-修改-压成字符串-20200507
    /**
     * hdr内部字段-20200507移除
     */
//    private long id;
//    private long qdcount;
//    private long ancount;
//    private long aucount;
//    private long adcount;

    /**
     * flags json
     */
//    private Collect_DNS_field_flags flags;//旧-废弃-20200507
    private String flags;//新-修改-压成字符串-20200507
    /**
     * flags内部字段-20200507移除
     */
//    private long qr;
//    private long opcode;
//    private long aa;
//    private long tc;
//    private long rd;
//    private long ra;
//    private long z;
//    private long rcode;

    private String question;

    private String rr;

    //20200509新增
    private LOG_D_TAG_BEAN d_tag;//json版本-20200509
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串


//    public Collect_DNS_field_hdr getHdr() {
//        return hdr;
//    }

//    public void setHdr(Collect_DNS_field_hdr hdr) {
//        this.hdr = hdr;
//        //增加自赋值
//        this.id = hdr.getId();
//        this.qdcount = hdr.getQdcount();
//        this.ancount = hdr.getAncount();
//        this.aucount = hdr.getAucount();
//        this.adcount = hdr.getAdcount();
//    }

//    public Collect_DNS_field_flags getFlags() {
//        return flags;
//    }
//
//    public void setFlags(Collect_DNS_field_flags flags) {
//        this.flags = flags;
//        //增加自赋值
//        this.qr = flags.getQr();
//        this.opcode = flags.getOpcode();
//        this.aa = flags.getAa();
//        this.tc = flags.getTc();
//        this.rd = flags.getRd();
//        this.ra = flags.getRa();
//        this.z = flags.getZ();
//        this.rcode = flags.getRcode();
//    }

    public long getDns_sub() {
        return dns_sub;
    }

    public void setDns_sub(long dns_sub) {
        this.dns_sub = dns_sub;
    }

    public String getHdr() {
        return hdr;
    }

    public void setHdr(String hdr) {
        this.hdr = hdr;
    }

    public String getFlags() {
        return flags;
    }

    public void setFlags(String flags) {
        this.flags = flags;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getRr() {
        return rr;
    }

    public void setRr(String rr) {
        this.rr = rr;
    }

    public LOG_D_TAG_BEAN getD_tag() {
        return d_tag;
    }

    public void setD_tag(LOG_D_TAG_BEAN d_tag) {
        this.d_tag = d_tag;
    }

    public String getX_tag() {
        return x_tag;
    }

    public void setX_tag(String x_tag) {
        this.x_tag = x_tag;
    }

    /**
     * 旧
     */
//    @Override
//    public String toString() {
//        return field() +
//                dns_id + "\t" +
//                qr + "\t" +
//                rd + "\t" +
//                ra + "\t" +
//                tc + "\t" +
//                aa + "\t" +
//                rcode + "\t" +
//                rr + "\t" +
//                qtype + "\t" +
//                qclass + "\t" +
//                opcode + "\t" +
//                qname + "\t" +
//                cname + "\t" +
//                dns_sub + "\n";
//    }

    /**
     * 新
     */
    @Override
    public String toString() {
        return "NTC_COLLECT_DNS_LOG{" +
                "dns_sub=" + dns_sub +
                ", hdr='" + hdr + '\'' +
                ", flags='" + flags + '\'' +
                ", question='" + question + '\'' +
                ", rr='" + rr + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
