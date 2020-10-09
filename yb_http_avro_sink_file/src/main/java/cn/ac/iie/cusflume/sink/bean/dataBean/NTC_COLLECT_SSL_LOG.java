package cn.ac.iie.cusflume.sink.bean.dataBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

public class NTC_COLLECT_SSL_LOG extends CommonLog {

    private String c2s_pkt_num;
    private String s2c_pkt_num;
    private String c2s_byte_num;
    private String s2c_byte_num;


//    private String SNI;//旧-废弃-20200507
    private String sni;//新-修改-20200507
    private long create_time;

//    private long lastmtime;//旧-废弃-20200507
    private long last_time;//新-修改-20200507

    private String server_ciphersuits;
    private String client_ciphersuits;

//    private Collect_SSL_field_server_cert server_cert;//旧-废弃-20200507
    private String server_cert;//新-修改-压成字符串-20200507

//    private String version;//旧-server_cert内部字段-废弃-20200507

    private String client_cert;

    //20200509新增
    private LOG_D_TAG_BEAN d_tag;//json版本-20200509
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串

    public String getC2s_pkt_num() {
        return c2s_pkt_num;
    }

    public void setC2s_pkt_num(String c2s_pkt_num) {
        this.c2s_pkt_num = c2s_pkt_num;
    }

    public String getS2c_pkt_num() {
        return s2c_pkt_num;
    }

    public void setS2c_pkt_num(String s2c_pkt_num) {
        this.s2c_pkt_num = s2c_pkt_num;
    }

    public String getC2s_byte_num() {
        return c2s_byte_num;
    }

    public void setC2s_byte_num(String c2s_byte_num) {
        this.c2s_byte_num = c2s_byte_num;
    }

    public String getS2c_byte_num() {
        return s2c_byte_num;
    }

    public void setS2c_byte_num(String s2c_byte_num) {
        this.s2c_byte_num = s2c_byte_num;
    }

    public String getSni() {
        return sni;
    }

    public void setSni(String sni) {
        this.sni = sni;
    }

    public long getCreate_time() {
        return create_time;
    }

    public void setCreate_time(long create_time) {
        this.create_time = create_time;
    }

    public long getLast_time() {
        return last_time;
    }

    public void setLast_time(long last_time) {
        this.last_time = last_time;
    }

    public String getServer_ciphersuits() {
        return server_ciphersuits;
    }

    public void setServer_ciphersuits(String server_ciphersuits) {
        this.server_ciphersuits = server_ciphersuits;
    }

    public String getClient_ciphersuits() {
        return client_ciphersuits;
    }

    public void setClient_ciphersuits(String client_ciphersuits) {
        this.client_ciphersuits = client_ciphersuits;
    }

    public String getServer_cert() {
        return server_cert;
    }

    public void setServer_cert(String server_cert) {
        this.server_cert = server_cert;
    }

    public String getClient_cert() {
        return client_cert;
    }

    public void setClient_cert(String client_cert) {
        this.client_cert = client_cert;
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

    //    @Override
//    public String toString() {
//        return field() +
////                version + "\t" +
//                c2s_pkt_num + "\t" +
//                s2c_pkt_num + "\t" +
//                c2s_byte_num + "\t" +
//                s2c_byte_num + "\t" +
//                sni + "\t" +
//                create_time + "\t" +
//                last_time + "\t" +
//                server_ciphersuits + "\t" +
//                client_ciphersuits + "\t" +
//
//                server_cert + "\t" +
//                client_cert + "\n";
//    }


    @Override
    public String toString() {
        return "NTC_COLLECT_SSL_LOG{" +
                "c2s_pkt_num='" + c2s_pkt_num + '\'' +
                ", s2c_pkt_num='" + s2c_pkt_num + '\'' +
                ", c2s_byte_num='" + c2s_byte_num + '\'' +
                ", s2c_byte_num='" + s2c_byte_num + '\'' +
                ", sni='" + sni + '\'' +
                ", create_time=" + create_time +
                ", last_time=" + last_time +
                ", server_ciphersuits='" + server_ciphersuits + '\'' +
                ", client_ciphersuits='" + client_ciphersuits + '\'' +
                ", server_cert='" + server_cert + '\'' +
                ", client_cert='" + client_cert + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
