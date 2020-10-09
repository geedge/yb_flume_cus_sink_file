package cn.ac.iie.cusflume.sink.bean.dataBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

public class NTC_CONN_RECORD_LOG extends CommonLog {
    private String app_label;
    private String c2s_pkt_num;
    private String s2c_pkt_num;
    private String c2s_byte_num;
    private String s2c_byte_num;

    //20191216新增
//    private String d_tag;//json字符串版本
    private LOG_D_TAG_BEAN d_tag;//json版本-20191216
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串
    //20191216新增

    public String getApp_label() {
        return app_label;
    }

    public void setApp_label(String app_label) {
        this.app_label = app_label;
    }

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

    @Override
    public String toString() {
        return "NTC_CONN_RECORD_LOG{" +
                "app_label='" + app_label + '\'' +
                ", c2s_pkt_num='" + c2s_pkt_num + '\'' +
                ", s2c_pkt_num='" + s2c_pkt_num + '\'' +
                ", c2s_byte_num='" + c2s_byte_num + '\'' +
                ", s2c_byte_num='" + s2c_byte_num + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
