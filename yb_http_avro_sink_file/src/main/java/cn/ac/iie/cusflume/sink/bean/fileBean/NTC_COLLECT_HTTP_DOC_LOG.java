package cn.ac.iie.cusflume.sink.bean.fileBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

public class NTC_COLLECT_HTTP_DOC_LOG extends CommonLog_File {

    private String url;
    private String referer;
    private String user_agent;
    private int http_seq;
    private String req_line;
    private String res_line;
    private String cookie;
    private String content_type;

    private long content_len;//20200507新增字段
    private String doc_type;//20200514新增字段

    private String req_body_file_path;//新增-20191216
    private String res_body_file_path;//新增-20191216

    //20191209新增
//    private String d_tag;//json字符串版本
    private LOG_D_TAG_BEAN d_tag;//json版本-20191216
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串
    //20191209新增


    private String set_cookie;//目前未使用-20191216

    private String req_hdr_file;//原本;//目前未使用-20191216
    private String req_hdr_file_id;//目前未使用-20191216
    private String req_body_file;//原本;//目前未使用-20191216
    private String req_body_file_id;//目前未使用-20191216

    private String res_hdr_file;//原本;//目前未使用-20191216
    private String res_hdr_file_id;//目前未使用-20191216
    private String res_body_file;//原本;//目前未使用-20191216
    private String res_body_file_id;//目前未使用-20191216


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public int getHttp_seq() {
        return http_seq;
    }

    public void setHttp_seq(int http_seq) {
        this.http_seq = http_seq;
    }

    public String getReq_line() {
        return req_line;
    }

    public void setReq_line(String req_line) {
        this.req_line = req_line;
    }

    public String getRes_line() {
        return res_line;
    }

    public void setRes_line(String res_line) {
        this.res_line = res_line;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getContent_type() {
        return content_type;
    }

    public void setContent_type(String content_type) {
        this.content_type = content_type;
    }

    public long getContent_len() {
        return content_len;
    }

    public void setContent_len(long content_len) {
        this.content_len = content_len;
    }

    public String getDoc_type() {
        return doc_type;
    }

    public void setDoc_type(String doc_type) {
        this.doc_type = doc_type;
    }

    public String getReq_body_file_path() {
        return req_body_file_path;
    }

    public void setReq_body_file_path(String req_body_file_path) {
        this.req_body_file_path = req_body_file_path;
    }

    public String getRes_body_file_path() {
        return res_body_file_path;
    }

    public void setRes_body_file_path(String res_body_file_path) {
        this.res_body_file_path = res_body_file_path;
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

    public String getSet_cookie() {
        return set_cookie;
    }

    public void setSet_cookie(String set_cookie) {
        this.set_cookie = set_cookie;
    }

    public String getReq_hdr_file() {
        return req_hdr_file;
    }

    public void setReq_hdr_file(String req_hdr_file) {
        this.req_hdr_file = req_hdr_file;
    }

    public String getReq_hdr_file_id() {
        return req_hdr_file_id;
    }

    public void setReq_hdr_file_id(String req_hdr_file_id) {
        this.req_hdr_file_id = req_hdr_file_id;
    }

    public String getReq_body_file() {
        return req_body_file;
    }

    public void setReq_body_file(String req_body_file) {
        this.req_body_file = req_body_file;
    }

    public String getReq_body_file_id() {
        return req_body_file_id;
    }

    public void setReq_body_file_id(String req_body_file_id) {
        this.req_body_file_id = req_body_file_id;
    }

    public String getRes_hdr_file() {
        return res_hdr_file;
    }

    public void setRes_hdr_file(String res_hdr_file) {
        this.res_hdr_file = res_hdr_file;
    }

    public String getRes_hdr_file_id() {
        return res_hdr_file_id;
    }

    public void setRes_hdr_file_id(String res_hdr_file_id) {
        this.res_hdr_file_id = res_hdr_file_id;
    }

    public String getRes_body_file() {
        return res_body_file;
    }

    public void setRes_body_file(String res_body_file) {
        this.res_body_file = res_body_file;
    }

    public String getRes_body_file_id() {
        return res_body_file_id;
    }

    public void setRes_body_file_id(String res_body_file_id) {
        this.res_body_file_id = res_body_file_id;
    }

    @Override
    public String toString() {
        return "NTC_COLLECT_HTTP_DOC_LOG{" +
                "url='" + url + '\'' +
                ", referer='" + referer + '\'' +
                ", user_agent='" + user_agent + '\'' +
                ", http_seq=" + http_seq +
                ", req_line='" + req_line + '\'' +
                ", res_line='" + res_line + '\'' +
                ", cookie='" + cookie + '\'' +
                ", content_type='" + content_type + '\'' +
                ", content_len=" + content_len +
                ", doc_type='" + doc_type + '\'' +
                ", req_body_file_path='" + req_body_file_path + '\'' +
                ", res_body_file_path='" + res_body_file_path + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
