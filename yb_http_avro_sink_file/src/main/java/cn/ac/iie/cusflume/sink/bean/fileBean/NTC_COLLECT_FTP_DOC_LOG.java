package cn.ac.iie.cusflume.sink.bean.fileBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

public class NTC_COLLECT_FTP_DOC_LOG extends CommonLog_File {

//    private String url;
//    private String res_body_file;
//    private String res_body_file_id;

    private String ftp_url;
    private String ftp_content;
//    private String ftp_content_id;//旧-20200506废弃
    private String content_path;//新-20200506新修改

    //20200211新增
    private LOG_D_TAG_BEAN d_tag;//json版本
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串
    //20200211新增

    public String getFtp_url() {
        return ftp_url;
    }

    public void setFtp_url(String ftp_url) {
        this.ftp_url = ftp_url;
    }

    public String getFtp_content() {
        return ftp_content;
    }

    public void setFtp_content(String ftp_content) {
        this.ftp_content = ftp_content;
    }

    public String getContent_path() {
        return content_path;
    }

    public void setContent_path(String content_path) {
        this.content_path = content_path;
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
//        return "NTC_COLLECT_FTP_DOC_LOG{" +
//                "ftp_url='" + ftp_url + '\'' +
//                ", ftp_content='" + ftp_content + '\'' +
//                ", ftp_content_id='" + content_path + '\'' +
//                ", d_tag=" + d_tag +
//                ", x_tag='" + x_tag + '\'' +
//                '}';
//    }


    @Override
    public String toString() {
        return "NTC_COLLECT_FTP_DOC_LOG{" +
                "ftp_url='" + ftp_url + '\'' +
                ", ftp_content='" + ftp_content + '\'' +
                ", content_path='" + content_path + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
