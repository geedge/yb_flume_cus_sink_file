package cn.ac.iie.cusflume.sink.bean.fileBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

/**
 * TELNET 采集日志-20200325
 */
public class NTC_COLLECT_TELNET_LOG extends CommonLog_File {

    private String username;
    private String password;
//    private String telnet_content_path;//旧-20200506废弃
    private String content_path;//新-20200506新修改

    //20200211新增
    private LOG_D_TAG_BEAN d_tag;//json版本
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串
    //20200211新增


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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
//        return "NTC_COLLECT_TELNET_LOG{" +
//                "username='" + username + '\'' +
//                ", password='" + password + '\'' +
//                ", telnet_content_path='" + content_path + '\'' +
//                ", d_tag=" + d_tag +
//                ", x_tag='" + x_tag + '\'' +
//                '}';
//    }


    @Override
    public String toString() {
        return "NTC_COLLECT_TELNET_LOG{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", content_path='" + content_path + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
