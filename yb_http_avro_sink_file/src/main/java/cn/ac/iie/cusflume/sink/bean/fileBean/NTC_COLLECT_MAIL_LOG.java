package cn.ac.iie.cusflume.sink.bean.fileBean;

import cn.ac.iie.cusflume.sink.bean.tagBean.LOG_D_TAG_BEAN;

import java.util.Arrays;

public class NTC_COLLECT_MAIL_LOG extends CommonLog_File{

    private String mail_proto;
    private String mail_from;
    private String mail_to;
    private String mail_cc;//新增-20191216
    private String mail_date;//新增-20200304
    private String subject;

    private String eml_file_path;//新增-20191216
    private String attachments_path;//新增-20191216
    private String subject_charset;

    //20191209新增
//    private String d_tag;//json字符串版本
    private LOG_D_TAG_BEAN d_tag;//json版本-20191216
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串
    //20191209新增

    private String eml_file;//目前未使用-20191216
    private String eml_file_id;//目前未使用-20191216

    private String[] attachments;//目前未使用-20191216
    private String attachments_id;//目前未使用-20191216




    public String getMail_proto() {
        return mail_proto;
    }

    public void setMail_proto(String mail_proto) {
        this.mail_proto = mail_proto;
    }

    public String getMail_from() {
        return mail_from;
    }

    public void setMail_from(String mail_from) {
        this.mail_from = mail_from;
    }

    public String getMail_to() {
        return mail_to;
    }

    public void setMail_to(String mail_to) {
        this.mail_to = mail_to;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getEml_file() {
        return eml_file;
    }

    public void setEml_file(String eml_file) {
        this.eml_file = eml_file;
    }

    public String getEml_file_id() {
        return eml_file_id;
    }

    public void setEml_file_id(String eml_file_id) {
        this.eml_file_id = eml_file_id;
    }

    public String[] getAttachments() {
        return attachments;
    }

    public void setAttachments(String[] attachments) {
        this.attachments = attachments;
    }

    public String getAttachments_id() {
        return attachments_id;
    }

    public void setAttachments_id(String attachments_id) {
        this.attachments_id = attachments_id;
    }

    public String getSubject_charset() {
        return subject_charset;
    }

    public void setSubject_charset(String subject_charset) {
        this.subject_charset = subject_charset;
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

    public String getMail_cc() {
        return mail_cc;
    }

    public void setMail_cc(String mail_cc) {
        this.mail_cc = mail_cc;
    }

    public String getEml_file_path() {
        return eml_file_path;
    }

    public void setEml_file_path(String eml_file_path) {
        this.eml_file_path = eml_file_path;
    }

    public String getAttachments_path() {
        return attachments_path;
    }

    public void setAttachments_path(String attachments_path) {
        this.attachments_path = attachments_path;
    }

    public String getMail_date() {
        return mail_date;
    }

    public void setMail_date(String mail_date) {
        this.mail_date = mail_date;
    }

    //    @Override
//    public String toString() {
//        return "NTC_COLLECT_MAIL_LOG{" +
//                "mail_proto='" + mail_proto + '\'' +
//                ", mail_from='" + mail_from + '\'' +
//                ", mail_to='" + mail_to + '\'' +
//                ", subject='" + subject + '\'' +
//                ", eml_file='" + eml_file + '\'' +
//                ", eml_file_id='" + eml_file_id + '\'' +
//                ", attachments=" + Arrays.toString(attachments) +
//                ", attachments_id='" + attachments_id + '\'' +
//                ", subject_charset='" + subject_charset + '\'' +
//                '}';
//    }


    @Override
    public String toString() {
        return "NTC_COLLECT_MAIL_LOG{" +
                "mail_proto='" + mail_proto + '\'' +
                ", mail_from='" + mail_from + '\'' +
                ", mail_to='" + mail_to + '\'' +
                ", mail_cc='" + mail_cc + '\'' +
                ", mail_date='" + mail_date + '\'' +
                ", subject='" + subject + '\'' +
                ", eml_file_path='" + eml_file_path + '\'' +
                ", attachments_path='" + attachments_path + '\'' +
                ", subject_charset='" + subject_charset + '\'' +
                ", d_tag=" + d_tag +
                ", x_tag='" + x_tag + '\'' +
                ", eml_file='" + eml_file + '\'' +
                ", eml_file_id='" + eml_file_id + '\'' +
                ", attachments=" + Arrays.toString(attachments) +
                ", attachments_id='" + attachments_id + '\'' +
                '}';
    }
}
