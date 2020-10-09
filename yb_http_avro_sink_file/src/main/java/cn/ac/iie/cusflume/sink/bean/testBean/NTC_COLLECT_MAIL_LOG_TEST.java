package cn.ac.iie.cusflume.sink.bean.testBean;

import cn.ac.iie.cusflume.sink.bean.fileBean.CommonLog_File;

import java.util.Arrays;

public class NTC_COLLECT_MAIL_LOG_TEST extends CommonLog_File{

    private String mail_proto;
    private String mail_from;
    private String mail_to;
    private String mail_cc;//新增-20191216
    private String subject;

    private String eml_file_path;//新增-20191216
    private String attachments_path;//新增-20191216
    private String subject_charset;

    //20191209新增
//    private String d_tag;//json字符串版本
    private LOG_D_TAG_BEAN_TEST d_tag;//json版本-20191216
    private String x_tag;//json字符串版本,就算前端传入的为json也会强转为字符串

    //d_tag字段----↓
    private String tag_version;
    private int data_type;
    private int data_subtype;
    private int producer_id;
    private int data_source;
    private int[] task_id;
    private long data_id;
    private long flow_id;

    private long[] file_id_list;
    private String[] file_path_list;
    //d_tag字段----↑

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

    public LOG_D_TAG_BEAN_TEST getD_tag() {
        return d_tag;
    }

    //给Mail表层字段重新赋值
    public void setD_tag(LOG_D_TAG_BEAN_TEST d_tag) {
        this.d_tag = d_tag;
        this.tag_version=d_tag.getTag_version();
        this.data_type=d_tag.getData_type();
        this.data_subtype=d_tag.getData_subtype();
        this.producer_id=d_tag.getProducer_id();
        this.data_source=d_tag.getData_source();
        this.task_id=d_tag.getTask_id();
        this.data_id=d_tag.getData_id();
        this.flow_id=d_tag.getFlow_id();

        this.file_id_list=d_tag.getFile_id_list();
        this.file_path_list=d_tag.getFile_path_list();
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

    public String getTag_version() {
        return tag_version;
    }

    public void setTag_version(String tag_version) {
        this.tag_version = tag_version;
    }

    public int getData_type() {
        return data_type;
    }

    public void setData_type(int data_type) {
        this.data_type = data_type;
    }

    public int getData_subtype() {
        return data_subtype;
    }

    public void setData_subtype(int data_subtype) {
        this.data_subtype = data_subtype;
    }

    public int getProducer_id() {
        return producer_id;
    }

    public void setProducer_id(int producer_id) {
        this.producer_id = producer_id;
    }

    public int getData_source() {
        return data_source;
    }

    public void setData_source(int data_source) {
        this.data_source = data_source;
    }

    public int[] getTask_id() {
        return task_id;
    }

    public void setTask_id(int[] task_id) {
        this.task_id = task_id;
    }

    public long getData_id() {
        return data_id;
    }

    public void setData_id(long data_id) {
        this.data_id = data_id;
    }

    public long getFlow_id() {
        return flow_id;
    }

    public void setFlow_id(long flow_id) {
        this.flow_id = flow_id;
    }

    public long[] getFile_id_list() {
        return file_id_list;
    }

    public void setFile_id_list(long[] file_id_list) {
        this.file_id_list = file_id_list;
    }

    public String[] getFile_path_list() {
        return file_path_list;
    }

    public void setFile_path_list(String[] file_path_list) {
        this.file_path_list = file_path_list;
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
