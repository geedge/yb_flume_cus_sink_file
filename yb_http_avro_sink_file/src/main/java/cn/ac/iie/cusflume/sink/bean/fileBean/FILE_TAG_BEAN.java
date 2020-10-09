package cn.ac.iie.cusflume.sink.bean.fileBean;

import java.util.Arrays;

/**
 * 接收NTC-COLLECT-FILE-LOG数据,是独立出来的文件标签,单独发送不与数据对象拼接
 */
public class FILE_TAG_BEAN {
//    /**
//     * 旧
//     * @return
//     */
//    private String tag_version;
//    private long data_type;
//    private long data_subtype;
//    private long producer_id;
//    private long data_source;
//    private long task_id;
//
//    private long file_id;
//    private String file_path;
//    private String file_name;
//    private String file_name_charset;//新增-20191216
//    private double file_size;
//    private String md5;
//    private String encrypted;
//    private String timestamp;
//    private long parent_id;
////    private long parent_type;
//    private long flow_id;
//
//    private String x_tag;//新增-20191216


    /**
     * 新-20191219
     * @return
     */
    private String tag_version;
    private int data_type;
    private int data_subtype;
    private int producer_id;
    private int data_source;
    private int[] task_id;//int数组

    private long file_id;
    private String file_path;
    private String file_name;
    private String file_name_charset;//新增-20191216
    private float file_size;
    private String md5;
    private String encrypted;
    private String timestamp;
    private long parent_id;
    private long flow_id;

    private String x_tag;//新增-20191216

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

    public long getFile_id() {
        return file_id;
    }

    public void setFile_id(long file_id) {
        this.file_id = file_id;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public String getFile_name_charset() {
        return file_name_charset;
    }

    public void setFile_name_charset(String file_name_charset) {
        this.file_name_charset = file_name_charset;
    }

    public float getFile_size() {
        return file_size;
    }

    public void setFile_size(float file_size) {
        this.file_size = file_size;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public String getEncrypted() {
        return encrypted;
    }

    public void setEncrypted(String encrypted) {
        this.encrypted = encrypted;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public long getParent_id() {
        return parent_id;
    }

    public void setParent_id(long parent_id) {
        this.parent_id = parent_id;
    }

    public long getFlow_id() {
        return flow_id;
    }

    public void setFlow_id(long flow_id) {
        this.flow_id = flow_id;
    }

    public String getX_tag() {
        return x_tag;
    }

    public void setX_tag(String x_tag) {
        this.x_tag = x_tag;
    }

    @Override
    public String toString() {
        return "FILE_TAG_BEAN{" +
                "tag_version='" + tag_version + '\'' +
                ", data_type=" + data_type +
                ", data_subtype=" + data_subtype +
                ", producer_id=" + producer_id +
                ", data_source=" + data_source +
                ", task_id=" + Arrays.toString(task_id) +
                ", file_id=" + file_id +
                ", file_path='" + file_path + '\'' +
                ", file_name='" + file_name + '\'' +
                ", file_name_charset='" + file_name_charset + '\'' +
                ", file_size=" + file_size +
                ", md5='" + md5 + '\'' +
                ", encrypted='" + encrypted + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", parent_id=" + parent_id +
                ", flow_id=" + flow_id +
                ", x_tag='" + x_tag + '\'' +
                '}';
    }
}
