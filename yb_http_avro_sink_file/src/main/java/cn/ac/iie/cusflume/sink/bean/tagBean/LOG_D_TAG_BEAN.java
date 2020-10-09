package cn.ac.iie.cusflume.sink.bean.tagBean;

import java.util.Arrays;

/**
 * 存储在日志消息内部的d_tag字段,实际为日志标签,需要提取出来进行avro序列化后和数据对象拼成字节数组一同发送
 */
public class LOG_D_TAG_BEAN {

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

    @Override
    public String toString() {
        return "LOG_D_TAG_BEAN{" +
                "tag_version='" + tag_version + '\'' +
                ", data_type=" + data_type +
                ", data_subtype=" + data_subtype +
                ", producer_id=" + producer_id +
                ", data_source=" + data_source +
                ", task_id=" + Arrays.toString(task_id) +
                ", data_id=" + data_id +
                ", flow_id=" + flow_id +
                ", file_id_list=" + Arrays.toString(file_id_list) +
                ", file_path_list=" + Arrays.toString(file_path_list) +
                '}';
    }
}
