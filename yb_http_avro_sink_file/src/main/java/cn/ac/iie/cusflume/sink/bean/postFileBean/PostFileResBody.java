package cn.ac.iie.cusflume.sink.bean.postFileBean;

import java.util.Map;

/**
 * 推送文件成功后的返回体
 */
public class PostFileResBody {
    private String msg;
    private int code;
//    private Map<String,String> data;//{"id":"123456"}----旧
    private String data;//{"id":"123456"}----新-20191115

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "PostFileResBody{" +
                "msg='" + msg + '\'' +
                ", code=" + code +
                ", data='" + data + '\'' +
                '}';
    }
}
