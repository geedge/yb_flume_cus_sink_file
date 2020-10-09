package cn.ac.iie.cusflume.sink.bean.ac.Res;

public class AcResBody {
    private int code;//请求响应状态code-200,400,500,老版字段名称status,新版code
    private String msg;//请求响应结果msg-请求成功,请求失败,服务器异常-老版字段名称message,新版msg
    private String sessionId;//会话标识Set-Cookie,即返回的响应头内包含的sessionId,注意sessionId存在于响应头中,是我手动添加进去的
//    private AcResData data;//数据
    private String data;//数据

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "AcResBody{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
