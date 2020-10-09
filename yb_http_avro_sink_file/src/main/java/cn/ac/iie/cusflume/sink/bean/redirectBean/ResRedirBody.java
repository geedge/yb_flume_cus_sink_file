package cn.ac.iie.cusflume.sink.bean.redirectBean;

public class ResRedirBody {
    private int code;
    private String msg;
    private ResRedirBodyData data;

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

    public ResRedirBodyData getData() {
        return data;
    }

    public void setData(ResRedirBodyData data) {
        this.data = data;
    }


    @Override
    public String toString() {
        return "ResRedirBody{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", data=" + data +
                '}';
    }
}
