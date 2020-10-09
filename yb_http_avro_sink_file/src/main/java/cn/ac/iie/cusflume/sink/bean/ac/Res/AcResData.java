package cn.ac.iie.cusflume.sink.bean.ac.Res;

/**
 * 暂未使用,AcResBody的data字段的对象形式
 */
public class AcResData {
    private int status;//审核状态-0,1,2
    private String reason;//审核说明-审核通过,审核中,审核失败

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "AcResData{" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                '}';
    }
}
