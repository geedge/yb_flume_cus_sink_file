package cn.ac.iie.cusflume.sink.HttpAsyncUtils.ybBean;

public class PutFileInfo {
    private String hostIp;
    private String authorization;


    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public String getAuthorization() {
        return authorization;
    }

    public void setAuthorization(String authorization) {
        this.authorization = authorization;
    }

    @Override
    public String toString() {
        return "PutFileInfo{" +
                "hostIp='" + hostIp + '\'' +
                ", authorization='" + authorization + '\'' +
                '}';
    }
}
