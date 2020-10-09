package cn.ac.iie.cusflume.sink.bean.configBean;

/**
 * 存放与ZX交互的url以及验证标识---只存放长期固定不变的数据
 * 1.存储post文件,信息url(配置文件获取);
 * 2.存储入库前验证文件,信息url(配置文件获取);
 * 3.存储文件,信息SessionCookie(请求总线获取);
 * 4.存储msg作为avro入库所需的topicName,userAgent,xTag,batchSize
 */
public class ConfigInfo {

    private String postMsgUrl;
    private String postFileUrl;

    private String checkMsgUrl;
    private String checkFileUrl;

    private String msgSessionCookie;
    private String fileSessionCookie;
    private String monitorSessionCookie;//20200818新增，缓存monitorSessionCookie

//    private String sendMsg;

    private String topicName;
    private String userAgent;
    private String xTag;
    private int batchSize;

    public String getPostMsgUrl() {
        return postMsgUrl;
    }

    public void setPostMsgUrl(String postMsgUrl) {
        this.postMsgUrl = postMsgUrl;
    }

    public String getPostFileUrl() {
        return postFileUrl;
    }

    public void setPostFileUrl(String postFileUrl) {
        this.postFileUrl = postFileUrl;
    }

    public String getCheckMsgUrl() {
        return checkMsgUrl;
    }

    public void setCheckMsgUrl(String checkMsgUrl) {
        this.checkMsgUrl = checkMsgUrl;
    }

    public String getCheckFileUrl() {
        return checkFileUrl;
    }

    public void setCheckFileUrl(String checkFileUrl) {
        this.checkFileUrl = checkFileUrl;
    }

    public String getMsgSessionCookie() {
        return msgSessionCookie;
    }

    public void setMsgSessionCookie(String msgSessionCookie) {
        this.msgSessionCookie = msgSessionCookie;
    }

    public String getFileSessionCookie() {
        return fileSessionCookie;
    }

    public void setFileSessionCookie(String fileSessionCookie) {
        this.fileSessionCookie = fileSessionCookie;
    }

    public String getMonitorSessionCookie() {
        return monitorSessionCookie;
    }

    public void setMonitorSessionCookie(String monitorSessionCookie) {
        this.monitorSessionCookie = monitorSessionCookie;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getxTag() {
        return xTag;
    }

    public void setxTag(String xTag) {
        this.xTag = xTag;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    //    public String getSendMsg() {
//        return sendMsg;
//    }
//
//    public void setSendMsg(String sendMsg) {
//        this.sendMsg = sendMsg;
//    }


    @Override
    public String toString() {
        return "ConfigInfo{" +
                "postMsgUrl='" + postMsgUrl + '\'' +
                ", postFileUrl='" + postFileUrl + '\'' +
                ", checkMsgUrl='" + checkMsgUrl + '\'' +
                ", checkFileUrl='" + checkFileUrl + '\'' +
                ", msgSessionCookie='" + msgSessionCookie + '\'' +
                ", fileSessionCookie='" + fileSessionCookie + '\'' +
                ", monitorSessionCookie='" + monitorSessionCookie + '\'' +
                ", topicName='" + topicName + '\'' +
                ", userAgent='" + userAgent + '\'' +
                ", xTag='" + xTag + '\'' +
                ", batchSize=" + batchSize +
                '}';
    }
}
