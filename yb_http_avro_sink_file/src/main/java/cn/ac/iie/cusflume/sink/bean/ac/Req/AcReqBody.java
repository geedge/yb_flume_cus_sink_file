package cn.ac.iie.cusflume.sink.bean.ac.Req;

public class AcReqBody {

    private int requestType;//接入请求类型-10(生产消息)、20（从最开始位置开始消费),21（从上次消费位置之后开始消费，即获取总线中缓存的该标签自上次消费之后的增量消息）、22（从最新开始消费，即获取总线中缓存的该标签的最后一个消息）

    public int getRequestType() {
        return requestType;
    }

    public void setRequestType(int requestType) {
        this.requestType = requestType;
    }

    @Override
    public String toString() {
        return "AcReqBody{" +
                "requestType=" + requestType +
                '}';
    }
}
