package cn.ac.iie.cusflume.sink.bean.producer.Res;

/**
 * 暂未使用,ProResBody中data的对象形式
 */
public class ProResData {
    private String checksum;

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    @Override
    public String toString() {
        return "ProResData{" +
                "checksum='" + checksum + '\'' +
                '}';
    }
}
