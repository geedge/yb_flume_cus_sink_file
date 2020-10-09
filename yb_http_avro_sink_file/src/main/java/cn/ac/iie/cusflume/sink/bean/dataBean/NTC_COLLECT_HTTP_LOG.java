package cn.ac.iie.cusflume.sink.bean.dataBean;

public class NTC_COLLECT_HTTP_LOG extends CommonLog {

    private String k;//用于获取http_seq和proxy_flag
    private String v;

    private int http_seq;
    private int proxy_flag;
    private String url;
    private String referer;
    private String domain;


    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }

    public int getHttp_seq() {
        return http_seq;
    }

    public void setHttp_seq(int http_seq) {
        this.http_seq = http_seq;
    }

    public int getProxy_flag() {
        return proxy_flag;
    }

    public void setProxy_flag(int proxy_flag) {
        this.proxy_flag = proxy_flag;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    @Override
    public String toString() {
        return field() +
                http_seq + "\t" +
                proxy_flag + "\t" +
                url + "\t" +
                referer + "\t" +
                domain + "\n";
    }
}
