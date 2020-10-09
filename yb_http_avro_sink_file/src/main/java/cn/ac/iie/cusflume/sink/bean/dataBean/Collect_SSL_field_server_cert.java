package cn.ac.iie.cusflume.sink.bean.dataBean;

public class Collect_SSL_field_server_cert {

    private String version;
    private String serial_number;
    private String algorithm_id;
    private String issuer;
    private String issue_country;
    private String issue_organize;
    private String issue_cname;
    private String sub;
    private String sub_country;
    private String sub_organize;
    private String sub_cname;
    private String start_time;
    private String expire_time;
    private String san;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getSerial_number() {
        return serial_number;
    }

    public void setSerial_number(String serial_number) {
        this.serial_number = serial_number;
    }

    public String getAlgorithm_id() {
        return algorithm_id;
    }

    public void setAlgorithm_id(String algorithm_id) {
        this.algorithm_id = algorithm_id;
    }

    public String getIssuer() {
        return issuer;
    }

    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }

    public String getIssue_country() {
        return issue_country;
    }

    public void setIssue_country(String issue_country) {
        this.issue_country = issue_country;
    }

    public String getIssue_organize() {
        return issue_organize;
    }

    public void setIssue_organize(String issue_organize) {
        this.issue_organize = issue_organize;
    }

    public String getIssue_cname() {
        return issue_cname;
    }

    public void setIssue_cname(String issue_cname) {
        this.issue_cname = issue_cname;
    }

    public String getSub() {
        return sub;
    }

    public void setSub(String sub) {
        this.sub = sub;
    }

    public String getSub_country() {
        return sub_country;
    }

    public void setSub_country(String sub_country) {
        this.sub_country = sub_country;
    }

    public String getSub_organize() {
        return sub_organize;
    }

    public void setSub_organize(String sub_organize) {
        this.sub_organize = sub_organize;
    }

    public String getSub_cname() {
        return sub_cname;
    }

    public void setSub_cname(String sub_cname) {
        this.sub_cname = sub_cname;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getExpire_time() {
        return expire_time;
    }

    public void setExpire_time(String expire_time) {
        this.expire_time = expire_time;
    }

    public String getSan() {
        return san;
    }

    public void setSan(String san) {
        this.san = san;
    }

    @Override
    public String toString() {
        return "server_cert{" +
                "version='" + version + '\'' +
                ", serial_number='" + serial_number + '\'' +
                ", algorithm_id='" + algorithm_id + '\'' +
                ", issuer='" + issuer + '\'' +
                ", issue_country='" + issue_country + '\'' +
                ", issue_organize='" + issue_organize + '\'' +
                ", issue_cname='" + issue_cname + '\'' +
                ", sub='" + sub + '\'' +
                ", sub_country='" + sub_country + '\'' +
                ", sub_organize='" + sub_organize + '\'' +
                ", sub_cname='" + sub_cname + '\'' +
                ", start_time='" + start_time + '\'' +
                ", expire_time='" + expire_time + '\'' +
                ", san='" + san + '\'' +
                '}';
    }
}
