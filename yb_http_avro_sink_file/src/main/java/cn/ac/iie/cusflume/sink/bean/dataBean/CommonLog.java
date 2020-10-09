package cn.ac.iie.cusflume.sink.bean.dataBean;

public class CommonLog {
    //后期特别新增的字段,加到前面
    private String device_id;//20200506
    //后期特别新增的字段,加到前面


    private int cfg_id;
    private int found_time;
    private int recv_time;
    private String trans_proto;
    private int addr_type;
    private String d_ip;
    private String s_ip;
    private int d_port;
    private int s_port;
    private int service;
    private int encap_type;
    private int direction;
    private String inner_smac;
    private String inner_dmac;
    private int stream_dir;
    private String cap_ip;
    private String addr_list;
    private String server_locate;
    private String client_locate;
    private String s_asn;
    private String d_asn;
    private String s_subscribe_id;
    private String d_subscribe_id;
    private String user_region;
    private String scene_file;
    private String scene_file_id;

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public int getCfg_id() {
        return cfg_id;
    }

    public void setCfg_id(int cfg_id) {
        this.cfg_id = cfg_id;
    }

    public int getFound_time() {
        return found_time;
    }

    public void setFound_time(int found_time) {
        this.found_time = found_time;
    }

    public int getRecv_time() {
        return recv_time;
    }

    public void setRecv_time(int recv_time) {
        this.recv_time = recv_time;
    }

    public String getTrans_proto() {
        return trans_proto;
    }

    public void setTrans_proto(String trans_proto) {
        this.trans_proto = trans_proto;
    }

    public int getAddr_type() {
        return addr_type;
    }

    public void setAddr_type(int addr_type) {
        this.addr_type = addr_type;
    }

    public String getD_ip() {
        return d_ip;
    }

    public void setD_ip(String d_ip) {
        this.d_ip = d_ip;
    }

    public String getS_ip() {
        return s_ip;
    }

    public void setS_ip(String s_ip) {
        this.s_ip = s_ip;
    }

    public int getD_port() {
        return d_port;
    }

    public void setD_port(int d_port) {
        this.d_port = d_port;
    }

    public int getS_port() {
        return s_port;
    }

    public void setS_port(int s_port) {
        this.s_port = s_port;
    }

    public int getService() {
        return service;
    }

    public void setService(int service) {
        this.service = service;
    }

    public int getEncap_type() {
        return encap_type;
    }

    public void setEncap_type(int encap_type) {
        this.encap_type = encap_type;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public String getInner_smac() {
        return inner_smac;
    }

    public void setInner_smac(String inner_smac) {
        this.inner_smac = inner_smac;
    }

    public String getInner_dmac() {
        return inner_dmac;
    }

    public void setInner_dmac(String inner_dmac) {
        this.inner_dmac = inner_dmac;
    }

    public int getStream_dir() {
        return stream_dir;
    }

    public void setStream_dir(int stream_dir) {
        this.stream_dir = stream_dir;
    }

    public String getCap_ip() {
        return cap_ip;
    }

    public void setCap_ip(String cap_ip) {
        this.cap_ip = cap_ip;
    }

    public String getAddr_list() {
        return addr_list;
    }

    public void setAddr_list(String addr_list) {
        this.addr_list = addr_list;
    }

    public String getServer_locate() {
        return server_locate;
    }

    public void setServer_locate(String server_locate) {
        this.server_locate = server_locate;
    }

    public String getClient_locate() {
        return client_locate;
    }

    public void setClient_locate(String client_locate) {
        this.client_locate = client_locate;
    }

    public String getS_asn() {
        return s_asn;
    }

    public void setS_asn(String s_asn) {
        this.s_asn = s_asn;
    }

    public String getD_asn() {
        return d_asn;
    }

    public void setD_asn(String d_asn) {
        this.d_asn = d_asn;
    }

    public String getS_subscribe_id() {
        return s_subscribe_id;
    }

    public void setS_subscribe_id(String s_subscribe_id) {
        this.s_subscribe_id = s_subscribe_id;
    }

    public String getD_subscribe_id() {
        return d_subscribe_id;
    }

    public void setD_subscribe_id(String d_subscribe_id) {
        this.d_subscribe_id = d_subscribe_id;
    }

    public String getUser_region() {
        return user_region;
    }

    public void setUser_region(String user_region) {
        this.user_region = user_region;
    }

    public String getScene_file() {
        return scene_file;
    }

    public void setScene_file(String scene_file) {
        this.scene_file = scene_file;
    }

    public String getScene_file_id() {
        return scene_file_id;
    }

    public void setScene_file_id(String scene_file_id) {
        this.scene_file_id = scene_file_id;
    }

    @Override
    public String toString() {
        return
                device_id + "#" +
                        cfg_id + "#" +
                        found_time + "#" +
                        recv_time + "#" +
                        trans_proto + "#" +
                        addr_type + "#" +
                        d_ip + "#" +
                        s_ip + "#" +
                        d_port + "#" +
                        s_port + "#" +
                        service + "#" +
                        encap_type + "#" +
                        direction + "#" +
                        inner_smac + "#" +
                        inner_dmac + "#" +
                        stream_dir + "#" +
                        cap_ip + "#" +
                        addr_list + "#" +
                        server_locate + "#" +
                        client_locate + "#" +
                        s_asn + "#" +
                        d_asn + "#" +
                        s_subscribe_id + "#" +
                        d_subscribe_id + "#" +
                        user_region + "#" +
                        scene_file + "#" +
                        scene_file_id + "#";
    }

    public String field() {
        return
                device_id + "#" +
                        cfg_id + "#" +
                        found_time + "#" +
                        recv_time + "#" +
                        trans_proto + "#" +
                        addr_type + "#" +
                        d_ip + "#" +
                        s_ip + "#" +
                        d_port + "#" +
                        s_port + "#" +
                        service + "#" +
                        encap_type + "#" +
                        direction + "#" +
                        inner_smac + "#" +
                        inner_dmac + "#" +
                        stream_dir + "#" +
                        cap_ip + "#" +
                        addr_list + "#" +
                        server_locate + "#" +
                        client_locate + "#" +
                        s_asn + "#" +
                        d_asn + "#" +
                        s_subscribe_id + "#" +
                        d_subscribe_id + "#" +
                        user_region + "#" +
                        scene_file + "#" +
                        scene_file_id + "#";
    }
}
