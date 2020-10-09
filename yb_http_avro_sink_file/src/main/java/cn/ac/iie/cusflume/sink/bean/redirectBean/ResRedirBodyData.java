package cn.ac.iie.cusflume.sink.bean.redirectBean;

/**
 * ProReirBodyData中data的对象形式-20200818新增
 */
public class ResRedirBodyData {
    private String redirect;

    public String getRedirect() {
        return redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    @Override
    public String toString() {
        return "ResRedirBodyData{" +
                "redirect='" + redirect + '\'' +
                '}';
    }
}
