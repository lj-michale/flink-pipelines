package com.turing.java.flink20.pipeline.demo10;


public class FlinkUser {
    public Long id;

    public String name;

    public Long createtime;

    // 一定要提供一个 空参 的构造器(反射的时候要使用)
    public FlinkUser() {
    }

    public FlinkUser(Long id, String name, Long createtime) {
        this.id = id;
        this.name = name;
        this.createtime = createtime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getCreatetime() {
        return createtime;
    }

    public void setCreatetime(Long createtime) {
        this.createtime = createtime;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public String toString() {
        return "FlinkUser{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", createtime=" + createtime +
                '}';
    }
}
