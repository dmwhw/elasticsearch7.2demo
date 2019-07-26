package com.haowen;

import java.util.Date;

public class Book {
    private String number;
    private Double price;
    private String title;
    private String province;
    private Date publishTime;

    public String getNumber() {
        return number;
    }

    public Book setNumber(String number) {
        this.number = number;
        return this;
    }

    public Double getPrice() {
        return price;
    }

    public Book setPrice(Double price) {
        this.price = price;
        return this;

    }

    public String getTitle() {
        return title;
    }

    public Book setTitle(String title) {
        this.title = title;
        return this;

    }

    public String getProvince() {
        return province;
    }

    public Book setProvince(String province) {
        this.province = province;
        return this;

    }

    public Date getPublishTime() {
        return publishTime;
    }

    public Book setPublishTime(Date publishTime) {
        this.publishTime = publishTime;
        return this;

    }

    @Override
    public String toString() {
        return "Book [number=" + number + ", price=" + price + ", title=" + title + ", province=" + province
            + ", publishTime=" + publishTime + "]";
    }

}