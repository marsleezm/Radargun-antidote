package org.radargun.tpcw.domain;

import java.io.Serializable;
import java.sql.Date;

import org.radargun.CacheWrapper;
import org.radargun.tpcw.DomainObject;

public class Book implements Serializable, DomainObject {

    private String title;
    private Date pubDate;
    private String publisher;
    private String subject;
    private String description;
    private String thumbnail;
    private String image;
    private double srp;
    private double cost;
    private Date avail;
    private int stock;
    private String isbn;
    private int page;
    private String backing;
    private String dimensions;
    private String uuid;
    
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getPubDate() {
        return pubDate;
    }

    public void setPubDate(Date pubDate) {
        this.pubDate = pubDate;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public double getSrp() {
        return srp;
    }

    public void setSrp(double srp) {
        this.srp = srp;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public Date getAvail() {
        return avail;
    }

    public void setAvail(Date avail) {
        this.avail = avail;
    }

    public int getStock() {
        return stock;
    }

    public void setStock(int stock) {
        this.stock = stock;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public String getBacking() {
        return backing;
    }

    public void setBacking(String backing) {
        this.backing = backing;
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    public String getUUID() {
        return uuid;
    }

    public void setUUID(String uuid) {
        this.uuid = uuid;
    }

    private String getKey() {
        return "BOOK_" + this.uuid;
    }
    
    @Override
    public void store(CacheWrapper wrapper) throws Throwable {
        wrapper.put(null, this.getKey(), this);
    }

    @Override
    public void store(CacheWrapper wrapper, int nodeIndex) throws Throwable {
        // TODO Auto-generated method stub
        store(wrapper);
    }

    @Override
    public void storeToPopulate(CacheWrapper wrapper, int nodeIndex, boolean localOnly) throws Throwable {
        // TODO Auto-generated method stub
        store(wrapper, nodeIndex);
    }

    @Override
    public boolean load(CacheWrapper wrapper) throws Throwable {
        // TODO Auto-generated method stub
        return false;
    }

}
