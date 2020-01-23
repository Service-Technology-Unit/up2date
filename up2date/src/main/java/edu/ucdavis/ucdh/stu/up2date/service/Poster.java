/*
 * Decompiled with CFR 0_115.
 * 
 * Could not load the following classes:
 *  org.apache.commons.logging.Log
 *  org.apache.commons.logging.LogFactory
 *  org.apache.http.HttpEntity
 *  org.apache.http.HttpResponse
 *  org.apache.http.NameValuePair
 *  org.apache.http.StatusLine
 *  org.apache.http.client.HttpClient
 *  org.apache.http.client.entity.UrlEncodedFormEntity
 *  org.apache.http.client.methods.HttpPost
 *  org.apache.http.client.methods.HttpUriRequest
 *  org.apache.http.util.EntityUtils
 */
package edu.ucdavis.ucdh.stu.up2date.service;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;

import edu.ucdavis.ucdh.stu.up2date.beans.Update;

public class Poster
extends Thread {
    private static final Random RANDOM = new Random();
    @SuppressWarnings("rawtypes")
	private Log log = LogFactory.getLog((Class)Poster.class);
    private String url = null;
    private Update update = null;
    private HttpClient client = null;
    private int delay = 0;

    Poster(String url, Update update, HttpClient client) {
        this.url = url;
        this.update = update;
        this.client = client;
    }

    @Override
    public void run() {
        int rc = 0;
        for (int attempt = 0; rc != 200 && attempt < 25; ++attempt) {
            if (this.delay > 0 || attempt > 0) {
                int pause = this.computePauseSeconds(this.delay, attempt);
                if (this.log.isDebugEnabled()) {
                    this.log.debug((Object)("Poster sleeping for " + pause + " seconds."));
                }
                try {
                    Thread.sleep(1000 * pause);
                }
                catch (InterruptedException e) {
                    this.log.error((Object)("Sleep interrupted: " + e.getMessage()), (Throwable)e);
                }
            }
            rc = this.post();
        }
    }

    private int post() {
        int rc = 0;
        HttpPost post = new HttpPost(this.url);
        try {
            post.setEntity((HttpEntity)new UrlEncodedFormEntity(this.update.getPostParameters()));
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("Posting to the following URL: " + this.url));
                this.log.debug((Object)("Posting the following parameters: " + this.update.getPostParameters()));
            }
            HttpResponse response = this.client.execute((HttpUriRequest)post);
            rc = response.getStatusLine().getStatusCode();
            String resp = null;
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("HTTP Response Code: " + rc));
            }
            if (rc == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    resp = EntityUtils.toString((HttpEntity)entity);
                    if (this.log.isDebugEnabled()) {
                        this.log.debug((Object)("HTTP Response Length: " + resp.length()));
                        this.log.debug((Object)("HTTP Response: " + resp));
                    }
                }
            } else {
                try {
                    resp = EntityUtils.toString((HttpEntity)response.getEntity());
                    this.log.error((Object)("Invalid response code (" + rc + ") encountered while posting to URL " + this.url + "; response body: " + resp));
                }
                catch (Exception e) {
                    this.log.error((Object)("Invalid response code (" + rc + ") encountered while posting to URL " + this.url));
                }
            }
        }
        catch (Exception e) {
            this.log.error((Object)("Exception encountered accessing URL " + this.url), (Throwable)e);
        }
        return rc;
    }

    private int computePauseSeconds(int delay, int attempt) {
        return 10 * (delay + attempt) + RANDOM.nextInt(10);
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }
}

