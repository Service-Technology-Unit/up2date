package edu.ucdavis.ucdh.stu.up2date.service;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.up2date.beans.Update;

public class Up2DateService {
    private Log log;
    private String url;
    private int max;
    private int delay;
    private int runningCount;
    private HttpClient client;

    public Up2DateService(String serviceEndPoint) {
        this(serviceEndPoint, 0);
    }

    public Up2DateService(String serviceEndPoint, int maxConcurrent) {
        this.log = LogFactory.getLog(this.getClass());
        this.url = null;
        this.delay = 1;
        this.runningCount = 0;
        this.url = serviceEndPoint;
        this.max = maxConcurrent;
        try {
        	this.client = HttpClientProvider.getClient();
        } catch (Exception e) {
            this.log.error("Exception encountered while creating HTTP Client: " + e);
       }
    }

    public void post(Update update) {
        Poster poster = new Poster(this.url + "/publish", update, client);
        if (this.max > 0) {
            ++this.runningCount;
            if (this.runningCount > this.max) {
                int thisDelay = this.delay * (this.runningCount / this.max);
                poster.setDelay(thisDelay);
                if (this.log.isDebugEnabled()) {
                    this.log.debug((Object)("Initial delay set to " + thisDelay + "."));
                }
            }
        }
        poster.start();
    }

    public Properties getPublisher(String publisherId) {
        Properties publisher = null;
        String publisherUrl = this.url + "/publisher/" + publisherId;
        HttpGet get = new HttpGet(publisherUrl);
        try {
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("Obtaining publisher data from the following URL: " + publisherUrl));
            }
            HttpResponse response = client.execute((HttpUriRequest)get);
            int rc = response.getStatusLine().getStatusCode();
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
                    publisher = new Properties();
                    publisher.load(new ByteArrayInputStream(resp.getBytes()));
                }
            } else {
                try {
                    resp = EntityUtils.toString((HttpEntity)response.getEntity());
                    this.log.error((Object)("Invalid response code (" + rc + ") encountered while opening URL " + publisherUrl + "; response body: " + resp));
                }
                catch (Exception e) {
                    this.log.error((Object)("Invalid response code (" + rc + ") encountered while opening URL " + publisherUrl));
                }
            }
        }
        catch (Exception e) {
            this.log.error((Object)("Exception encountered accessing URL " + publisherUrl), (Throwable)e);
        }
        return publisher;
    }

    public boolean updatePublisher(String publisherId, Date lastPolled) {
        boolean success = false;
        for (int attempt = 0; !success && attempt < 10; ++attempt) {
            success = this.updatePublisherAttempt(publisherId, lastPolled, attempt);
        }
        return success;
    }

    private boolean updatePublisherAttempt(String publisherId, Date lastPolled, int attempt) {
        boolean success = false;
        if (attempt > 0) {
            try {
                Thread.sleep(10000 * attempt);
            }
            catch (InterruptedException e) {
                this.log.error((Object)("Sleep interrupted: " + e.getMessage()), (Throwable)e);
            }
        }
        String publisherUrl = this.url + "/publisher/" + publisherId;
        HttpPost post = new HttpPost(publisherUrl);
        try {
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("Attempt #" + (attempt + 1) + " to update publisher data using the following URL: " + publisherUrl));
            }
            ArrayList<BasicNameValuePair> postParameters = new ArrayList<BasicNameValuePair>();
            postParameters.add(new BasicNameValuePair("lastPolled", "" + lastPolled.getTime() + ""));
            post.setEntity((HttpEntity)new UrlEncodedFormEntity(postParameters));
            HttpResponse response = client.execute((HttpUriRequest)post);
            int rc = response.getStatusLine().getStatusCode();
            String resp = null;
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("HTTP Response Code: " + rc));
            }
            if (rc == 200) {
                success = true;
                if (this.log.isDebugEnabled()) {
                    this.log.debug((Object)EntityUtils.toString((HttpEntity)response.getEntity()));
                }
            } else {
                try {
                    resp = EntityUtils.toString((HttpEntity)response.getEntity());
                    this.log.error((Object)("Invalid response code (" + rc + ") encountered while opening URL " + publisherUrl + "; response body: " + resp));
                }
                catch (Exception e) {
                    this.log.error((Object)("Invalid response code (" + rc + ") encountered while opening URL " + publisherUrl));
                }
            }
        }
        catch (Exception e) {
            this.log.error((Object)("Exception encountered accessing URL " + publisherUrl), (Throwable)e);
        }
        return success;
    }
}

