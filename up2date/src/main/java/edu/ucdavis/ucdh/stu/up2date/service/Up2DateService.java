package edu.ucdavis.ucdh.stu.up2date.service;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.scheme.SchemeSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import edu.ucdavis.ucdh.stu.up2date.beans.Update;

public class Up2DateService {
    private Log log;
    private String url;
    private int max;
    private int delay;
    private int runningCount;

    public Up2DateService(String serviceEndPoint) {
        this.log = LogFactory.getLog(this.getClass());
        this.url = null;
        this.max = 0;
        this.delay = 1;
        this.runningCount = 0;
        this.url = serviceEndPoint;
    }

    public Up2DateService(String serviceEndPoint, int maxConcurrent) {
        this.log = LogFactory.getLog(this.getClass());
        this.url = null;
        this.max = 0;
        this.delay = 1;
        this.runningCount = 0;
        this.url = serviceEndPoint;
        this.max = maxConcurrent;
    }

    public void post(Update update) {
        Poster poster = new Poster(this.url + "/publish", update, this.createHttpClient());
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
            HttpResponse response = this.createHttpClient().execute((HttpUriRequest)get);
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
            HttpResponse response = this.createHttpClient().execute((HttpUriRequest)post);
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

    private HttpClient createHttpClient() {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {

                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }

                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLSocketFactory ssf = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            ClientConnectionManager ccm = httpClient.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", 443, (SchemeSocketFactory)ssf));
            httpClient = new DefaultHttpClient(ccm, httpClient.getParams());
        }
        catch (Exception e) {
            this.log.error((Object)("Exception encountered: " + e.getClass().getName() + "; " + e.getMessage()), (Throwable)e);
        }
        return httpClient;
    }

}

