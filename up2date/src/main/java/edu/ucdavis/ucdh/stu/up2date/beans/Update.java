/*
 * Decompiled with CFR 0_115.
 * 
 * Could not load the following classes:
 *  org.apache.http.NameValuePair
 *  org.apache.http.message.BasicNameValuePair
 */
package edu.ucdavis.ucdh.stu.up2date.beans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

public class Update
implements Serializable {
    private static final long serialVersionUID = 1;
    private String publisherId = null;
    private String jobId = null;
    private String action = null;
    private Properties properties = null;

    public Update(String publisherId, String action, Properties properties) throws IllegalArgumentException {
        this(publisherId, null, action, properties);
    }

    public Update(String publisherId, String jobId, String action, Properties properties) throws IllegalArgumentException {
        this.publisherId = publisherId;
        this.jobId = jobId;
        this.action = action;
        this.properties = properties;
        if (!Update.isInteger(publisherId)) {
            throw new IllegalArgumentException("\"" + publisherId + "\" is not a valid publisher ID.");
        }
        if (!("add".equalsIgnoreCase(action) || "change".equalsIgnoreCase(action) || "delete".equalsIgnoreCase(action))) {
            throw new IllegalArgumentException("\"" + action + "\" is not a valid action.");
        }
        action = action.toLowerCase();
    }

    private static boolean isInteger(String string) {
        boolean isInteger = false;
        try {
            Integer.parseInt(string);
            isInteger = true;
        }
        catch (Exception e) {
            // empty catch block
        }
        return isInteger;
    }

    public List<NameValuePair> getPostParameters() {
        ArrayList<NameValuePair> postParameters = new ArrayList<NameValuePair>();
        postParameters.add((NameValuePair)new BasicNameValuePair("_pid", this.publisherId));
        postParameters.add((NameValuePair)new BasicNameValuePair("_jid", this.jobId));
        postParameters.add((NameValuePair)new BasicNameValuePair("_action", this.action));
        Iterator<Object> i = this.properties.keySet().iterator();
        while (i.hasNext()) {
            String thisField = i.next().toString();
            postParameters.add((NameValuePair)new BasicNameValuePair(thisField, this.properties.getProperty(thisField)));
        }
        return postParameters;
    }

    public String getPublisherId() {
        return this.publisherId;
    }

    public String getJobId() {
        return this.jobId;
    }

    public String getAction() {
        return this.action;
    }

    public Properties getProperties() {
        return this.properties;
    }
}

