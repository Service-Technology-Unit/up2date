package edu.ucdavis.ucdh.stu.up2date.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;

public class PublishServlet
extends HttpServlet {
    private static final long serialVersionUID = 1;
    private DataSource dataSource = null;
    private Log log;

    public PublishServlet() {
        log = LogFactory.getLog(getClass());
    }

    public void init() throws ServletException {
        super.init();
        ServletConfig config = getServletConfig();
        dataSource = (DataSource)WebApplicationContextUtils.getRequiredWebApplicationContext((ServletContext)config.getServletContext()).getBean("dataSource");
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        sendError(req, res, 0, null, 405, "The GET method is not allowed for this URL");
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        block20 : {
            String publisherIdString = req.getParameter("_pid");
            if (log.isDebugEnabled()) {
                log.debug((Object)("New incoming request; publisher ID: " + publisherIdString));
            }
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
                int requestId = logIncomingRequest(req, conn);
                if (StringUtils.isNotEmpty(publisherIdString)) {
                    int publisherId = Integer.parseInt(publisherIdString);
                    String publisher = fetchPublisher(publisherId, conn);
                    if (StringUtils.isNotEmpty(publisher)) {
                        List<Map<String, String>> subscriber = fetchSubscribers(publisherId, conn);
                        if (subscriber.size() > 0) {
                            if (log.isDebugEnabled()) {
                                log.debug((Object)("" + subscriber.size() + " subscription(s) on file for publisher ID " + publisherId + "; preparing to contact subscribers"));
                            }
                            List<String> field = fetchFields(publisherId, conn);
                            for (int i = 0; i < subscriber.size(); ++i) {
                                postToSubscriber(req, requestId, field, subscriber.get(i), conn);
                            }
                            String response = "0;Update posted to " + subscriber.size() + " subscribers.";
                            res.setCharacterEncoding("UTF-8");
                            res.setContentType("text/plain;charset=UTF-8");
                            res.getWriter().write(response);
                            updateRequestLog(req, requestId, field, 200, response, conn);
                        } else {
                            sendError(req, res, requestId, conn, 204, "There are no subscribers on file for publisher ID " + publisherId);
                        }
                    } else {
                        sendError(req, res, requestId, conn, 400, "Invalid publisher ID parameter (\"_pid\"): " + publisherId);
                    }
                    break block20;
                }
                sendError(req, res, requestId, conn, 400, "Required parameter publisher ID (\"_pid\") not present");
            }
            catch (SQLException e) {
                log.error((Object)("Exception encountered connecting to up2date database: " + e.getMessage()), (Throwable)e);
                sendError(req, res, 0, conn, 500, "Up2Date service unable to connect to Up2Date database");
            }
            finally {
                if (conn != null) {
                    try {
                        conn.close();
                    }
                    catch (Exception e) {}
                }
            }
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private int logIncomingRequest(HttpServletRequest req, Connection conn) {
        int requestId;
        requestId = 0;
        String publisherId = req.getParameter("_pid");
        if (log.isDebugEnabled()) {
            log.debug((Object)("Logging new request for publisher ID " + publisherId));
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "INSERT INTO REQUEST_LOG (SOURCE_ID, JOB_ID, ACTION, START_TIME, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) OUTPUT Inserted.ID VALUES(?, ?, ?, getdate(), 0, 'Up2Date', getdate(), ?)";
            if (log.isDebugEnabled()) {
                log.debug((Object)("Using the following SQL: " + sql));
            }
            ps = conn.prepareStatement(sql);
            ps.setString(1, publisherId);
            ps.setString(2, req.getParameter("_jid"));
            ps.setString(3, capitalize(req.getParameter("_action")));
            ps.setString(4, req.getRemoteAddr());
            rs = ps.executeQuery();
            if (rs.next()) {
                requestId = rs.getInt(1);
            }
        }
        catch (SQLException e) {
            log.error((Object)("Exception encountered logging request: " + e.getMessage()), (Throwable)e);
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (Exception e) {}
            }
            if (ps != null) {
                try {
                    ps.close();
                }
                catch (Exception e) {}
            }
        }
        return requestId;
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private String fetchPublisher(int publisherId, Connection conn) {
        String publisher;
        publisher = "";
        if (log.isDebugEnabled()) {
            log.debug((Object)("Fetching publisher data for publisher ID " + publisherId));
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "SELECT NAME FROM SOURCE WHERE ID=?";
            if (log.isDebugEnabled()) {
                log.debug((Object)("Using the following SQL: " + sql));
            }
            ps = conn.prepareStatement(sql);
            ps.setInt(1, publisherId);
            rs = ps.executeQuery();
            while (rs.next()) {
                publisher = rs.getString("NAME");
            }
        }
        catch (SQLException e) {
            log.error((Object)("Exception encountered fetching publisher data: " + e.getMessage()), (Throwable)e);
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (Exception e) {}
            }
            if (ps != null) {
                try {
                    ps.close();
                }
                catch (Exception e) {}
            }
        }
        return publisher;
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private List<Map<String, String>> fetchSubscribers(int publisherId, Connection conn) {
        ArrayList<Map<String, String>> subscriber;
        subscriber = new ArrayList<Map<String, String>>();
        if (log.isDebugEnabled()) {
            log.debug((Object)("Fetching subscribers for publisher ID " + publisherId));
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "SELECT ID, URL FROM SUBSCRIPTION WHERE SOURCE_ID=?";
            if (log.isDebugEnabled()) {
                log.debug((Object)("Using the following SQL: " + sql));
            }
            ps = conn.prepareStatement(sql);
            ps.setInt(1, publisherId);
            rs = ps.executeQuery();
            while (rs.next()) {
                HashMap<String, String> thisSubscriber = new HashMap<String, String>();
                thisSubscriber.put("id", rs.getString("ID"));
                thisSubscriber.put("url", rs.getString("URL"));
                subscriber.add(thisSubscriber);
            }
        }
        catch (SQLException e) {
            log.error((Object)("Exception encountered fetching subscriber data: " + e.getMessage()), (Throwable)e);
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (Exception e) {}
            }
            if (ps != null) {
                try {
                    ps.close();
                }
                catch (Exception e) {}
            }
        }
        return subscriber;
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private List<String> fetchFields(int publisherId, Connection conn) {
        ArrayList<String> field;
        field = new ArrayList<String>();
        if (log.isDebugEnabled()) {
            log.debug((Object)("Fetching fields for publisher ID " + publisherId));
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "SELECT FIELD_NAME FROM SOURCE_FIELD WHERE SOURCE_ID=?";
            if (log.isDebugEnabled()) {
                log.debug((Object)("Using the following SQL: " + sql));
            }
            ps = conn.prepareStatement(sql);
            ps.setInt(1, publisherId);
            rs = ps.executeQuery();
            while (rs.next()) {
                field.add(rs.getString("FIELD_NAME"));
            }
        }
        catch (SQLException e) {
            log.error((Object)("Exception encountered fetching field data: " + e.getMessage()), (Throwable)e);
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (Exception e) {}
            }
            if (ps != null) {
                try {
                    ps.close();
                }
                catch (Exception e) {}
            }
        }
        return field;
    }

    private void postToSubscriber(HttpServletRequest req, int requestId, List<String> field, Map<String, String> subscriber, Connection conn) {
        HttpPost post = new HttpPost(subscriber.get("url"));
        Date start = new Date();
        String contentType = "";
        String resp = "";
        int rc = 0;
        try {
            HttpClient client = HttpClientProvider.getClient();
            HttpResponse response;
            ArrayList<BasicNameValuePair> urlParameters = new ArrayList<BasicNameValuePair>();
            urlParameters.add(new BasicNameValuePair("_rid", "" + requestId + ""));
            urlParameters.add(new BasicNameValuePair("_sid", subscriber.get("id")));
            urlParameters.add(new BasicNameValuePair("_pid", req.getParameter("_pid")));
            urlParameters.add(new BasicNameValuePair("_jid", req.getParameter("_jid")));
            urlParameters.add(new BasicNameValuePair("_action", req.getParameter("_action")));
            for (String fieldName : field) {
                urlParameters.add(new BasicNameValuePair(fieldName, req.getParameter(fieldName)));
            }
            post.setEntity((HttpEntity)new UrlEncodedFormEntity(urlParameters));
            if (log.isDebugEnabled()) {
                log.debug((Object)("Posting to the following URL: " + subscriber.get("url")));
                log.debug((Object)("Posting the following parameters: " + urlParameters));
            }
            if ((rc = (response = client.execute((HttpUriRequest)post)).getStatusLine().getStatusCode()) == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    Header header = entity.getContentType();
                    if (header != null) {
                        contentType = header.getValue();
                    }
                    resp = EntityUtils.toString((HttpEntity)entity);
                    if (log.isDebugEnabled()) {
                        log.debug((Object)("HTTP Response Length: " + resp.length()));
                        log.debug((Object)("HTTP Response: " + resp));
                    }
                }
            } else {
                log.error((Object)("Invalid response code (" + response.getStatusLine().getStatusCode() + ") encountered accessing to URL " + subscriber.get("url")));
                try {
                    resp = EntityUtils.toString((HttpEntity)response.getEntity());
                }
                catch (Exception e) {}
            }
        }
        catch (Exception e) {
            log.error((Object)("Exception encountered accessing URL " + subscriber.get("url")), (Throwable)e);
            resp = ExceptionUtils.getStackTrace((Throwable)e);
        }
        logActivity(req, requestId, subscriber, contentType, rc, resp, start, conn);
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private void logActivity(HttpServletRequest req, int requestId, Map<String, String> subscriber, String contentType, int rc, String response, Date start, Connection conn) {
        block24 : {
            if (log.isDebugEnabled()) {
                log.debug((Object)("Logging results for " + subscriber.get("url")));
            }
            PreparedStatement ps = null;
            ResultSet rs = null;
            String returnCode = null;
            String messageText = null;
            if (StringUtils.isNotEmpty(response) && response.indexOf(";") == 1) {
                returnCode = response.substring(0, 1);
                messageText = response.substring(2);
            }
            try {
                String sql = "INSERT INTO ACTIVITY_LOG (REQUEST_ID, SUBSCRIPTION_ID, SUCCESS, START_TIME, END_TIME, RETURN_CODE, MESSAGE_TEXT, CONTENT_TYPE, RESPONSE_CODE, RESPONSE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'up2date', getdate(), ?)";
                if (log.isDebugEnabled()) {
                    log.debug((Object)("Using the following SQL: " + sql));
                }
                ps = conn.prepareStatement(sql);
                ps.setInt(1, requestId);
                ps.setInt(2, convertToInteger(subscriber.get("id")));
                ps.setInt(3, rc == 200 && response.startsWith("0") ? 1 : 0);
                ps.setTimestamp(4, new Timestamp(start.getTime()));
                ps.setTimestamp(5, new Timestamp(new Date().getTime()));
                ps.setString(6, returnCode);
                ps.setString(7, messageText);
                ps.setString(8, contentType);
                ps.setInt(9, rc);
                ps.setString(10, response);
                ps.setString(11, req.getRemoteAddr());
                if (ps.executeUpdate() > 0) {
                    if (log.isDebugEnabled()) {
                        log.debug((Object)("Results logged for " + subscriber.get("url")));
                    }
                    break block24;
                }
                log.error((Object)("Unable to log results for " + subscriber.get("url")));
            }
            catch (SQLException e) {
                log.error((Object)("Exception encountered logging results for " + subscriber.get("url")), (Throwable)e);
            }
            finally {
                if (rs != null) {
                    try {
                        rs.close();
                    }
                    catch (Exception e) {}
                }
                if (ps != null) {
                    try {
                        ps.close();
                    }
                    catch (Exception e) {}
                }
            }
        }
    }

    private String capitalize(String string) {
        String response = "";
        if (StringUtils.isNotEmpty(string)) {
            response = string.substring(0, 1).toUpperCase() + string.substring(1).toLowerCase();
        }
        return response;
    }

    private Integer convertToInteger(String string) {
        Integer response = null;
        if (StringUtils.isNotEmpty(string)) {
            response = Integer.parseInt(string);
        }
        return response;
    }

    private void sendError(HttpServletRequest req, HttpServletResponse res, int requestId, Connection conn, int errorCode, String errorMessage) throws IOException {
        sendError(req, res, requestId, conn, errorCode, errorMessage, null);
    }

    private void sendError(HttpServletRequest req, HttpServletResponse res, int requestId, Connection conn, int errorCode, String errorMessage, Throwable throwable) throws IOException {
        if (throwable != null) {
            log.error((Object)("Sending error " + errorCode + "; message=" + errorMessage), throwable);
        } else if (log.isDebugEnabled()) {
            log.debug((Object)("Sending error " + errorCode + "; message=" + errorMessage));
        }
        res.setContentType("text/plain");
        res.sendError(errorCode, errorMessage);
        if (conn != null) {
            updateRequestLog(req, requestId, null, errorCode, errorMessage, conn);
        } else {
            log.error((Object)"Unable to update request log entry due to connection issues");
        }
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private void updateRequestLog(HttpServletRequest req, int requestId, List<String> field, int responseCode, String response, Connection conn) {
        block16 : {
            if (log.isDebugEnabled()) {
                log.debug((Object)("Updating request log for request " + requestId));
            }
            PreparedStatement ps = null;
            try {
                String sql = "UPDATE REQUEST_LOG SET END_TIME=getdate(), DATA_VALUES=?, RESPONSE_CODE=?, RESPONSE=?, SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='Up2Date', SYSMODTIME=getdate(), SYSMODADDR=? WHERE ID=?";
                if (log.isDebugEnabled()) {
                    log.debug((Object)("Using the following SQL: " + sql));
                }
                ps = conn.prepareStatement(sql);
                ps.setString(1, getDataValues(req, field));
                ps.setString(2, "" + responseCode + "");
                ps.setString(3, response);
                ps.setString(4, req.getRemoteAddr());
                ps.setInt(5, requestId);
                if (ps.executeUpdate() > 0) {
                    if (log.isDebugEnabled()) {
                        log.debug((Object)"Request log entry updated");
                    }
                    break block16;
                }
                log.error((Object)("Unable to update request log for request " + requestId));
            }
            catch (SQLException e) {
                log.error((Object)("Exception encountered updating request log for request " + requestId + ": " + e.getMessage()), (Throwable)e);
            }
            finally {
                if (ps != null) {
                    try {
                        ps.close();
                    }
                    catch (Exception e) {}
                }
            }
        }
    }

    private String getDataValues(HttpServletRequest req, List<String> field) {
        String response = "";
        if (field != null && field.size() > 0) {
            String separator = "";
            for (String fieldName : field) {
                response = response + separator;
                response = response + fieldName;
                response = response + "=";
                response = response + req.getParameter(fieldName);
                separator = "\n";
            }
        }
        return response;
    }

}

