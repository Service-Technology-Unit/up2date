package edu.ucdavis.ucdh.stu.up2date.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;
import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

public class PollingPublisher implements SpringBatchJob {
    private final Log log;
    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;
    private String srcDriver;
    private String srcURL;
    private String srcUser;
    private String srcPassword;
    private String publisherId;
    private Up2DateService up2dateService;
    private String srcTableName;
    private String srcTimestampColumn;
    private Date srcLastPolled;
    private List<Map<String, String>> field;
    private int publisherRecordsObtained;
    private int publisherRecordsUpdated;
    private int sourceSystemRecordsRead;
    private int up2dateServiceCalls;

    public PollingPublisher() {
        this.log = LogFactory.getLog((String)this.getClass().getName());
        this.conn = null;
        this.ps = null;
        this.rs = null;
        this.srcDriver = null;
        this.srcURL = null;
        this.srcUser = null;
        this.srcPassword = null;
        this.publisherId = null;
        this.up2dateService = null;
        this.srcTableName = null;
        this.srcTimestampColumn = null;
        this.srcLastPolled = null;
        this.field = new ArrayList<Map<String, String>>();
        this.publisherRecordsObtained = 0;
        this.publisherRecordsUpdated = 0;
        this.sourceSystemRecordsRead = 0;
        this.up2dateServiceCalls = 0;
    }

    public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
        this.pollingPublisherBegin();
        while (this.rs.next()) {
            this.processUpdate();
        }
        return this.pollingPublisherEnd();
    }

    /*
     * Enabled force condition propagation
     * Lifted jumps to return sites
     */
    private void pollingPublisherBegin() throws Exception {
        String[] fieldList;
        this.log.info((Object)"PollingPublisher starting ...");
        this.log.info((Object)" ");
        this.log.info((Object)"Validating run time properties ...");
        this.log.info((Object)" ");
        if (StringUtils.isEmpty((String)this.srcDriver)) {
            throw new IllegalArgumentException("Required property \"srcDriver\" missing or invalid.");
        }
        this.log.info((Object)("srcDriver = " + this.srcDriver));
        if (StringUtils.isEmpty((String)this.srcUser)) {
            throw new IllegalArgumentException("Required property \"srcUser\" missing or invalid.");
        }
        this.log.info((Object)("srcUser = " + this.srcUser));
        if (StringUtils.isEmpty((String)this.srcPassword)) {
            throw new IllegalArgumentException("Required property \"srcPassword\" missing or invalid.");
        }
        this.log.info((Object)"srcPassword = ********");
        if (StringUtils.isEmpty((String)this.srcURL)) {
            throw new IllegalArgumentException("Required property \"srcURL\" missing or invalid.");
        }
        this.log.info((Object)("srcURL = " + this.srcURL));
        if (StringUtils.isEmpty((String)this.publisherId)) {
            throw new IllegalArgumentException("Required property \"publisherId\" missing or invalid.");
        }
        this.log.info((Object)("publisherId = " + this.publisherId));
        if (this.up2dateService == null) {
            throw new IllegalArgumentException("Required property \"up2dateService\" missing or invalid.");
        }
        this.log.info((Object)"Up2Date service validated.");
        this.log.info((Object)" ");
        this.log.info((Object)"Run time properties validated.");
        this.log.info((Object)" ");
        Properties publisher = this.up2dateService.getPublisher(this.publisherId);
        if (publisher == null) throw new IllegalArgumentException("There is no publisher on file with an ID of " + this.publisherId + ".");
        ++this.publisherRecordsObtained;
        this.srcTableName = publisher.getProperty("tableName");
        this.srcTimestampColumn = publisher.getProperty("timestampColumn");
        String lastPolled = publisher.getProperty("lastPolled");
        if (StringUtils.isNotEmpty((String)lastPolled)) {
            this.srcLastPolled = new Date(Long.parseLong(publisher.getProperty("lastPolled")));
        }
        if ((fieldList = publisher.getProperty("field").split(";")).length <= 0) throw new IllegalArgumentException("There are no source system fields on file for publisher " + this.publisherId + ".");
        int i = 0;
        while (i < fieldList.length) {
            HashMap<String, String> thisField = new HashMap<String, String>();
            String[] parts = fieldList[i].split(",");
            thisField.put("fieldName", parts[0]);
            thisField.put("columnName", parts[1]);
            this.field.add(thisField);
            ++i;
        }
        this.log.info((Object)("Publisher data obtained for publisher " + this.publisherId + "; data fields: " + this.field));
        this.log.info((Object)" ");
        String sql = "SELECT ";
        sql = String.valueOf(sql) + this.srcTimestampColumn;
        sql = String.valueOf(sql) + " AS LAST_UPDATE_DATE_TIME";
        Iterator<Map<String, String>> i2 = this.field.iterator();
        while (i2.hasNext()) {
            sql = String.valueOf(sql) + ", ";
            sql = String.valueOf(sql) + i2.next().get("columnName");
        }
        sql = String.valueOf(sql) + " FROM ";
        sql = String.valueOf(sql) + this.srcTableName;
        if (this.srcLastPolled != null) {
            sql = String.valueOf(sql) + " WHERE ";
            sql = String.valueOf(sql) + this.srcTimestampColumn;
            sql = String.valueOf(sql) + " > ?";
        }
        sql = String.valueOf(sql) + " ORDER BY LAST_UPDATE_DATE_TIME";
        Class.forName(this.srcDriver).newInstance();
        this.conn = DriverManager.getConnection(this.srcURL, this.srcUser, this.srcPassword);
        this.ps = this.conn.prepareStatement(sql);
        if (this.srcLastPolled != null) {
            this.ps.setTimestamp(1, new Timestamp(this.srcLastPolled.getTime()));
        }
        if (this.log.isDebugEnabled()) {
            this.log.debug((Object)("Using SQL: " + sql));
        }
        this.rs = this.ps.executeQuery();
        this.log.info((Object)"Connection established to source system database");
        this.log.info((Object)" ");
    }

    private List<BatchJobServiceStatistic> pollingPublisherEnd() throws Exception {
        this.rs.close();
        this.ps.close();
        this.conn.close();
        if (this.sourceSystemRecordsRead > 0) {
            if (this.up2dateService.updatePublisher(this.publisherId, this.srcLastPolled)) {
                ++this.publisherRecordsUpdated;
                if (this.log.isDebugEnabled()) {
                    this.log.debug((Object)("Last polled date/time reset to " + this.srcLastPolled));
                }
            } else {
                this.log.error((Object)"Unable to update publisher with last polled date/time");
            }
        }
        ArrayList<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
        if (this.publisherRecordsObtained > 0) {
            stats.add(new BatchJobServiceStatistic("Up2Date publisher information accessed", "integer", new BigInteger(String.valueOf(this.publisherRecordsObtained))));
        }
        if (this.sourceSystemRecordsRead > 0) {
            stats.add(new BatchJobServiceStatistic("Source system records read", "integer", new BigInteger(String.valueOf(this.sourceSystemRecordsRead))));
        }
        if (this.up2dateServiceCalls > 0) {
            stats.add(new BatchJobServiceStatistic("Calls made to the Up2Date service", "integer", new BigInteger(String.valueOf(this.up2dateServiceCalls))));
        }
        if (this.publisherRecordsUpdated > 0) {
            stats.add(new BatchJobServiceStatistic("Up2Date publisher information updated", "integer", new BigInteger(String.valueOf(this.publisherRecordsUpdated))));
        }
        this.log.info((Object)"PollingPublisher complete.");
        return stats;
    }

    private void processUpdate() throws Exception {
        ++this.sourceSystemRecordsRead;
        Timestamp lastPolled = this.rs.getTimestamp("LAST_UPDATE_DATE_TIME");
        if (lastPolled != null) {
            this.srcLastPolled = new Date(lastPolled.getTime());
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("srcLastPolled now " + this.srcLastPolled));
            }
        }
        Properties properties = new Properties();
        for (Map<String, String> thisField : this.field) {
            String fieldName = thisField.get("fieldName");
            String columnName = thisField.get("columnName");
            String fieldValue = this.rs.getString(columnName);
            if (StringUtils.isEmpty((String)fieldValue)) {
                fieldValue = "";
            }
            if (this.log.isDebugEnabled()) {
                this.log.debug((Object)("Field: " + fieldName + "; Column: " + columnName + "; Value: " + fieldValue));
            }
            properties.setProperty(fieldName, fieldValue);
        }
        this.up2dateService.post(new Update(this.publisherId, "change", properties));
        ++this.up2dateServiceCalls;
    }

    public void setSrcDriver(String srcDriver) {
        this.srcDriver = srcDriver;
    }

    public void setSrcURL(String srcURL) {
        this.srcURL = srcURL;
    }

    public void setSrcUser(String srcUser) {
        this.srcUser = srcUser;
    }

    public void setSrcPassword(String srcPassword) {
        this.srcPassword = srcPassword;
    }

    public void setPublisherId(String publisherId) {
        this.publisherId = publisherId;
    }

    public void setUp2dateService(Up2DateService up2dateService) {
        this.up2dateService = up2dateService;
    }
}

