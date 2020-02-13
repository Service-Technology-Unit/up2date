package edu.ucdavis.ucdh.stu.up2date.batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
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

public class IdBasedPublisher implements SpringBatchJob {
    private final Log log;
    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;
    private String srcDriver;
    private String srcURL;
    private String srcUser;
    private String srcPassword;
    private String srcIdColumn;
    private String publisherId;
    private String idFile;
    private String idValueList;
    private Up2DateService up2dateService;
    private String srcTableName;
    private String[] idValue;
    private List<Map<String, String>> field;
    private int publisherRecordsObtained;
    private int sourceSystemRecordsRead;
    private int up2dateServiceCalls;

    public IdBasedPublisher() {
        this.log = LogFactory.getLog((String)this.getClass().getName());
        this.conn = null;
        this.ps = null;
        this.rs = null;
        this.srcDriver = null;
        this.srcURL = null;
        this.srcUser = null;
        this.srcPassword = null;
        this.srcIdColumn = null;
        this.publisherId = null;
        this.idFile = null;
        this.idValueList = null;
        this.up2dateService = null;
        this.srcTableName = null;
        this.idValue = new String[0];
        this.field = new ArrayList<Map<String, String>>();
        this.publisherRecordsObtained = 0;
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
        if (StringUtils.isEmpty((String)this.srcIdColumn)) {
            throw new IllegalArgumentException("Required property \"srcIdColumn\" missing or invalid.");
        }
        this.log.info((Object)("srcIdColumn = " + this.srcIdColumn));
        if (StringUtils.isEmpty((String)this.publisherId)) {
            throw new IllegalArgumentException("Required property \"publisherId\" missing or invalid.");
        }
        this.log.info((Object)("publisherId = " + this.publisherId));
        if (StringUtils.isEmpty((String)this.idFile)) {
            if (StringUtils.isEmpty((String)this.idValueList)) {
                throw new IllegalArgumentException("Neither an \"idFile\" nor an \"idValueList\" property specified; one of these must be provided.");
            }
            this.log.info((Object)("idValueList = " + this.idValueList));
            this.idValue = this.idValueList.split(",");
            this.log.info((Object)("" + this.idValue.length + " ID(s) found in the idValueList"));
        } else {
            this.log.info((Object)("idFile = " + this.idFile));
            this.idValue = IdBasedPublisher.loadDataFromIdFile(this.idFile);
            this.log.info((Object)("" + this.idValue.length + " ID(s) found in the idFile"));
        }
        if (this.idValue.length == 0) {
            throw new IllegalArgumentException("No IDs to process.");
        }
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
        String[] fieldList = publisher.getProperty("field").split(";");
        if (fieldList.length <= 0) throw new IllegalArgumentException("There are no source system fields on file for publisher " + this.publisherId + ".");
        for (int i = 0; i < fieldList.length; ++i) {
            HashMap<String, String> thisField = new HashMap<String, String>();
            String[] parts = fieldList[i].split(",");
            thisField.put("fieldName", parts[0]);
            thisField.put("columnName", parts[1]);
            this.field.add(thisField);
        }
        this.log.info((Object)("Publisher data obtained for publisher " + this.publisherId + "; data fields: " + this.field));
        this.log.info((Object)" ");
        String sql = "SELECT ";
        sql = sql + this.srcIdColumn;
        sql = sql + " AS PRIMARY_ID_1";
        Iterator<Map<String, String>> i2 = this.field.iterator();
        while (i2.hasNext()) {
            sql = sql + ", ";
            sql = sql + i2.next().get("columnName");
        }
        sql = sql + " FROM ";
        sql = sql + this.srcTableName;
        sql = sql + " WHERE (";
        String separator = "";
        for (int x = 0; x < this.idValue.length; ++x) {
            sql = sql + separator;
            sql = sql + this.srcIdColumn;
            sql = sql + "='";
            sql = sql + this.idValue[x];
            sql = sql + "'";
            separator = " OR ";
        }
        sql = sql + ") ORDER BY ";
        sql = sql + this.srcIdColumn;
        Class.forName(this.srcDriver);
        this.conn = DriverManager.getConnection(this.srcURL, this.srcUser, this.srcPassword);
        this.ps = this.conn.prepareStatement(sql);
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
        ArrayList<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
        stats.add(new BatchJobServiceStatistic("Total number of IDs requested", "integer", new BigInteger("" + this.idValue.length + "")));
        if (this.publisherRecordsObtained > 0) {
            stats.add(new BatchJobServiceStatistic("Up2Date publisher information accessed", "integer", new BigInteger("" + this.publisherRecordsObtained + "")));
        }
        if (this.sourceSystemRecordsRead > 0) {
            stats.add(new BatchJobServiceStatistic("Source system records read", "integer", new BigInteger("" + this.sourceSystemRecordsRead + "")));
        }
        if (this.up2dateServiceCalls > 0) {
            stats.add(new BatchJobServiceStatistic("Calls made to the Up2Date service", "integer", new BigInteger("" + this.up2dateServiceCalls + "")));
        }
        this.log.info((Object)"PollingPublisher complete.");
        return stats;
    }

    private void processUpdate() throws Exception {
        ++this.sourceSystemRecordsRead;
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

    private static String[] loadDataFromIdFile(String fileName) throws Exception {
        ArrayList<String> value = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line = reader.readLine();
        while (line != null) {
            value.add(line);
            line = reader.readLine();
        }
        reader.close();
        return value.toArray(new String[0]);
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

    public void setSrcIdColumn(String srcIdColumn) {
        this.srcIdColumn = srcIdColumn;
    }

    public void setPublisherId(String publisherId) {
        this.publisherId = publisherId;
    }

    public void setIdFile(String idFile) {
        this.idFile = idFile;
    }

    public void setIdValueList(String idValueList) {
        this.idValueList = idValueList;
    }

    public void setUp2dateService(Up2DateService up2dateService) {
        this.up2dateService = up2dateService;
    }
}

