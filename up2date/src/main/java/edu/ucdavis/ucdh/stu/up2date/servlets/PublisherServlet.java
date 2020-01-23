package edu.ucdavis.ucdh.stu.up2date.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class PublisherServlet
extends HttpServlet {
	private static final long serialVersionUID = 1;
	private DataSource dataSource = null;
	private Log log;

	public PublisherServlet() {
		this.log = LogFactory.getLog(this.getClass());
	}

	public void init() throws ServletException {
		super.init();
		ServletConfig config = this.getServletConfig();
		this.dataSource = (DataSource)WebApplicationContextUtils.getRequiredWebApplicationContext((ServletContext)config.getServletContext()).getBean("dataSource");
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String publisherIdString = this.getIdFromUrl(req, "/publisher/");
		if (this.log.isDebugEnabled()) {
			this.log.debug((Object)("New incoming GET request; publisher ID: " + publisherIdString));
		}
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			if (StringUtils.isNotEmpty(publisherIdString)) {
				conn = dataSource.getConnection();
				int publisherId = Integer.parseInt(publisherIdString);
				Properties publisher = this.fetchPublisher(publisherId, conn);
				if (publisher == null) {
					this.sendError(req, res, 404, "Publisher " + publisherId + " is not on file.");
				} else {
					String response = this.formatResponse(publisher, conn);
					res.setCharacterEncoding("UTF-8");
					res.setContentType("text/plain;charset=UTF-8");
					res.getWriter().write(response);
				}
			} else {
				this.sendError(req, res, 400, "Required URL parameter publisher ID not present");
			}
		} catch (SQLException e) {
			this.sendError(req, res, 500, "Up2Date service unable to connect to Up2Date database", e);
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception var9_9) {
				}
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception var9_11) {
					
				}
			}
		}
	}


	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String publisherIdString = this.getIdFromUrl(req, "/publisher/");
		if (this.log.isDebugEnabled()) {
			this.log.debug((Object)("New incoming POST request; publisher ID: " + publisherIdString));
		}
		Connection conn = null;
		try {
			if (StringUtils.isNotEmpty(publisherIdString)) {
				int publisherId = Integer.parseInt(publisherIdString);
				long ms = Long.parseLong(req.getParameter("lastPolled"));
				if (ms > 0) {
					Timestamp lastPolled = new Timestamp(ms);
					conn = this.dataSource.getConnection();
					PreparedStatement ps = conn.prepareStatement("UPDATE SOURCE SET LAST_POLLED=?, SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='PollingPublisher', SYSMODADDR=NULL WHERE ID=?");
					ps.setTimestamp(1, lastPolled);
					ps.setInt(2, publisherId);
					if (ps.executeUpdate() > 0) {
						String response = "Last polled date/time for publisher " + publisherId + " reset to " + lastPolled;
						if (this.log.isDebugEnabled()) {
							this.log.debug((Object)response);
						}
						res.setCharacterEncoding("UTF-8");
						res.setContentType("text/plain;charset=UTF-8");
						res.getWriter().write(response);
					} else {
						this.sendError(req, res, 404, "Publisher " + publisherId + " is not on file.");
					}
					ps.close();
				} else {
					this.sendError(req, res, 400, "Required POST parameter lastPolled missing or invalid");
				}
			} else {
				this.sendError(req, res, 400, "Required URL parameter publisher ID not present");
			}
		} catch (SQLException e) {
			this.sendError(req, res, 500, "Up2Date service unable to connect to Up2Date database", e);
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception var12_11) {
					
				}
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception var12_13) {
					
				}
			}
		}
	}

	private Properties fetchPublisher(int publisherId, Connection conn) {
		Properties properties;
		block25 : {
			properties = null;
			if (this.log.isDebugEnabled()) {
				this.log.debug((Object)("Fetching publisher data for publisher ID " + publisherId));
			}
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				try {
					String sql = "SELECT TABLE_NAME, TIMESTAMP_COLUMN, LAST_POLLED FROM SOURCE WHERE ID=?";
					if (this.log.isDebugEnabled()) {
						this.log.debug((Object)("Using the following SQL: " + sql));
					}
					ps = conn.prepareStatement(sql);
					ps.setInt(1, publisherId);
					rs = ps.executeQuery();
					if (rs.next()) {
						if (this.log.isDebugEnabled()) {
							this.log.debug((Object)("Publisher " + publisherId + " found in the Up2Date database."));
						}
						properties = new Properties();
						properties.setProperty("ID", String.valueOf(publisherId));
						properties.setProperty("TABLE_NAME", rs.getString("TABLE_NAME"));
						properties.setProperty("TIMESTAMP_COLUMN", rs.getString("TIMESTAMP_COLUMN"));
						Timestamp ts = rs.getTimestamp("LAST_POLLED");
						if (ts != null) {
							properties.setProperty("LAST_POLLED", String.valueOf(ts.getTime()));
						}
					}
				}
				catch (SQLException e) {
					this.log.error((Object)("Exception encountered fetching publisher data: " + e.getMessage()), (Throwable)e);
					if (rs != null) {
						try {
							rs.close();
						}
						catch (Exception var9_9) {
							// empty catch block
						}
					}
					if (ps == null) break block25;
					try {
						ps.close();
					}
					catch (Exception var9_10) {}
				}
			}
			finally {
				if (rs != null) {
					try {
						rs.close();
					}
					catch (Exception var9_13) {}
				}
				if (ps != null) {
					try {
						ps.close();
					}
					catch (Exception var9_14) {}
				}
			}
		}
		return properties;
	}

	private String formatResponse(Properties publisher, Connection conn) {
		String response;
		block23 : {
			response = "";
			response = String.valueOf(response) + "id=";
			response = String.valueOf(response) + publisher.getProperty("ID");
			response = String.valueOf(response) + "\ntableName=";
			response = String.valueOf(response) + publisher.getProperty("TABLE_NAME");
			response = String.valueOf(response) + "\ntimestampColumn=";
			response = String.valueOf(response) + publisher.getProperty("TIMESTAMP_COLUMN");
			response = String.valueOf(response) + "\nlastPolled=";
			response = String.valueOf(response) + publisher.getProperty("LAST_POLLED");
			response = String.valueOf(response) + "\nfield=";
			if (this.log.isDebugEnabled()) {
				this.log.debug((Object)("Fetching fields for publisher ID " + publisher.getProperty("ID")));
			}
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				try {
					String sql = "SELECT FIELD_NAME, COLUMN_NAME FROM SOURCE_FIELD WHERE SOURCE_ID=?";
					if (this.log.isDebugEnabled()) {
						this.log.debug((Object)("Using the following SQL: " + sql));
					}
					ps = conn.prepareStatement(sql);
					ps.setString(1, publisher.getProperty("ID"));
					String separator = "";
					rs = ps.executeQuery();
					while (rs.next()) {
						response = String.valueOf(response) + separator;
						response = String.valueOf(response) + rs.getString("FIELD_NAME");
						response = String.valueOf(response) + ",";
						response = String.valueOf(response) + rs.getString("COLUMN_NAME");
						separator = ";";
					}
				}
				catch (SQLException e) {
					this.log.error((Object)("Exception encountered fetching field data: " + e.getMessage()), (Throwable)e);
					if (rs != null) {
						try {
							rs.close();
						}
						catch (Exception var9_9) {
							// empty catch block
						}
					}
					if (ps == null) break block23;
					try {
						ps.close();
					}
					catch (Exception var9_10) {}
				}
			}
			finally {
				if (rs != null) {
					try {
						rs.close();
					}
					catch (Exception var9_13) {}
				}
				if (ps != null) {
					try {
						ps.close();
					}
					catch (Exception var9_14) {}
				}
			}
		}
		return response;
	}

	protected String getIdFromUrl(HttpServletRequest req, String servletPath) {
		String id = null;
		String basePath = String.valueOf(req.getContextPath()) + servletPath;
		if (req.getRequestURI().length() > basePath.length()) {
			id = req.getRequestURI().substring(basePath.length());
		}
		return id;
	}

	private void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage) throws IOException {
		this.sendError(req, res, errorCode, errorMessage, null);
	}

	private void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, Throwable throwable) throws IOException {
		if (throwable != null) {
			this.log.error((Object)("Sending error " + errorCode + "; message=" + errorMessage), throwable);
		} else if (this.log.isDebugEnabled()) {
			this.log.debug((Object)("Sending error " + errorCode + "; message=" + errorMessage));
		}
		res.setContentType("text/plain");
		res.sendError(errorCode, errorMessage);
	}
}

