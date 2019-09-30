package au.com.michaelpage.gap.broadbean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import au.com.michaelpage.gap.common.util.DatabaseManager;
import au.com.michaelpage.gap.common.util.DeleteDirUtil;
import au.com.michaelpage.gap.common.util.MailUtils;
import au.com.michaelpage.gap.common.util.SQLGeneratorHelper;
import au.com.michaelpage.gap.common.util.Util;

public class BroadBeanImport {
	
	private Logger logger = LoggerFactory.getLogger(BroadBeanImport.class);
	
	private String filename;
	private char delimiter;
	private String tableName;
	private List<String> columns = new ArrayList<String>();
	private String renamedFilename;

	public BroadBeanImport(String filename, char delimiter) throws Exception {
		super();
		this.filename = filename;
		this.delimiter = delimiter;
		generateColumnsNames();
		this.tableName = extractTableName();
	}
	
	public BroadBeanImport(String filename) throws Exception {
		this(filename, ',');
	}
	
	private String extractTableName() {
		//System.out.println(new File(this.filename).getName());
		Pattern p = Pattern.compile(".+_([^_]+)\\.csv");
		Matcher m = p.matcher(new File(this.filename).getName());
		if (m.matches()) {
			//System.out.println(m.group(1));
			return m.group(1).toUpperCase();
		}
		return null;
	}
	
	private String generateColumnsNames() throws Exception {
		StringBuilder sb = new StringBuilder();
		CSVReader reader = null;
				
		try {
			reader = new CSVReader(new FileReader(this.filename), this.delimiter);
			
			for (String columnName : reader.readNext()) {
				
				if (columnName.trim().isEmpty()) continue;
				
				sb.append(String.format("BB_%s%s", columnName.replaceAll("[^\\w]", "_"), delimiter).replaceAll("_+", "_"));
				columns.add(String.format("BB_%s", columnName.replaceAll("[^\\w]", "_")).replaceAll("_+", "_"));
			}
			//System.out.println(sb.substring(0, sb.length() - 1));
			return sb.substring(0, sb.length() - 1);
			
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
				
		
	}
	
	private BroadBeanImport renameColumns() throws Exception {
		Writer out = null;
		BufferedReader br = null;
		try {
			renamedFilename = String.format("%s.renamedColumns.csv", this.filename);
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(renamedFilename)), "UTF8"));
			br = new BufferedReader(new FileReader(this.filename));
			String sCurrentLine = br.readLine();// skip columns
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.isEmpty()) continue;
				out.append(sCurrentLine);
				out.append("\r\n");
				out.flush();
			}
			br.close();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return this;
	}
	
	private BroadBeanImport createTable() throws Exception {
		String sqlCreateTable = SQLGeneratorHelper.generateCreateTable(columns, tableName);
		//System.out.println(sqlCreateTable);
		Connection conn = DatabaseManager.INSTANCE.getConnection();
		
		PreparedStatement ps = conn.prepareStatement(sqlCreateTable);
		ps.execute();
		
		if (tableName.equalsIgnoreCase("LIVEJOBS")) {
			ps = conn.prepareStatement("create index IDX_LIVEJOBS_" + Util.getRandom(100000) + " on LIVEJOBS (BB_USER, BB_TEAM, BB_OFFICE, BB_REFERENCE)");
			ps.execute();
		}
		
		ps.close();
		conn.close();
		
		return this;
	}
	
	public BroadBeanImport importData() throws Exception {
		renameColumns();
		createTable();
		
		Connection conn = DatabaseManager.INSTANCE.getConnection();
		
		PreparedStatement ps = conn.prepareStatement("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (?,?,?,?,?,?,?)");
		ps.setString(1, null);
		ps.setString(2, tableName);
		ps.setString(3, renamedFilename);
		ps.setString(4, String.valueOf(delimiter));
		ps.setString(5, null);
		ps.setString(6, null);
		ps.setString(7, "0");
		ps.execute();
		ps.close();
		
		
		ps = conn.prepareStatement("select count(*) from " + tableName);
		ResultSet rs = ps.executeQuery();
		rs.next();
		logger.info("Imported {} records from {}", rs.getInt(1), filename);
		rs.close();
		ps.close();
		
		
		conn.close();
		
		DeleteDirUtil.delete(new File(this.renamedFilename));
		if (this.tableName.equalsIgnoreCase("resp2")) {
			checkForUnknownJobBoards();
			checkForNonNextGenJobBoards();
		}
		
		return this;
	}

	public void checkForUnknownJobBoards() {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		List<String> list = new ArrayList<String>();

		String sql = "select bb_job_board, count(*) cnt from resp2 "
				+ "where bb_job_board not in (select name from jobboard) group by bb_job_board";
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				list.add(rs.getString("bb_job_board") + " (" + rs.getInt("cnt") + ")");
			}
			if (list.size() > 0) {
				this.logger.warn("Unknown job boards found: " + list);
				//new MailUtils().sendUnknownJobBoardsEmail(list, filename);
			}
		} catch (Throwable t) {
			throw new RuntimeException("Unable to check for unknown job boards. Message: " + t.getMessage(), t);
		} finally {
			DatabaseManager.INSTANCE.closeConnection(rs, ps, conn);
		}
	}

	public void checkForNonNextGenJobBoards() {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		List<String> list = new ArrayList<String>();

		String sql = "select distinct name from jobboard where nonnextgen = true";
		try {
			conn = DatabaseManager.INSTANCE.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				list.add(rs.getString("name"));
			}
			if (list.size() > 0) {
				this.logger.info("Non NextGen job boards: " + list);
			}
		} catch (Throwable t) {
			throw new RuntimeException("Unable to check for non NextGen job boards. Message: " + t.getMessage(), t);
		} finally {
			DatabaseManager.INSTANCE.closeConnection(rs, ps, conn);
		}
	}
}
