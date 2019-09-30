package au.com.michaelpage.gap.broadbean;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.IOUtils;
import com.google.common.base.Joiner;

import au.com.michaelpage.gap.common.util.DeleteDirUtil;

public class BroadBeanExtractor {
	
	Logger logger = LoggerFactory.getLogger(BroadBeanExtractor.class);
	
	// APAUGap-support@michaelpage.com.au
	public static final String ADCOURIER_PANEL_USERNAME = "m8HQUZZZ1kaYD9hIpfK1vDkEYVsjbpsErw+P8CFOWfWN0Em154vQeBbyWa8czFCJ";
	public static final String ADCOURIER_PANEL_PASSWORD = "SyMSKdzbwc4vK86LGz/mTA==";
	
	private static final String ADCOURIER_PANEL_LOGIN_FORM_URL = "https://panel.adcourier.com/";
	private static final String ADCOURIER_URL = "https://www.adcourier.com";
	private static final String ADCOURIER_LOGIN_FORM_URL = String.format("%s/login.cgi", ADCOURIER_URL);
	private static final String ADCOURIER_ACTIVITY_REPORTS_URL = String.format("%s/reports/downloadable_reports.cgi", ADCOURIER_URL);
	private static final String ADCOURIER_LIVEJOBS_REPORTS_URL = String.format("%s/reports/reports.cgi", ADCOURIER_URL);
	
	private String quote;
	private String username;
	private String password;
	private String separator;
	private String language;
	private List<String> cookies = new ArrayList<String>();
	private HttpURLConnection conn;

	private final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36";
	
	public BroadBeanExtractor(String username, String password, String separator, String language) {
		this.username = username;
		this.password = password;
		this.separator = separator;
		this.language = language;
		this.quote = "\"";
		this.cookies = cookies;
	}
	
	public BroadBeanExtractor(String username, String password, String separator, String language, List<String> cookies) {
		this(username,password,separator,language);
		this.cookies = cookies;
	}
	
	private void loginPanel() throws Exception {
		cookies = new ArrayList<String>();
		String loginPage = getPageContent(ADCOURIER_PANEL_LOGIN_FORM_URL);// get login form
		String postParams = getFormParams(loginPage, this.username, this.password); // get login params from the form
		sendPost(ADCOURIER_PANEL_LOGIN_FORM_URL, postParams); // submit the form with login params
	}
	
	private void loginClient() throws Exception {
		//cookies = new ArrayList<String>();
		
		/*
		String loginPage = getPageContent(ADCOURIER_LOGIN_FORM_URL);// get login form
		String postParams = getFormParams(loginPage, this.username, this.password); // get login params from the form
		sendPost(ADCOURIER_LOGIN_FORM_URL, postParams); // submit the form with login params
		String homePage = getPageContent(ADCOURIER_URL); // go to home page to get new cookie
		*/
		
		// https://panel.adcourier.com/panel_login.cgi?username=admin@superadmin.pageapactest2
		
		String homePage = getPageContent("https://panel.adcourier.com/panel_login.cgi?username=" + this.username); // go to home page to get new cookie
	}
	
	private List<BroadBeanClient> extractBroadBeanClients(Document doc) {
		List<BroadBeanClient> bbcs = new ArrayList<BroadBeanClient>();
		
		for (Element div : doc.select("div.collapseable")) {

			BroadBeanClientBuilder bbcb = new BroadBeanClientBuilder();
			
			for (Element link : div.select("a")) {
				
				if (link.text().equalsIgnoreCase("login")) {
					bbcb.setSite(link.attr("href"));
					
					bbcb.setUsername(link.attr("href").split("\\?")[1].split(";")[0].split("=")[1]);
						//.setPassword(link.attr("href").split("\\?")[1].split(";")[1].split("=")[1]);
				} else {
					bbcb.setId(link.text());
					bbcb.setSite(link.attr("href").substring(link.attr("href").indexOf("'") + 1, link.attr("href").lastIndexOf("'")));
				}
			}
			
			bbcs.add(bbcb.createBroadBeanClient());
		}
		return bbcs;
	}
	
	protected List<BroadBeanClient> extractBroadBeanClients(String page) {
		return extractBroadBeanClients(Jsoup.parse(page));
	}
	
	protected List<BroadBeanClient> extractBroadBeanClients(File file) throws Exception {
		return extractBroadBeanClients(Jsoup.parse(file, "UTF-8"));
	}
	
	public List<BroadBeanClient> extractSites() throws Exception {
		loginPanel();
		
		String homePageHtml = getPageContent(ADCOURIER_PANEL_LOGIN_FORM_URL);
		
		return extractBroadBeanClients(homePageHtml);
	}
	
	public BroadBeanClient extractSite(String site) throws Exception {
		for (BroadBeanClient bbc : extractSites()) {
			if (bbc.getSite().equalsIgnoreCase(site)) {
				return bbc;
			}
		}

		return null;
	}
	
	// example params "cand_resp", "07 June 2015", "08 June 2015","C:\\GTS\\BB"
	public BroadBeanExtractor extractActivityReportTo(String format, String from, String to, String toFile) throws Exception {
		
		logger.info("Starting Activity Report extract: (format: {}, output file:{})", format, toFile);
		
		loginClient();
		
		downloadFile(ADCOURIER_ACTIVITY_REPORTS_URL, String.format("type=ACTI&stats_from=%s&stats_to=%s&format=%s&compile=compile", URLEncoder.encode(from, "UTF-8"), URLEncoder.encode(to, "UTF-8"), format), toFile);
		
		logger.info("Finished Activity Report extract: (format: {}, output file:{}, number of lines in file: {})", format, toFile, countLines(toFile));
		
		return this;
	}
	
	
	public void removeNestedQuotes(String toFile) {

		String suffix = "_withoutNestedQuotes";
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			File outFile = new File(toFile + suffix);
			
			if (!outFile.exists()) {
				outFile.createNewFile();
			}

			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(toFile)), "UTF8"));
			
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), "UTF8"));
			
			String sCurrentLine = br.readLine();
			bw.write(sCurrentLine);
			bw.write("\n");
			bw.flush();
			
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.trim().length() == 0) {
					bw.write("\n");
					bw.flush();
					continue;
				}
				String[] cols = sCurrentLine.split(quote + separator + quote);
				
				// strip any double quotes found inside the column value
				for (int i = 0; i < cols.length; i++) {
					cols[i] = "\"" + cols[i].replaceAll("\"", "") + "\"";
				}
				bw.write(Joiner.on(separator).join(cols) + "\n");
				bw.flush();
			}
			bw.close();
			
			copyFile(outFile, new File(toFile));
			DeleteDirUtil.delete(outFile);

		} catch (IOException e) {
			logger.error("Error validating CSv File ({})", e);
			throw new RuntimeException("Error validating CSV file", e);
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				logger.error("Error closing buffered reader", ex);
			}
			try {
				if (bw != null)bw.close();
			} catch (IOException ex) {
				logger.error("Error closing buffered writer", ex);
			}
		}

	}
	
	public BroadBeanExtractor extractActivityReportTo(String format, Date extractDate, String toFile) throws Exception {

		String fromTo = new SimpleDateFormat("dd MMMMM yyyy", Locale.forLanguageTag(language)).format(extractDate);
		
		extractActivityReportTo(format, fromTo, fromTo, toFile);
		validateActivityReport(toFile);
		
		removeNestedQuotes(toFile);
		
		if (!language.equalsIgnoreCase("en")) {
			// convert file to english
			
			BufferedReader br = null;
			BufferedWriter bw = null;
			try {
				File outFile = new File(toFile + "_" + language);
				
				if (!outFile.exists()) {
					outFile.createNewFile();
				}

				br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(toFile)), "UTF8"));
				
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), "UTF8"));
				
				bw.write("Date/Time Posted,Consultant Name,Team,Office,Job Reference,Job Classification,Job Title,Unique Advert ID,Job Board,Response Email Address,Date and time received,Ranking,Emailed,\n");
				bw.write("\n");
				
				String sCurrentLine = br.readLine();// skip first line
				int line = 1;
				logger.debug(sCurrentLine);
				while ((sCurrentLine = br.readLine()) != null) {
					if (sCurrentLine.trim().length() == 0) continue;
					String[] cols = sCurrentLine.split(quote + separator + quote);
					line++;
					
					logger.debug(line + " Before: " + Joiner.on(",").join(cols) + "\n");
					
					// strip any double quotes found inside the column value
					for (int i = 0; i < cols.length; i++) {
						cols[i] = "\"" + cols[i].replaceAll("\"", "") + "\"";
					}
					
					cols[0] = "\"" + new SimpleDateFormat("dd MMMMM yyyy").format(new SimpleDateFormat("dd MMMMM yyyy", Locale.forLanguageTag(language)).parse(cols[0].replaceAll("\"", ""))) + "\"";
					cols[10] = "\"" + new SimpleDateFormat("dd MMMMM yyyy").format(new SimpleDateFormat("dd MMMMM yyyy", Locale.forLanguageTag(language)).parse(cols[10].replaceAll("\"", ""))) + "\"";
					
					logger.debug(line + " After: " + Joiner.on(",").join(cols) + "\n");
					
					bw.write(Joiner.on(",").join(cols) + "\n");
					bw.flush();
				}
				bw.close();
				
				copyFile(outFile, new File(toFile));
				DeleteDirUtil.delete(outFile);

			} catch (IOException e) {
				logger.error("Error validating CSv File ({})", e);
				throw new RuntimeException("Error validating CSV file", e);
			} finally {
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					logger.error("Error closing buffered reader", ex);
				}
				try {
					if (bw != null)bw.close();
				} catch (IOException ex) {
					logger.error("Error closing buffered writer", ex);
				}
			}

		}
		
		return this;
	}

	private void validateActivityReport(String toFile) throws Exception {
		Integer numberOfLines = countLines(toFile);
		if (numberOfLines > 1) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new FileReader(toFile));
				String firstLine = br.readLine();
				if (firstLine == null || firstLine.split(separator).length < 5) {
					throw new RuntimeException(toFile + " is invalid. First Line: " + firstLine);
				}
				
			} catch (Exception e) {
				logger.error("Error validating CSv File ({})", e);
				throw new RuntimeException("Error validating CSV file", e);
			} finally {
				try {
					if (br != null) {
						br.close();
					}
				} catch (IOException ex) {
					logger.error("Error closing buffered reader", ex);
				}
			}
		}
	}
	
	private static int countLines(String filename) throws Exception {
	    InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
	
	private String extractDownloadCsvUrl(String page) {
		Pattern p = Pattern.compile(".+href=\"([^\"]+)\">Download CSV</a>.+");
		Matcher m = p.matcher(page);
		if (m.matches()) {
			return m.group(1);
		}
		return null;
	}
	
//	private String extractReportStatusUrl(String page) {
//		Pattern p = Pattern.compile(".+(/ajax/report_status.cgi?[^\"]+)\".+");
//		Matcher m = p.matcher(page);
//		if (m.matches()) {
//			return m.group(1);
//		}
//		return null;
//	}
//	
//	private String extractReportStatusCompletedUrl(String page) {
//		Pattern p = Pattern.compile(".+(/reports/reports.cgi?[^\"]+)\".+");
//		Matcher m = p.matcher(page);
//		if (m.matches()) {
//			return m.group(1);
//		}
//		return null;
//	}
	
	public BroadBeanExtractor extractLiveJobs(String targetFilename) throws Exception {
		
		logger.info("Starting Live Jobs Report extract: (to:{})", targetFilename);
		
		loginClient();
		
		//String page = sendPost("http://www.adcourier.com/reports/reports.cgi", "type=LIJO&jobtype=&salary_cur=&salary_from=&salary_to=&salary_per=&jobtitle=&jobref=&compile=compile");
		
		String downloadCsvUrl = null;
		int sleepMinutes = 2;
		while ((downloadCsvUrl = extractDownloadCsvUrl(sendPost(ADCOURIER_LIVEJOBS_REPORTS_URL, "type=LIJO&jobtype=&salary_cur=&salary_from=&salary_to=&salary_per=&jobtitle=&jobref=&compile=compile"))) == null) {
			logger.debug("Download Csv Url is not present, retry in {} minutes...", sleepMinutes);
			TimeUnit.MINUTES.sleep(sleepMinutes);
		}
		
		logger.debug("Found Download CSV URL: {}", downloadCsvUrl);
//		}
		
		downloadFile(String.format("%s%s", ADCOURIER_URL, downloadCsvUrl.split("\\?")[0]), downloadCsvUrl.split("\\?")[1], targetFilename);
		
		validateLiveJobs(targetFilename);
		
		if (!language.equalsIgnoreCase("en")) {
			// convert file to english
			
			BufferedReader br = null;
			BufferedWriter bw = null;
			try {
				File outFile = new File(targetFilename + "_" + language);
				
				if (!outFile.exists()) {
					outFile.createNewFile();
				}

				br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(targetFilename)), "UTF8"));
				
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), "UTF8"));
				
				String sCurrentLine = null;
				int row = 0;
				while ((sCurrentLine = br.readLine()) != null) {
										
					if (sCurrentLine.trim().length() == 0) continue;
					
					String[] cols = sCurrentLine.split(quote + separator + quote);
					
					if (row++ == 0) {
						String[] newCols = "\"User\",\"Team\",\"Office\",\"Reference\",\"Title\",\"Date Posted\",\"Job Type\",\"Job Classification\",\"Currency\",\"Salary From\",\"Salary To\",\"Salary Per\",".split(",");
						for (int i = 0; i < newCols.length;i++) {
							cols[i] =  newCols[i];
						}
					}
					
					for (int i = 0; i < cols.length; i++) {
						cols[i] = "\"" + cols[i].replaceAll("\"", "") + "\"";
					}	
					
					bw.write(Joiner.on(",").join(cols) + "\n");
					bw.flush();
				}
				bw.close();
				
				copyFile(outFile, new File(targetFilename));
				DeleteDirUtil.delete(outFile);

			} catch (IOException e) {
				logger.error("Error validating CSv File ({})", e);
				throw new RuntimeException("Error validating CSV file", e);
			} finally {
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					logger.error("Error closing buffered reader", ex);
				}
				try {
					if (bw != null)bw.close();
				} catch (IOException ex) {
					logger.error("Error closing buffered writer", ex);
				}
			}

		}
		
		
		logger.info("Finished Live Jobs Report extract: (to:{},  number of lines in file:: {})", targetFilename, countLines(targetFilename));
		return this;
		
//		if (downloadCsvUrl == null) {
//			
//			
//			
//			try {
//				logger.debug("Download CSV URL not found. Proceed to generate new Live Jobs report");
//				
//				String reportStatusUrl = String.format("%s%s", ADCOURIER_URL, extractReportStatusUrl(page));
//				String reportStatusCompletedUrl = String.format("%s%s%s", ADCOURIER_URL, extractReportStatusCompletedUrl(page), Math.random());
//				
//				logger.debug("Report status URL: {}", reportStatusUrl);
//				logger.debug("Report status completed URL: {}", reportStatusCompletedUrl);
//				
//				int sleepSeconds = 10;
//				String pageStatus = null;
//				while (!(pageStatus = getPageContent(String.format("%s%s", reportStatusUrl, Math.random()))).trim().equalsIgnoreCase("COMPLETED")) {// while report status is NOT COMPLETE fetch status
//					logger.debug("Report status: <{}> Check report status again in {} seconds...", pageStatus, sleepSeconds);
//					TimeUnit.SECONDS.sleep(sleepSeconds);// check report status every ten seconds
//				}
//				
//				logger.debug("Report status: <{}>", pageStatus);
//				
//				String completedPage = getPageContent(reportStatusCompletedUrl);
//				downloadCsvUrl = extractDownloadCsvUrl(completedPage);
//			} catch (Exception ex) {
//				logger.error("Error checking status of report generation", ex);
//			}
//		} else {
//			logger.debug("Found existing Download CSV URL: {}", downloadCsvUrl);
//		}
//		
//		
//		if (downloadCsvUrl == null) {
//			
//			
//			
//		}
		
		
	}

	private void validateLiveJobs(String targetFilename) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(targetFilename));
			String firstLine = br.readLine();
			if (firstLine == null || firstLine.split(separator).length < 12) {
				throw new RuntimeException(targetFilename + " is invalid. First Line: " + firstLine);
			}
			
		} catch (Exception e) {
			logger.error("Error validating CSv File ({})", e);
			throw new RuntimeException("Error validating CSV file", e);
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException ex) {
				logger.error("Error closing buffered reader", ex);
			}
		}
	}
	
	private String sendPost(String url, String postParams) throws Exception {
		logger.debug("sendPost: <{}> sendPostParams <{}>", url, postParams);
		URL obj = new URL(url);
		conn = (HttpURLConnection) obj.openConnection();
	 
		// Acts like a browser
		conn.setUseCaches(false);
		
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Host", "www.adcourier.com");
		
		conn.setInstanceFollowRedirects(false);
		
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		conn.setRequestProperty("Accept-Language", "en-GB,en-US;q=0.8,en;q=0.6");
		//conn.setRequestProperty("Accept-Encoding", "gzip, deflate");
		
		Pattern p = Pattern.compile("(BEANSID=[^;]+);.+");
		
		for (String cookie : getCookies()) {
			Matcher m = p.matcher(cookie.split(";", 1)[0]);
			String value;
			if (m.matches()) {
				value = m.group(1);
			} else {
				value = cookie.split(";", 1)[0];
			}
			logger.debug("Add Cookie to Request: <{}>", value);
			conn.addRequestProperty("Cookie", value);
		}
		conn.setRequestProperty("Connection", "keep-alive");
		conn.setRequestProperty("Cache-Control", "no-cache");
		//conn.setRequestProperty("Referer", referrer);
		conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		conn.setRequestProperty("Content-Length", Integer.toString(postParams.length()));
		conn.setRequestProperty("Origin", "http://www.adcourier.com");
	 
		conn.setDoOutput(true);
		conn.setDoInput(true);
	 
		// Send post request
		DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		wr.writeBytes(postParams);
		wr.flush();
		wr.close();
	 
		int responseCode = conn.getResponseCode();
		logger.debug("Sending 'POST' request to URL : <{}>", url);
		logger.debug("Post parameters : <{}>", postParams);
		logger.debug("Response Code : <{}>", responseCode);
	 
		setCookies(conn.getHeaderFields().get("Set-Cookie"));
		
		BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
	 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		logger.debug("Response Body: <{}>", response.toString());
		
		return response.toString();
	  }
	
	
	private void downloadFile(String url, String postParams, String to) throws Exception {
		logger.debug("sendPost: <{}> postParams: <{}> to: <{}>", url, postParams, to);
		URL obj = new URL(url);
		conn = (HttpURLConnection) obj.openConnection();
	 
		// Acts like a browser
		conn.setUseCaches(false);
		
		conn.setRequestMethod("POST");
		
		conn.setConnectTimeout(5000); //set timeout to 5 seconds
		
		conn.setRequestProperty("Host", "www.adcourier.com");
		
		conn.setInstanceFollowRedirects(false);
		
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		conn.setRequestProperty("Accept-Language", "en-GB,en-US;q=0.8,en;q=0.6");
		//conn.setRequestProperty("Accept-Encoding", "gzip, deflate");
		
		Pattern p = Pattern.compile("(BEANSID=[^;]+);.+");
		
		for (String cookie : this.cookies) {
			Matcher m = p.matcher(cookie.split(";", 1)[0]);
			String value;
			if (m.matches()) {
				value = m.group(1);
			} else {
				value = cookie.split(";", 1)[0];
			}
			logger.debug("Add Cookie to Request: <{}>", value);
			conn.addRequestProperty("Cookie", value);
		}
		conn.setRequestProperty("Connection", "keep-alive");
		conn.setRequestProperty("Cache-Control", "no-cache");
		//conn.setRequestProperty("Referer", referrer);
		conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		conn.setRequestProperty("Content-Length", Integer.toString(postParams.length()));
		conn.setRequestProperty("Origin", "http://www.adcourier.com");
	 
		conn.setDoOutput(true);
		conn.setDoInput(true);
	 
		// Send post request
		DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		wr.writeBytes(postParams);
		wr.flush();
		wr.close();
	 
		int responseCode = conn.getResponseCode();
		logger.debug("Sending 'POST' request to URL : <{}>", url);
		logger.debug("Post parameters : <{}>", postParams);
		logger.debug("Response Code : <{}>", responseCode);
		
		long headerContentLength = Long.parseLong(conn.getHeaderField("Content-Length"));
	 
		setCookies(conn.getHeaderFields().get("Set-Cookie"));
		
		File outputFile = new File(to);
		File partialDownloadFile = new File(outputFile.getParentFile(), outputFile.getName() + ".partial");
		
		FileOutputStream output = new FileOutputStream(partialDownloadFile);
		IOUtils.copy(conn.getInputStream(), output);
		output.close();
		copyFile(partialDownloadFile, outputFile);
		
		DeleteDirUtil.delete(partialDownloadFile);

		long downloadedFileSize = outputFile.length();
		
		if (downloadedFileSize != headerContentLength) {
			DeleteDirUtil.delete(outputFile);
			throw new RuntimeException("File (" + to  + ") only partially downloaded... Actual size: " + headerContentLength + " Downloaded size: " + downloadedFileSize + " (" + ((downloadedFileSize/headerContentLength)*100) +"%)");
		}
		
		logger.info("File (" + to  + ")  downloaded size: " + downloadedFileSize);
	}
	
	private static void copyFile(File afile, File bfile) {
		InputStream inStream = null;
		OutputStream outStream = null;

		try {
			inStream = new FileInputStream(afile);
			outStream = new FileOutputStream(bfile);

			byte[] buffer = new byte[1024];

			int length;
			// copy the file content in bytes
			while ((length = inStream.read(buffer)) > 0) {

				outStream.write(buffer, 0, length);

			}

			inStream.close();
			outStream.close();


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getPageContent(String url) throws Exception {
		
		URL obj = new URL(url);
		conn = (HttpURLConnection) obj.openConnection();

		// default is GET
		conn.setRequestMethod("GET");

		conn.setUseCaches(false);

		// act like a browser
		conn.setRequestProperty("User-Agent", USER_AGENT);
		conn.setRequestProperty("Accept",
				"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		if (cookies != null) {
			for (String cookie : this.cookies) {
				conn.addRequestProperty("Cookie", cookie.split(";", 1)[0]);
			}
		}
		int responseCode = conn.getResponseCode();
		logger.debug("Sending 'GET' request to URL : <{}>", url);
		logger.debug("Response Code : <{}>", responseCode);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				conn.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// Get the response cookies
		setCookies(conn.getHeaderFields().get("Set-Cookie"));

		return response.toString();

	}

	public String getFormParams(String html, String username, String password)
			throws UnsupportedEncodingException {

		logger.debug("Extracting form's data...");

		Document doc = Jsoup.parse(html);

		// Google form id
		Element loginform = doc.getElementsByTag("form").get(0);
		Elements inputElements = loginform.getElementsByTag("input");
		List<String> paramList = new ArrayList<String>();
		for (Element inputElement : inputElements) {
			String key = inputElement.attr("name");
			String value = inputElement.attr("value");

			if (key.equals("username"))
				value = username;
			else if (key.equals("password") && password != null)
				value = password;
			paramList.add(key + "=" + URLEncoder.encode(value, "UTF-8"));
			//paramList.add(key + "=" + value);
		}

		// build parameters list
		StringBuilder result = new StringBuilder();
		for (String param : paramList) {
			if (result.length() == 0) {
				result.append(param);
			} else {
				result.append("&" + param);
			}
		}
		return result.toString();
	}

	protected List<String> getCookies() {
		if (cookies != null) return this.cookies;
		return new ArrayList<String>();
	}

	private void setCookies(List<String> cookies) {
		if (cookies != null) {
			for (String cookie : cookies) {
				logger.debug("Cookie: <{}>", cookie);
			}
		}
		this.cookies = cookies;
	}
	
	
	private static void write(String content, String filename) throws IOException {
		// if file doesnt exists, then create it
		File file = new File(filename);
		if (!file.exists()) {
			file.createNewFile();
		}
 
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(content);
		bw.close();
 
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		String baseDir = "C:\\GTS\\BB\\";
//		String content = "This is the content to write into file";
//		String filename = "C:\\GTS\\BB\\filename.partial";
//		
//		write(content, filename);
//		DeleteDirUtil.delete(new File(filename));
		
		
		/*
		for (BroadBeanClient bbc : new BroadBeanExtractor("", "", "", "").extractBroadBeanClients(new File("panelHomeScreen.txt"))) {
			
			if (!bbc.getSite().equalsIgnoreCase("pagejp")) continue; 
			
			//System.out.println(String.format("%s%s\\", baseDir, bbc.getId()));
			File dir = new File(String.format("%s%s\\", baseDir, bbc.getSite()));
			if (!dir.exists())	dir.mkdir();
			
			String filename = String.format("%s%s\\cand_resp-%s.csv", baseDir, bbc.getSite(), new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date()));
			
			
			File file = new File(filename);
			
			//System.out.println(file.getAbsolutePath());
			//System.out.println(file.getCanonicalPath());
			//System.out.println(file.getName());
			//System.out.println(file.getPath());
			//System.out.println(file.getParentFile().getPath());
			
			//System.out.println(new File(file.getParentFile(), file.getName()));
			
			//new BroadBeanExtractor(bbc.getUsername(), bbc.getPassword()).extractActivityReportTo("cand_resp", "10 June 2015", "10 June 2015", filename);
			new BroadBeanExtractor(bbc.getUsername(), bbc.getPassword(), "", "").extractLiveJobs(String.format("%s%s\\livejobs-%s.csv", baseDir, bbc.getSite(), new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date())));
		}
		*/
//		
		//System.out.println(doc.select("div.collapseable").size());
		
		
		//new BroadBeanExtractor("admin@superadmin.pageuk", "MDRl2pCJ")
		//.extractActivityReportTo("cand_resp", "12 June 2015", "12 June 2015", String.format("C:\\GTS\\BB\\pagepersonneluk_cand_resp2%s.csv", Util.getTimestamp()))
		//;
		
		
		
		
//		String username = "kylewebb@michaelpage.com.au";
//		String password = "APprs14";
//		
//		new BroadBeanExtractor(username, password).extractSites();
		
		
		
		//System.out.println(encrypt("admin@superadmin.pageuk"));
		//System.out.println(decrypt(encrypt("admin@superadmin.pageuk")));
		// handle https
		// check spec for extractor protocol
		
		
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(new Date());
//		cal.add(Calendar.DATE, -1);
//			
//		//System.out.println(new Format("dd MMMMM yyyy").format(cal.getTime()));
//		String dateToFrom = new Format("dd MMMMM yyyy").format(cal.getTime());
//		
//		new BroadBeanExtractor("admin@superadmin.pageuk", "MDRl2pCJ")
//		.extractActivityReportTo("cand_resp", dateToFrom, dateToFrom, String.format("C:\\GTS\\BB\\pagepersonneluk_cand_resp2%s.csv", Util.getTimestamp()))
//		.extractLiveJobs(String.format("C:\\GTS\\BB\\LiveJobs%s.csv", Util.getTimestamp()));
//		;	
		
//		new BroadBeanExtractor("admin@superadmin.pageuk", "MDRl2pCJ")
//			.extractActivityReportTo("cand_resp", "10 June 2015", "11 June 2015", String.format("D:\\GTS\\BB\\pagepersonneluk_cand_resp2%s.csv", Util.getTimestamp()))
//			.extractLiveJobs(String.format("D:\\GTS\\BB\\LiveJobs%s.csv", Util.getTimestamp()));
//			;		
	
	}
}

class BroadBeanClient {
	private String id;
	private String username;
	private String password;
	private String site;
	
	public BroadBeanClient(String id, String username, String password, String site) {
		super();
		this.id = id;
		this.username = username;
		this.password = password;
		this.site = site;
	}
	public String getId() {
		return id;
	}
	public String getUsername() {
		return username;
	}
	public String getPassword() {
		return password;
	}
	public String getSite() {
		return site;
	}
}

class BroadBeanClientBuilder {
	private String id;
	private String username;
	private String password;
	private String site;
	
	public BroadBeanClientBuilder setId(String id) {
		this.id = id;
		return this;
	}
	
	public BroadBeanClientBuilder setUsername(String username) {
		this.username = username;
		return this;
	}
	
	public BroadBeanClientBuilder setPassword(String password) {
		this.password = password;
		return this;
	}
	
	public BroadBeanClientBuilder setSite(String site) {
		this.site = site;
		return this;
	}
	
	public BroadBeanClient createBroadBeanClient() {
		return new BroadBeanClient(id, username, password, site);
	}
	
}
