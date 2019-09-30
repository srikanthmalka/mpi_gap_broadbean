package au.com.michaelpage.gap.broadbean;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.michaelpage.gap.common.util.CryptUtil;
import au.com.michaelpage.gap.common.util.DeleteDirUtil;
import au.com.michaelpage.gap.common.util.Util;

public class BroadBeanExtractorMainApp {
	
	private static final Logger logger = LoggerFactory.getLogger(BroadBeanExtractorMainApp.class);

	public static void main(String[] args) throws Exception {
		int count = 0;
		int maxTries = 3;
		
		String hostName = System.getProperty("gts.hostName");
		
		String broadbeanFolder = "c:\\GTS\\BB\\" + hostName + "\\";
		
		String lastExtractDateFileName = "c:\\GTS\\Settings\\" + hostName + "\\last_extract_date_broadbean.dat";
		
		String lastExtractDateStr = Util.readTextFile(lastExtractDateFileName);
		
		Date lastExtractDate = Util.strToDate(lastExtractDateStr, "yyyy-MM-dd");
		
		String separator = System.getProperty("separator", ",");
		
		String locale = System.getProperty("locale", "en");
		
		String site = System.getProperty("gts.site");
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(lastExtractDate);
		cal.add(Calendar.DATE, 1);
		
		Date extractDate = cal.getTime();
		
		String extractDateStr = new SimpleDateFormat("yyyy-MM-dd").format(extractDate);
		
		String baseDir = broadbeanFolder + extractDateStr + "\\";
		File baseDirFile = new File(baseDir);
		baseDirFile.mkdirs();
		
		while (true) {
			try {
				logger.info("Started Broadbean Extract");
				logger.info("Folder: " + baseDir);
	
				BroadBeanExtractor bbe = new BroadBeanExtractor(CryptUtil.decrypt(BroadBeanExtractor.ADCOURIER_PANEL_USERNAME), CryptUtil.decrypt(BroadBeanExtractor.ADCOURIER_PANEL_PASSWORD), separator, locale);
				
				BroadBeanClient bbc = bbe.extractSite(site);
				
				String liveJobsFile = String.format("%s%s_LiveJobs.csv", baseDir, bbc.getSite());
				String candidateResponseFile = String.format("%s%s_cand_resp2.csv", baseDir, bbc.getSite());
				
				new BroadBeanExtractor(bbc.getUsername(), bbc.getPassword(), separator, locale, bbe.getCookies()).extractLiveJobs(liveJobsFile);
				new BroadBeanExtractor(bbc.getUsername(), bbc.getPassword(), separator, locale, bbe.getCookies()).extractActivityReportTo("cand_resp", extractDate, candidateResponseFile);
				
				//Update last_extract_date_broadbean.dat file with the new extract date
				Util.writeTextFile(lastExtractDateFileName, extractDateStr);
				
				logger.info("Finished Broadbean Extract");
				
				break;// assume everything is all good then break out of loop
			} catch (Exception ex) {
				logger.error("Error occurred extracting data from BroadBean (Retry count: {}, Site: {}, OutputDir: {})", (count + 1), baseDir, ex);
				// cleanup
				DeleteDirUtil.delete(baseDirFile);
				if (++count == maxTries) throw ex;
			}
		}
	}

}
