package au.com.michaelpage.gap.broadbean;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import au.com.michaelpage.gap.common.util.CryptUtil;

public class ActivityReportExtractor {

	public static void main(String[] args) throws Exception {
		
		String reports = "posted;cand_resp";
		
		Map<String,String> countries = new HashMap<String,String>();
		
		for (BroadBeanClient bbc : new BroadBeanExtractor(CryptUtil.decrypt("DLmNE0SQLV3h+J5WPVFJ4oSgdxCXiANy8bpBTOD2LIc="), CryptUtil.decrypt("rfpsD70SfSQyjAa/t0MBLg=="), ",", "en").extractSites()) {
			
			countries.put(bbc.getUsername() + ";" + bbc.getPassword(), reports);
		}	
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, Integer.parseInt(System.getProperty("calendar.add", "0")));
			
		String fromTo = new SimpleDateFormat("dd MMMMM yyyy").format(cal.getTime());
		
		String baseDir = System.getProperty("outputDir") + new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime()) + "\\";
		
		File baseDirFile = new File(baseDir);
		System.out.println("Make Dir: " + baseDir + " " + baseDirFile.mkdir());
		
		for (Entry<String, String> entry : countries.entrySet()) {
			String username = entry.getKey().split(";")[0];
			String password = entry.getKey().split(";")[1];
			BroadBeanExtractor bbe = new BroadBeanExtractor(username, password,",","en");
			
			for (String activityType : entry.getValue().split(";")) {
				String site = username.split("\\.")[1];
				String filename = String.format("%s%s_%s.csv",baseDir, site, activityType);
				bbe.extractActivityReportTo(activityType, fromTo, fromTo, filename);
			}
		}
	}

}
