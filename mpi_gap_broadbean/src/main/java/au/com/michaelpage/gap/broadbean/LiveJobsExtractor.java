package au.com.michaelpage.gap.broadbean;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import au.com.michaelpage.gap.common.util.CryptUtil;

public class LiveJobsExtractor {

	public static void main(String[] args) throws Exception {
		
		String baseDir = System.getProperty("outputDir");//"C:\\GTS\\BB\\LIVEJOBS\\";
		// kyle webb's credentials
		String username = "DLmNE0SQLV3h+J5WPVFJ4oSgdxCXiANy8bpBTOD2LIc=";
		String password = "rfpsD70SfSQyjAa/t0MBLg==";
		Date now = new Date();
		String formatDate = new SimpleDateFormat("yyyy-MM-dd-HH").format(now);
		
		for (BroadBeanClient bbc : new BroadBeanExtractor(CryptUtil.decrypt(username), CryptUtil.decrypt(password),",","en").extractSites()) {
			
			if (!bbc.getSite().equalsIgnoreCase(System.getProperty("Site"))) continue;
			
			String outputDir = String.format("%s%s\\", baseDir, bbc.getSite());
			
			File dir = new File(outputDir);
			if (!dir.exists())	dir.mkdir();
			
			String outputFilename = String.format("%slivejobs-%s.csv", outputDir, formatDate);
			
			new BroadBeanExtractor(bbc.getUsername(), bbc.getPassword(),",","en").extractLiveJobs(outputFilename);
		}	
	}

}
