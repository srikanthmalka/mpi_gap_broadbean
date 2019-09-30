package au.com.michaelpage.gap.broadbean;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.michaelpage.gap.common.Settings;
import au.com.michaelpage.gap.common.generator.DataOrigin;
import au.com.michaelpage.gap.common.generator.DimensionsGenerator;
import au.com.michaelpage.gap.common.generator.HitsGenerator;
import au.com.michaelpage.gap.common.google.GoogleDimensionsUploader;
import au.com.michaelpage.gap.common.google.GoogleHitsUploader;
import au.com.michaelpage.gap.common.google.GoogleUploaderDao;
import au.com.michaelpage.gap.common.util.DatabaseManager;
import au.com.michaelpage.gap.common.util.Util;

import com.google.common.io.Files;

public class BroadbeanTransformMainApp {

	private static final Logger logger = LoggerFactory.getLogger(BroadbeanTransformMainApp.class);
	
	private static final String OUTPUT_FOLDER = "C:\\GTS\\Output\\Broadbean\\";
	
	private static final String OUTPUT_FOLDER_FOR_GLOBAL_REGIONAL = "C:\\GTS\\global-regional\\";
	
	private static final String ARCHIVE_FOLDER = "C:\\GTS\\Archive\\Broadbean\\";
	
	private static final String DATABASE_LOCATION = "c:\\Temp\\GTS_DB_BROADBEAN";
	
	public static void main(String[] args) throws Exception {

		logger.info("Started Broadbean Transformations");
		
		String incomingFolder = System.getProperty("gts.incomingFolder");
		String hostName = Settings.INSTANCE.getHostName(); 

		String workingFolder = System.getProperty("gts.workingFolder");
		if (Util.isEmpty(workingFolder)) {
			workingFolder = hostName;
		}

		String databaseLocation = DATABASE_LOCATION + "\\" + workingFolder;
		
		String processLiveJobsFileStr = System.getProperty("gts.processLiveJobsFile");
		boolean processLiveJobsFile = Util.isEmpty(processLiveJobsFileStr) ||  "true".equalsIgnoreCase(processLiveJobsFileStr);
		
		// this is to use for one-off uploads - it allows to extract data without uploading
		boolean extractOnly = !Util.isEmpty(System.getProperty("gts.extractOnly")) && "true".equals(System.getProperty("gts.extractOnly"));
		
		int attempts = 0;
		boolean processingFinished = false;
		boolean newFilesProcessed = false;
		
		while (true) {
			attempts++;

			if (attempts > 10 && !processingFinished) {
				logger.info("This job hasn't completed successfully after 10 attempts.");
				throw new RuntimeException();
			} else {
				
				if (!extractOnly) {
					if (processingFinished) {
						break;
					}
					
					if (attempts > 1) {
						logger.info("5 minutes pause before attempt #{}.", attempts);
						Thread.sleep(1000*60*5);
					}
				}
				
				try {

					Map<Integer, String> outstandingFiles = new HashMap<Integer, String>(); 

					DatabaseManager.INSTANCE.initDatabase(databaseLocation);
					
					if (!extractOnly) {
						
						 outstandingFiles = new GoogleUploaderDao().findOutstandingFiles(); 
						
						if (outstandingFiles.size() == 0) {
							if (newFilesProcessed) {
								logger.info("Finished Broadbean Transformations successfully.");
								break;
							} else {
								logger.info("No incomplete uploads found, proceeding with transformations.");
							}
						} else {
							logger.info("The following {} incomplete uploads have been found, trying to upload them first.", outstandingFiles.size());
							
							for (String fileName : outstandingFiles.values()) {
								logger.info(fileName);
							}
							
							Map<Integer, String> outstandingDimensionFiles = new GoogleUploaderDao().findOutstandingFiles("DIMENSION"); 
							for (String fileName : outstandingDimensionFiles.values()) {
								new GoogleDimensionsUploader().upload(fileName);
							}

							Map<Integer, String> outstandingHitFiles = new GoogleUploaderDao().findOutstandingFiles("HIT"); 
							for (String fileName : outstandingHitFiles.values()) {
								new GoogleHitsUploader().upload(fileName);
							}

							outstandingFiles = new GoogleUploaderDao().findOutstandingFiles();
							if (outstandingFiles.size() > 0) {
								logger.info("Incomplete uploads are still found, restarting uploads.");
								continue;
							} else {
								logger.info("Finished Broadbean Transformations successfully.");
								break;
							}
						}
					
					}

					DatabaseManager.INSTANCE.shutdownDatabase(false, true);
					
					DatabaseManager.INSTANCE.initDatabase(databaseLocation);

					String currentExtractFolder = resolveIncomingFolder(incomingFolder);

					String broadbeanExtractDate = currentExtractFolder.substring(currentExtractFolder.lastIndexOf("\\") + 1);
					
					String timestamp = Util.getTimestamp();
					
					String fileTimestamp = broadbeanExtractDate + "-" + timestamp;
					
					String outputFolder = OUTPUT_FOLDER + workingFolder + "\\" + fileTimestamp + "\\";
					
					String countryCode = getBroadbeanCountryCodeFromHostName(hostName).toLowerCase();
					
					String liveJobsFile = null;
					if (processLiveJobsFile) {
						liveJobsFile = findFileByPartialMatch(currentExtractFolder, String.format("%s_LiveJobs.csv", countryCode));
					} else {
						logger.warn("Processing of live jobs file has been turned off.");
					}
					
					
					String candidateResponseFile = findFileByPartialMatch(currentExtractFolder, String.format("%s_cand_resp2.csv", countryCode));

					try {
						new BroadBeanImport(candidateResponseFile, System.getProperty("separator", ",").charAt(0)).importData();
						
						if (processLiveJobsFile) {
							new BroadBeanImport(liveJobsFile, System.getProperty("separator", ",").charAt(0)).importData();
						}
					} catch (Throwable t) {
						logger.error("An error occured during extracting information from the extract, this job hasn't completed successfully. Please check log files for details");
						logger.debug(t.getMessage(), t);
						DatabaseManager.INSTANCE.restorePreviousDatabase();
						throw new RuntimeException();
					}

					String hitsOutputFolder = outputFolder + "Hits\\";
					logger.info("Hits output folder: " + hitsOutputFolder);

					String dimensionsOutputFolder = outputFolder + "Dimensions\\";
					logger.info("Dimensions output folder: " + dimensionsOutputFolder);

					// Generate hits & dimensions
					boolean dimensionsGenerated = false;
					try {
						new HitsGenerator(hitsOutputFolder).generate(DataOrigin.BROADBEAN, processLiveJobsFile);
						if (processLiveJobsFile) {
							dimensionsGenerated = new DimensionsGenerator(dimensionsOutputFolder, OUTPUT_FOLDER_FOR_GLOBAL_REGIONAL, fileTimestamp).generate(DataOrigin.BROADBEAN);
						}
					} catch (Exception ge) {
						logger.error("A generator error occured, this job hasn't completed successfully. Please check log files for details");
						logger.debug(ge.getMessage(), ge);
						DatabaseManager.INSTANCE.cleanupUploadData();
						DatabaseManager.INSTANCE.restorePreviousDatabase();
						Util.delete(outputFolder);
						throw new RuntimeException();
					}	

					// Archive extract 
					String archivePath = ARCHIVE_FOLDER + workingFolder + "\\" + broadbeanExtractDate + "-" + timestamp + "\\";
					new File(archivePath).mkdirs();
					
					Files.copy(new File(candidateResponseFile), new File(archivePath, new File(candidateResponseFile).getName()));
					Util.delete(candidateResponseFile);
					
					if (processLiveJobsFile) {
						Files.copy(new File(liveJobsFile), new File(archivePath, new File(liveJobsFile).getName()));
						Util.delete(liveJobsFile);
					}					
					
					Util.delete(currentExtractFolder);

					
					newFilesProcessed = true;
					
					
					if (!extractOnly) {

						//Upload dimensions
						if (dimensionsGenerated) {
							File dimensionsFolder = new File(dimensionsOutputFolder);
							for (File file : dimensionsFolder.listFiles()) {
								new GoogleDimensionsUploader().upload(file.getCanonicalPath());
							}
						} else {
							logger.info("No dimensions have been generated.");
						}

						// Upload hits
						File hitsFolder = new File(hitsOutputFolder);
						for (File file : hitsFolder.listFiles()) {
							new GoogleHitsUploader().upload(file.getCanonicalPath());
						}
						
						outstandingFiles = new GoogleUploaderDao().findOutstandingFiles(); 
						if (outstandingFiles.size() > 0) {
							processingFinished = false;
							logger.info("Some file(s) haven't been uploaded.");
						} else {
							processingFinished = true;
							logger.info("Finished Broadbean Transformations successfully.");
						}
						
					} else {
						processingFinished = true;
					}
					
					
				} catch (Throwable t) {
					logger.error("An error occured, this job hasn't completed successfully. Please check log files for details");
					logger.debug(t.getMessage(), t);
					throw new RuntimeException();
				} finally {
					DatabaseManager.INSTANCE.shutdownDatabase(false, false);
				}
			}
		}
				
	}

	private static String resolveIncomingFolder(String base) throws IOException {
		File folder = new File(base);
		File[] files = folder.listFiles();
		
		if (files != null && files.length > 0) {
			Arrays.sort(files, new Comparator<File>() {
				@Override
				public int compare(File f1, File f2) {
					try {
						return f1.getCanonicalPath().compareTo(f2.getCanonicalPath());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});
			return files[0].getCanonicalPath();
		} else {
			logger.error("Broadbean folder " + base + " doesn't contain any extracts.");
			throw new RuntimeException("Broadbean folder " + base + " doesn't contain any extracts.");
		}
	}
	
	private static String findFileByPartialMatch(String base, String partialFileName) throws IOException {
		File folder = new File(base);
		File[] files = folder.listFiles();
		
		if (files != null && files.length > 0) {
			for (File file : files) {
				if (file.getCanonicalPath().contains(partialFileName)) {
					return file.getCanonicalPath();
				}
			}
		}
		throw new RuntimeException("Couldn't find a file to proces by partial name " + partialFileName + " in folder " + base);
	}
	
	private static String getBroadbeanCountryCodeFromHostName(String hostName) {
		return hostName.equalsIgnoreCase("michaelpageafrica.com") ? "za" : ((hostName.endsWith("com")) || (hostName.endsWith("ca")) ? "usa" : Util.getCountryCodeFromHostName(hostName));
	}	
	
}
