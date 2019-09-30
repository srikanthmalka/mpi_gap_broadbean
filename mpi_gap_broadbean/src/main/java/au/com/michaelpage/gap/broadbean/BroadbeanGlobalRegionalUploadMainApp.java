package au.com.michaelpage.gap.broadbean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import au.com.michaelpage.gap.common.generator.DimensionsGenerator;
import au.com.michaelpage.gap.common.generator.GeneratorDao;
import au.com.michaelpage.gap.common.google.GoogleDimensionsUploader;
import au.com.michaelpage.gap.common.google.GoogleUploaderDao;
import au.com.michaelpage.gap.common.util.DatabaseManager;
import au.com.michaelpage.gap.common.util.Util;

public class BroadbeanGlobalRegionalUploadMainApp {

	private static final Logger logger = LoggerFactory.getLogger(BroadbeanGlobalRegionalUploadMainApp.class);
	
	private static final String INCOMING_FOLDER = "C:\\GTS\\global-regional\\";

	private static final String OUTPUT_FOLDER = "C:\\GTS\\Output\\global-regional\\";
	
	private static final String ARCHIVE_FOLDER = "C:\\GTS\\Archive\\global-regional\\";
	
	private static final String DATABASE_LOCATION = "c:\\Temp\\GTS_DB_BROADBEAN";
	
	public static void main(String[] args) throws Exception {
		
		logger.info("Started Global-Regional Upload");
		
		String databaseLocation = DATABASE_LOCATION + "\\global-regional";
		
		int attempts = 0;
		boolean processingFinished = false;
		boolean newFilesProcessed = false;
		
		while (true) {
			attempts++;

			if (attempts > 10 && !processingFinished) {
				logger.info("This job hasn't completed successfully after 10 attempts.");
				throw new RuntimeException();
			} else {
				if (processingFinished) {
					break;
				}
				
				if (attempts > 1) {
					logger.info("5 minutes pause before attempt #{}.", attempts);
					Thread.sleep(1000*60*5);
				}
				
				try {

					DatabaseManager.INSTANCE.initDatabase(databaseLocation, false);
					
					Map<Integer, String> outstandingFiles = new GoogleUploaderDao().findOutstandingFiles(); 
					
					if (outstandingFiles.size() == 0) {
						if (newFilesProcessed) {
							logger.info("Finished Global-Regional Upload successfully.");
							break;
						} else {
							logger.info("No incomplete uploads found, proceeding with new extracts.");
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

						outstandingFiles = new GoogleUploaderDao().findOutstandingFiles();
						if (outstandingFiles.size() > 0) {
							logger.info("Incomplete uploads are still found, restarting uploads.");
							continue;
						} else {
							logger.info("Finished Global-Regional Upload successfully.");
							break;
						}
					}
					
					DatabaseManager.INSTANCE.shutdownDatabase(true, true);
					
					DatabaseManager.INSTANCE.initDatabase(databaseLocation, false);

					Set<String> folders = findAllFoldersWithData(INCOMING_FOLDER); 
					
					Map<String, String> archivePaths = new HashMap<String, String>();

					String timestamp = Util.getTimestamp();
					
					if (folders.size() > 0) {
						logger.info("{} folders with data found, starting processing.", folders.size());
						for (String folder : folders) {
							String[] s = folder.split("\\\\");
							String profileId = s[s.length - 2];
							String dataSetName = s[s.length - 1].split("---")[0];
							String dataSetId = s[s.length - 1].split("---")[1];
							
							Set<String> files = findAllDataFiles(folder, "");
							
							String exportFileName = dataSetName + "---" + profileId + "---" + dataSetId + ".csv";
							String exportFullFileName = OUTPUT_FOLDER + profileId + "\\" + timestamp + "\\" + exportFileName;
							
							String tempFileName = "c:\\temp\\" + exportFileName + "_" + UUID.randomUUID() + ".csv";
							
							String archivePath = ARCHIVE_FOLDER + profileId + "\\" + timestamp + "\\";
							archivePaths.put(folder, archivePath);
							
							combineFiles(tempFileName, files);
							
							Util.renameFile(tempFileName, exportFullFileName);
							new GeneratorDao().populateFilesToUploadTables(exportFullFileName, "DIMENSION");
							
							logger.info("Processed folder " + folder);
						}
					} else {
						logger.info("No folders with data found.");
					}
					
					
					// Archive files
					for (String sourceFolder : archivePaths.keySet()) {
						String destFolder = archivePaths.get(sourceFolder); 
						new File(destFolder).mkdirs();
						Files.move(new File(sourceFolder), new File(destFolder));
					}
					
					
					
					newFilesProcessed = true;
					
	/*				
					//Upload dimensions
					File dimensionsFolder = new File(dimensionsOutputFolder);
					for (File file : dimensionsFolder.listFiles()) {
						new GoogleDimensionsUploader().upload(file.getCanonicalPath());
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
	
					
							
/*
String currentExtractFolder = resolveIncomingFolder(incomingFolder);

					if (Util.isEmpty(currentExtractFolder)) {
						break;
					}
					
					String broadbeanExtractDate = currentExtractFolder.substring(currentExtractFolder.lastIndexOf("\\") + 1);
					
					String timestamp = Util.getTimestamp();
					
					String fileTimestamp = broadbeanExtractDate + "-" + timestamp;
					
					String outputFolder = OUTPUT_FOLDER + hostName + "\\" + fileTimestamp + "\\";
					
					String countryCode = Util.getCountryCodeFromHostName(hostName).toLowerCase();
					String liveJobsFile = findFileByPartialMatch(currentExtractFolder, String.format("%s_LiveJobs.csv", countryCode));
					String candidateResponseFile = findFileByPartialMatch(currentExtractFolder, String.format("%s_cand_resp2.csv", countryCode));

					try {
						for (String fileToImport : new String[] {candidateResponseFile, liveJobsFile}) {
							new BroadBeanImport(fileToImport).importData();
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
					try {
						new HitsGenerator(hitsOutputFolder).generate(DataOrigin.BROADBEAN);
						new DimensionsGenerator(dimensionsOutputFolder, OUTPUT_FOLDER_FOR_GLOBAL_REGIONAL, fileTimestamp).generate(DataOrigin.BROADBEAN);
					} catch (Exception ge) {
						logger.error("A generator error occured, this job hasn't completed successfully. Please check log files for details");
						logger.debug(ge.getMessage(), ge);
						DatabaseManager.INSTANCE.cleanupUploadData();
						DatabaseManager.INSTANCE.restorePreviousDatabase();
						Util.delete(outputFolder);
						throw new RuntimeException();
					}	

					*/
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

	private static Set<String> findAllFoldersWithData(String base) throws IOException {
		File folder = new File(base);
		File[] files = folder.listFiles();
		Set<String> set = new TreeSet<String>();
		
		if (files != null && files.length > 0) {
			for (File file : files) {
				if (file.isDirectory()) {
					set.addAll(findAllFoldersWithData(file.getCanonicalPath()));
				} else {
					set.add(file.getParent());
				}
			}
		}
		return set;
	}
	
	private static Set<String> findAllDataFiles(String base, String fileName) throws IOException {
		File folder = new File(base);
		File[] files = folder.listFiles();
		Set<String> set = new TreeSet<String>();

		if (files != null && files.length > 0) {
			for (File file : files) {
				if (!file.isDirectory() && file.getCanonicalPath().endsWith(fileName)) {
					set.add(file.getCanonicalPath());
				} else {
					set.addAll(findAllDataFiles(file.getCanonicalPath(), fileName));
				}
			}
		}
		
		return set;
	}
	
	private static void combineFiles(String destFile, Set<String> sourceFiles) throws IOException {
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destFile), "UTF-8"));
		
			int i = 0;
			for (String file : sourceFiles) {
				try {
					br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
					
					String line = null;
					boolean headerLine = true;
					while ((line = br.readLine()) != null) {
						if (i != 0 && headerLine) {
							headerLine = false;
							continue;
						} else {
							bw.append(line);
							bw.append("\r\n");
							bw.flush();
						}
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			
				i++;
			}

		} finally {
			if (bw != null) {
				bw.close();
			}
		}
		
	}
	
}
