package com.wdpr.dc.agents;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


//import com.disney.agent.Agent;
import com.disney.logging.EventLogger;
import com.disney.util.PrintStack;
import com.wdpr.dc.command.AddOnCalendarCommand;
import com.wdpr.dc.common.constants.StringConstants;
import com.wdpr.dc.common.exception.DCServiceException;
import com.wdpr.dc.common.services.ServicesIF;
import com.wdpr.dc.model.nonpersist.DAOServiceVO;
import com.wdpr.dc.model.persist.AddOnCalendarRunStatus;
import com.wdpr.dc.model.persist.SyncStatus.SyncStatusCodes;
import com.wdpr.dc.persist.DAOFactory;
import com.wdpr.dc.services.CRUDServiceHelper;
import com.wdpr.dc.util.CalendarPathUtil;
import com.wdpr.dc.util.DBConnectionUtil;
import com.wdpr.dc.util.DCUtils;
import com.wdpr.dc.util.ReadExcelFile;
import com.wdw.eai.foundation.audit.ErrorCode;
import com.wdw.eai.foundation.audit.EventType;

public class AddOnCalenderLoadAgent extends DirectConnectAgent {

	private static final String LINE_BREAK = StringConstants.LINE_BREAK;
	private static final String LOCK = new String(
			StringConstants.ADDON_CALENDARAGENT_LOCK);
	private static final String ADD_ON_CALENDER_FOLDER_PATH = StringConstants.ADD_ON_CALENDER_FOLDER_PATH;
	private static final String ADD_ON_PROCESS_FOLDER_PATH = StringConstants.ADD_ON_PROCESS_FOLDER_PATH;
	private static final String ADD_ON_ARCHIVE_FOLDER_PATH = StringConstants.ADD_ON_ARCHIVE_FOLDER_PATH;
	private static final String ADD_ON_CALENDER_FOLDER_ERROR_PATH = StringConstants.ADD_ON_CALENDER_FOLDER_ERROR_PATH;

	private static final String RETRIEVE_ADD_ON_CALENDAR_DATA_QUERY = "select * from ADD_ON_CALENDAR";

	private static String serverJVMName = (String) System.getProperties().get(
			"APP_NAME");

	private static final EventLogger EVL = EventLogger
			.getLogger(AddOnCalenderLoadAgent.class);

	private Set<String> fileSet = new HashSet<String>();

	@Override
	protected void init() {
	}

	@Override
	protected void doWork() {
		
		if(!shouldProcess())
		{
			this.appendToStatus("Dark Side instance, not processing..." + LINE_BREAK);
			return;
		}

		synchronized (LOCK) {
			AddOnCalendarRunStatus addOnCalendarrunStatus = null;
			
			try {
				if (initializeParams()) {
					this.status = "reading records from files to insert..........."
							+ LINE_BREAK;
					addOnCalendarrunStatus = findAndSetAddOnCalendarRunStatus();

					List<AddOnCalendarRunStatus> runningAddOnCalendarAgent = checkRunningAddOnCalendarAgentStatus();

					if (!DCUtils.isNullOrEmpty(runningAddOnCalendarAgent)) {
						this.appendToStatus(String
								.format("Another AddOnCalendaragent is already PROCESSING records on jvm : "
										+ runningAddOnCalendarAgent.get(0)
												.getServiceJvmname())
								+ LINE_BREAK);
						return;
					}

					processAddOnCalenderFiles();
					updateAddOnCalenderRunStatus(addOnCalendarrunStatus,
							SyncStatusCodes.FINISHED);
				}
			} catch (Exception e) {
				evl.sendException(EventType.EXCEPTION,
						ErrorCode.GENERAL_EXCEPTION, e, this);
				this.appendToStatus("Exception caught during AddonCalendarLoadAgent ====> doWork:"
						+ LINE_BREAK
						+ PrintStack.getTraceString(e)
						+ LINE_BREAK);
			} finally {
				try {

					updateAddOnCalenderRunStatus(addOnCalendarrunStatus,
							SyncStatusCodes.FINISHED);

				} catch (DCServiceException e1) {

					evl.sendException(EventType.EXCEPTION,
							ErrorCode.GENERAL_EXCEPTION, e1, this);
					this.appendToStatus("Exception caught when updating agent status to ERROR PROCESSING during AddonCalendarLoadAgent ===> doWork:"
							+ LINE_BREAK
							+ PrintStack.getTraceString(e1)
							+ LINE_BREAK);
				}
			}
			this.appendToStatus("Finished Calendar Load Agent Process.");
		}
	}

	private void processAddOnCalenderFiles() {
		final File folder = new File(
				CalendarPathUtil
						.getPaths(ADD_ON_CALENDER_FOLDER_PATH));

		File[] listOfFiles = listFilesForFolder(folder);
		if (listOfFiles.length > 0) {
			List<File> fileList = getOnlyFile(listOfFiles);

			Map<String, List<File>> fileMap = validateFiles(fileList);
			List<File> validateFilesList = null;
			if (isValidFiles(fileMap)) {
				validateFilesList = fileMap
						.get(StringConstants.VALID_FILES);
			}

			// Handle Error files
			handleErrorFiles(fileMap);

			// Process the valid files
			if (!DCUtils.isNullOrEmpty(validateFilesList)) {

				this.status = "Processing the Calendar Files..................."
						+ LINE_BREAK;
				executeProcess(validateFilesList);
			} else {
				this.status = "No Valid Files to process..................."
						+ LINE_BREAK;
			}
		}
	}

	/**
	 * checking the running Addon calendar agent status
	 * 
	 * @return List<AddOnCalendarRunStatus>
	 * @throws DCServiceException
	 */
	private List<AddOnCalendarRunStatus> checkRunningAddOnCalendarAgentStatus()
			throws DCServiceException {
		DAOServiceVO vo = new DAOServiceVO(
				DAOFactory.getAddOnCalendarRunStatusDAO(),
				"findAddOnCalendarAgentStatus", serverJVMName,
				SyncStatusCodes.PROCESSING.name(), StringConstants.ADDONCALENDARLOAD);

		List<AddOnCalendarRunStatus> runningCalendarAgentList = (List<AddOnCalendarRunStatus>) ServicesIF.DAO_SERVICE.EXECUTE
				.getService().execute(vo);
		return runningCalendarAgentList;
	}

	/**
	 * 
	 * @param fileMap
	 * @return boolean
	 */
	private boolean isValidFiles(Map<String, List<File>> fileMap) {
		return !DCUtils.isNullOrEmpty(fileMap) && null != fileMap.get(StringConstants.VALID_FILES);
	}

	private void handleErrorFiles(Map<String, List<File>> fileMap) {
		// Move the Invalid files to error folder
		boolean isErrorFilesMoved = false;
		if (!DCUtils.isNullOrEmpty(fileMap)
				&& null != fileMap.get(StringConstants.INVALID_FILES)) {
			isErrorFilesMoved = moveFilesToErrorFolder(fileMap
					.get(StringConstants.INVALID_FILES));
			if (isErrorFilesMoved) {
				this.status = "Moved the Invalid Files to Error folder ..................."
						+ LINE_BREAK;
			} else {
				this.status = "Error moving some of the  Invalid Files to Error folder ..................."
						+ LINE_BREAK;
			}
		}
		// End of moving invalid files
	}

	/**
	 * @param addOnCalendarRunStatus
	 * @param syncStatusCode
	 * @throws DCServiceException
	 */
	private void updateAddOnCalenderRunStatus(
			AddOnCalendarRunStatus addOnCalendarRunStatus,
			SyncStatusCodes syncStatusCode) throws DCServiceException {

		if (addOnCalendarRunStatus != null && syncStatusCode != null) {

			addOnCalendarRunStatus.setStatusName(syncStatusCode.name());
			addOnCalendarRunStatus.setUpdateUserID(StringConstants.SYSTEM);
			addOnCalendarRunStatus.setUpdateDate(new Date(System
					.currentTimeMillis()));
			CRUDServiceHelper.update(addOnCalendarRunStatus,
					DAOFactory.getAddOnCalendarRunStatusDAO());
		}
	}

	/**
	 * 
	 * @return
	 * @throws DCServiceException
	 */
	private AddOnCalendarRunStatus findAndSetAddOnCalendarRunStatus()
			throws DCServiceException {

		DAOServiceVO vo = new DAOServiceVO(
				DAOFactory.getAddOnCalendarRunStatusDAO(),
				"findAddOnCalendarAgent", serverJVMName, StringConstants.ADDONCALENDARLOAD);
		AddOnCalendarRunStatus addOncalendarAgent = (AddOnCalendarRunStatus) ServicesIF.DAO_SERVICE.EXECUTE
				.getService().execute(vo);

		if (addOncalendarAgent == null) {

			addOncalendarAgent = new AddOnCalendarRunStatus();
			addOncalendarAgent.setServiceJvmname(serverJVMName);
			addOncalendarAgent.setStatusName(SyncStatusCodes.PROCESSING.name());
			addOncalendarAgent.setServiceMethodName(StringConstants.ADDONCALENDARLOAD);
			addOncalendarAgent.setCreateDate(new Date(System
					.currentTimeMillis()));
			addOncalendarAgent.setUpdateDate(new Date(System
					.currentTimeMillis()));
			addOncalendarAgent.setCreateUserID(StringConstants.SYSTEM);
			addOncalendarAgent.setUpdateUserID(StringConstants.SYSTEM);
			addOncalendarAgent = CRUDServiceHelper.create(addOncalendarAgent,
					DAOFactory.getAddOnCalendarRunStatusDAO());
		} else {

			addOncalendarAgent.setStatusName(SyncStatusCodes.PROCESSING.name());
			addOncalendarAgent.setUpdateUserID(StringConstants.SYSTEM);
			addOncalendarAgent.setUpdateDate(new Date(System
					.currentTimeMillis()));
			addOncalendarAgent = CRUDServiceHelper.update(addOncalendarAgent,
					DAOFactory.getAddOnCalendarRunStatusDAO());
		}
		return addOncalendarAgent;
	}

	/**
	 * 
	 * @param invalidFileList
	 * @return
	 */
	private boolean moveFilesToErrorFolder(List<File> invalidFileList) {
		boolean isFilesMoved = Boolean.FALSE;
		int noofFiles = 0;
		String errorCode = null;
		
		ReadExcelFile readExcelFile = new ReadExcelFile();
		this.status = "Validation Failed on the files,so moving the files to error folder............"
				+ LINE_BREAK;
		errorCode = "DC8050";
		for (File errorFile : invalidFileList) {
			StringBuilder errorDesc = new StringBuilder();
			Path sourcePath = Paths.get(CalendarPathUtil
					.getPaths(ADD_ON_CALENDER_FOLDER_PATH)
					+ "//"
					+ errorFile.getName());

			Path destinationPath = Paths.get(CalendarPathUtil
					.getPaths(ADD_ON_CALENDER_FOLDER_ERROR_PATH)
					+ "//"
					+ errorFile.getName());
			try {
				Files.move(sourcePath, destinationPath);
				renameFileAfterMoving(destinationPath);
			} catch (IOException e) {
				EVL.sendException(e.getMessage(), EventType.FATAL, null, e,
						this);
			}
			errorDesc.append(String.format(StringConstants.ERROR_DC8050,
					errorFile.getName()));
			try {
				readExcelFile.insertNotificationRow(errorCode,
						errorDesc.toString());

			} catch (DCServiceException e) {
				EVL.sendException(e.getMessage(), EventType.FATAL, null, e,
						this);
			}
			noofFiles++;
		}

		if (invalidFileList.size() != noofFiles) {
			isFilesMoved = Boolean.FALSE;
		}

		return isFilesMoved;
	}

	/**
	 * 
	 * @param listOfFiles
	 */
	private void executeProcess(List<File> listOfFiles) {

		try {
			// Move the files to process folder to start processing
			moveFilesToProcessFolder(listOfFiles);

			final File processFolder = new File(
					CalendarPathUtil.getPaths(ADD_ON_PROCESS_FOLDER_PATH));
			File[] listOfFilesProcess = listFilesForFolder(processFolder);
			fileSet = insertIntoDatabase(listOfFilesProcess);
			// Handle processed files (move to error or archive folder)
			handleProcessedFilesAfterComplete(processFolder);
		} catch (DCServiceException e) {
			EVL.sendException(e.getMessage(), EventType.FATAL,
					e.getErrorCode(), e, this);
		}
		// Handle the archived files
		handleArchivedFiles();
	}

	/**
	 * 
	 */
	private void handleArchivedFiles() {
		final File archiveFileFolder = new File(
				CalendarPathUtil.getPaths(ADD_ON_ARCHIVE_FOLDER_PATH));
		if (null != archiveFileFolder) {
			for (File file : archiveFileFolder.listFiles()) {

				int noOfDays = 14;

				if (file.isFile()) {

					String fileName = file.getName();
					String[] fileNameTokens = fileName.split("_");
					String fileDate = fileNameTokens[1].substring(0,
							fileNameTokens[1].length() - 13);
					//fileDate = "20" + fileDate;
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Date fileDate1 = null;
					try {
						fileDate1 = sdf.parse(fileDate);
					} catch (ParseException e) {
						EVL.sendException(e.getMessage(), EventType.FATAL,
								null, e, null);
					}
					Date date = (Date) Calendar.getInstance().getTime();
					if (null != date && null != fileDate1) {
						int diffInDays = (int) ((date.getTime() - fileDate1
								.getTime()) / (1000 * 60 * 60 * 24));

						if (diffInDays > noOfDays) {

							Path deleteFilePath = Paths.get(CalendarPathUtil
									.getPaths(ADD_ON_ARCHIVE_FOLDER_PATH)
									+ "//" + file.getName());
							try {
								Files.delete(deleteFilePath);
							} catch (IOException e) {
								EVL.sendException(e.getMessage(),
										EventType.FATAL, null, e, null);
							}
						}
					}
				}
			}
		}

	}

	/**
	 * 
	 * @param processFolder
	 * @throws DCServiceException
	 */
	private void handleProcessedFilesAfterComplete(final File processFolder)
			throws DCServiceException {
		Path sourcePath = null;
		Path destinationPath = null;
		if (null != processFolder) {
			for (File file : processFolder.listFiles()) {
				if (file.isFile()) {
					if (fileSet.contains(file.getName())) {
						sourcePath = Paths.get(CalendarPathUtil
								.getPaths(ADD_ON_PROCESS_FOLDER_PATH)
								+ "//"
								+ file.getName());

						destinationPath = Paths.get(CalendarPathUtil
								.getPaths(ADD_ON_CALENDER_FOLDER_ERROR_PATH)
								+ "//" + file.getName());

					} else {
						sourcePath = Paths.get(CalendarPathUtil
								.getPaths(ADD_ON_PROCESS_FOLDER_PATH)
								+ "//"
								+ file.getName());

						destinationPath = Paths.get(CalendarPathUtil
								.getPaths(ADD_ON_ARCHIVE_FOLDER_PATH)
								+ "//"
								+ file.getName());
					}
					try {
						Files.move(sourcePath, destinationPath);
						renameFileAfterMoving(destinationPath);
					} catch (IOException e) {

						EVL.sendException(e.getMessage(), EventType.FATAL,
								null, e, this);
						throw new DCServiceException(
								"Error moving files from Source " + sourcePath
										+ " to Destination " + destinationPath,
								e, ErrorCode.APPLICATION_EXCEPTION);
					}
				}
			}
		}

	}

	/**
	 * 
	 * @param destinationPath
	 */
	private void renameFileAfterMoving(Path destinationPath) {
		DateFormat dateFormat = new SimpleDateFormat(
				StringConstants.DATE_FORMAT);
		Date date = new Date();
		String ts = dateFormat.format(date);
		File oldFile = destinationPath.toAbsolutePath().toFile();
		if (oldFile.isFile()) {
			Path path = destinationPath.getFileName();
			if (path == null) {
				throw new IllegalStateException("destinationPath file name not specified...");
			}
			String newFileName = DCUtils.addTimestamp(path.toString(), ts);
			if (oldFile.renameTo(new File(oldFile.getParent() + File.separator
					+ newFileName))) {
				this.status = "File is moved and renamed successfully............"
						+ LINE_BREAK;
			} else {
				this.status = "Some error occured while moving and renaming the file............"
						+ LINE_BREAK;
			}
		}

	}
	
	/**
	 * 
	 * @param listOfFiles
	 */
	private void moveFilesToProcessFolder(List<File> listOfFiles) {
		for (File file : listOfFiles) {

			if (file.isFile()) {

				Path sourcePath = Paths.get(CalendarPathUtil
						.getPaths(ADD_ON_CALENDER_FOLDER_PATH)
						+ "//"
						+ file.getName());

				Path destinationPath = Paths.get(CalendarPathUtil
						.getPaths(ADD_ON_PROCESS_FOLDER_PATH)
						+ "//"
						+ file.getName());
				try {

					Files.move(sourcePath, destinationPath);
				} catch (IOException e) {
					EVL.sendException(e.getMessage(), EventType.FATAL, null, e,
							this);
				}
			}
		}
	}

	/**
	 * 
	 * @param listOfFiles
	 * @return
	 */
	private List<File> getOnlyFile(File[] listOfFiles) {
		List<File> listOnlyFile = new ArrayList<File>();
		for (File file : listOfFiles) {
			if (file.isFile()) {
				listOnlyFile.add(file);
			}
		}
		return listOnlyFile;
	}

	/**
	 * 
	 * @param fileList2
	 * @return
	 */
	private Map<String, List<File>> validateFiles(List<File> fileList) {
		Map<String, List<File>> fileMap = new HashMap<String, List<File>>();
		List<File> validFilesList = new ArrayList<File>();
		List<File> invalidFilesList = new ArrayList<File>();
		for (File file : fileList) {
			String fileName = file.getName();
			if (fileName.contains(StringConstants.EXCURSION_CALENDAR)
					|| fileName.contains(StringConstants.FOOD_CALENDAR)) {
				validFilesList.add(file);
			} else {
				invalidFilesList.add(file);
			}
		}
		if (!DCUtils.isNullOrEmpty(validFilesList)) {
			fileMap.put(StringConstants.VALID_FILES, validFilesList);
		}
		if (!DCUtils.isNullOrEmpty(invalidFilesList)) {
			fileMap.put(StringConstants.INVALID_FILES, invalidFilesList);
		}
		return fileMap;
	}

	/**
	 * 
	 * @param listOfFiles
	 * @return
	 * @throws DCServiceException
	 */
	private Set<String> insertIntoDatabase(File[] listOfFiles)
			throws DCServiceException {
		Set<String> errorFileSet = new HashSet<String>();
		try {
			errorFileSet = persisitIntoAddOnCalendar(listOfFiles);
		} catch (SQLException e) {
			EVL.sendException(e.getMessage(), EventType.FATAL, null, e, this);
		} catch (IOException e) {
			EVL.sendException(e.getMessage(), EventType.FATAL, null, e, this);
		}
		return errorFileSet;
	}

	/**
	 * 
	 * @param listOfFiles
	 * @return
	 * @throws IOException
	 * @throws DCServiceException
	 * @throws SQLException
	 */
	private Set<String> persisitIntoAddOnCalendar(File[] listOfFiles)
			throws IOException, DCServiceException, SQLException {
		FileInputStream fis = null;
		//String line = "";
		Map<String, String> dbMap = new HashMap<String, String>();
		BufferedReader fileReader = null;
		Set<String> errorFileSet = new HashSet<String>();
		for (File file : listOfFiles) {
			try {
				fis = new FileInputStream(file.getAbsoluteFile());
				fileReader = new BufferedReader(new FileReader(
						file.getAbsolutePath()));
				fileIteratorOnpersisitIntoAddOnCalendar(dbMap, fileReader, errorFileSet, file);
			} catch (FileNotFoundException e) {
				EVL.sendException(e.getMessage(), EventType.FATAL, null, e,
						null);
			}
		}
		try {
			processAddOnCalendar(dbMap);
		} catch (Exception ex) {
			EVL.sendException(ex.getMessage(), EventType.FATAL, null, ex, null);
		} finally {
			if (null != fileReader) {
				fileReader.close();
			}
			if (null != fis) {
				fis.close();
			}
		}
		return errorFileSet;
	}

	private void fileIteratorOnpersisitIntoAddOnCalendar(Map<String, String> dbMap, BufferedReader fileReader,
			Set<String> errorFileSet, File file) throws IOException {
		String line;
		boolean firstLine = true;
		while ((line = fileReader.readLine()) != null) {
			if (firstLine) {
				firstLine = false;
				continue;
			}
			String validateString = validateAddonCalendarData(line,
					file.getName());
			String[] validateStringTokens = validateString
					.split(StringConstants.TILD);
			boolean status = Boolean
					.parseBoolean(validateStringTokens[0]);
			if (status && !"NA".equals(validateStringTokens[1])) {
				errorFileSet.add(file.getName());
			} else {
				String dbDataString = prepareDataToInsert(
						validateStringTokens[2], file.getName());
				String[] tokens = dbDataString.split("\\|");
				String key = tokens[0] + "~" + tokens[1]
						+ StringConstants.TILD + tokens[2];
				dbMap.put(key, tokens[3]);
			}
		}
	}

	/**
	 * 
	 * @param dbMap
	 * @throws SQLException
	 */
	private void processAddOnCalendar(Map<String, String> dbMap)
			throws SQLException {
		List<String> dataList = retrieveDataListFromMap(dbMap);
		int totalListSize = dataList.size();
		int noOfThreads = 0;
		int min = 0;
		int size = 5000;
		int max = size;
		if ((totalListSize % size) == 0) {
			noOfThreads = totalListSize / size;
		} else {
			noOfThreads = (totalListSize / size) + 1;
		}
		if (noOfThreads == 1) {
			max = totalListSize;
		}
		List<String> innerList = null;
		List<List<String>> allInnerList = new ArrayList<List<String>>();
		for (int thread = 0; thread < noOfThreads; thread++) {
			innerList = new ArrayList<String>();
			for (; min < max; min++) {
				innerList.add(dataList.get(min));
			}
			allInnerList.add(innerList);
			totalListSize = totalListSize - size;
			min = max;

			if (totalListSize < size) {
				max = max + totalListSize;
			} else {
				max = max + size;
			}
		}
		processAddOnCalendarParallel(noOfThreads, allInnerList);
	}

	/**
	 * 
	 * @param dbMap
	 * @return list
	 */
	private List<String> retrieveDataListFromMap(Map<String, String> dbMap) {
		Iterator<Entry<String, String>> it = dbMap.entrySet().iterator();
		List<String> dataList = new ArrayList<String>();
		while (it.hasNext()) {
			Map.Entry<String, String> pair = (Map.Entry<String, String>) it
					.next();
			dataList.add(pair.getKey() + "=" + pair.getValue());
		}
		return dataList;
	}

	/**
	 * 
	 * @param noOfThreads
	 * @param allInnerList
	 * @throws SQLException
	 */
	private void processAddOnCalendarParallel(int noOfThreads,
			List<List<String>> allInnerList) throws SQLException {
		// Retrieve Existing data from AddOnCalendar entity
		Map<String, String> calendarMap = retrieveExistingAddOnCalendarData();
		// Process the data parallel
		HashMap<Integer, AddOnCalendarCommand> processBatchMap = new HashMap<Integer, AddOnCalendarCommand>();
		for (int i = 0; i < allInnerList.size(); i++) {

			AddOnCalendarCommand batchCommand = new AddOnCalendarCommand(
					allInnerList.get(i), calendarMap);
			processBatchMap.put(i, batchCommand);
		}
		ReadExcelFile.processAddonCalendarsParallel(processBatchMap, 30000L,
				noOfThreads);
	}

	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	private Map<String, String> retrieveExistingAddOnCalendarData()
			throws SQLException {
		HashMap<String, String> calendarMap = new HashMap<String, String>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = DBConnectionUtil.getConnection();
			ps = conn.prepareStatement(RETRIEVE_ADD_ON_CALENDAR_DATA_QUERY);
			rs = ps.executeQuery();
			while (rs.next()) {
				String type = rs.getString("type");
				String inventoryCode = rs.getString("code");
				Date consumptionDate = rs.getDate("consumption_date");
				String status = rs.getString("consumption_status");
				String dateFormat = null;
				if (type.equalsIgnoreCase(StringConstants.FOOD)) {
					dateFormat = StringConstants.FOOD_CAL_DATEFORMAT;
				} else {
					dateFormat = StringConstants.EXCURSION_CAL_DATEFORMAT;
				}
				SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
				String strDate = sdf.format(consumptionDate);
				String key1 = type + "~" + inventoryCode + "~" + strDate;
				calendarMap.put(key1, status);
			}
		} catch (Exception ex) {
			EVL.sendException(ex.getMessage(), EventType.FATAL, null, ex, null);
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
		return calendarMap;
	}

	/**
	 * 
	 * @param line
	 * @param string
	 * @return
	 */
	private String prepareDataToInsert(String line, String fileName) {
		StringBuilder dbString = new StringBuilder();
		String retDBString = null;
		String[] stringTokens = line.split(";");
		// Type
		if (fileName.contains(StringConstants.FOOD_CALENDAR)) {
			dbString.append(StringConstants.FOOD).append(StringConstants.PIPE);
		} else if (fileName.contains(StringConstants.EXCURSION_CALENDAR)) {
			dbString.append(StringConstants.EXCURSION).append(
					StringConstants.PIPE);
		}
		// code
		dbString.append(stringTokens[0]).append(StringConstants.PIPE);
		// Consumption date
		dbString.append(stringTokens[1]).append(StringConstants.PIPE);
		// consumption status
		if (stringTokens[2].equalsIgnoreCase(StringConstants.OPEN)) {
			dbString.append(StringConstants.STRING_OPEN).append(
					StringConstants.PIPE);
		} else if (stringTokens[2].equalsIgnoreCase(StringConstants.CLOSE)) {
			dbString.append(StringConstants.STRING_CLOSE).append(
					StringConstants.PIPE);
		}
		retDBString = dbString.toString();
		if (retDBString.length() > 0
				&& retDBString.charAt(retDBString.length() - 1) == '|') {
			retDBString = retDBString.substring(0, retDBString.length() - 1);
		}
		return retDBString;
	}

	/**
	 * 
	 * @param line
	 * @param fileName
	 * @return
	 */
	public String validateAddonCalendarData(String line, String fileName) {
		ReadExcelFile readExcelFile = new ReadExcelFile();
		List<String> actionList = Arrays.asList(("O,X").split(","));
		boolean isInvalid = false;
		String errorCode = "NA";
		String strLine = line;
		StringBuilder errorDesc = new StringBuilder();
		SimpleDateFormat validateDate = null;
		String lineSeparator = ";";
		line = line.replaceAll(",", ";");
		line = line.replaceAll("^\"|\"$", "");
		if (fileName.contains(StringConstants.EXCURSION_CALENDAR)) {
			validateDate = new SimpleDateFormat(
					StringConstants.EXCURSION_CAL_DATEFORMAT);
		} else if (fileName.contains(StringConstants.FOOD_CALENDAR)) {
			validateDate = new SimpleDateFormat(
					StringConstants.FOOD_CAL_DATEFORMAT);
		}
		String[] lineTokens = line.split(lineSeparator);
		if (lineTokens.length < 3) {
			errorCode = "DC8051";
			errorDesc.append(
					String.format(StringConstants.ERROR_DC8051, fileName,
							strLine)).append(StringConstants.TILD);
			isInvalid = true;
		} else {

			String categoryCode = lineTokens[0].trim();
			String categoryDate = lineTokens[1].trim();
			String action = lineTokens[2].trim();
			// category cdoe validation
			if (null == categoryCode || categoryCode.isEmpty()) {
				errorCode = "DC8052";
				errorDesc.append(
						String.format(StringConstants.ERROR_DC8052, fileName,
								strLine)).append(StringConstants.TILD);
				isInvalid = true;
			}

			// category date validation
			if (null != categoryDate && categoryDate.length() == 10) {

				try {
					validateDate.parse(categoryDate);
				} catch (ParseException e) {
					isInvalid = true;
					errorCode = "DC8053";
					errorDesc.append(
							String.format(StringConstants.ERROR_DC8053,
									fileName, strLine)).append(
							StringConstants.TILD);
				}
			} else {
				errorCode = "DC8053";
				errorDesc.append(
						String.format(StringConstants.ERROR_DC8053, fileName,
								strLine)).append(StringConstants.TILD);
				isInvalid = true;
			}

			if (!actionList.contains(action)) {
				errorCode = "DC8054";
				errorDesc.append(
						String.format(StringConstants.ERROR_DC8054, fileName,
								strLine)).append(StringConstants.TILD);
				isInvalid = true;
			}
		}
		createErrorNotification(line, fileName, readExcelFile, errorCode,
				errorDesc);
		return isInvalid + "~" + errorCode + "~" + line;
	}

	/**
	 * 
	 * @param line
	 * @param fileName
	 * @param readExcelFile
	 * @param errorCode
	 * @param errorDesc
	 */
	private void createErrorNotification(String line, String fileName,
			ReadExcelFile readExcelFile, String errorCode,
			StringBuilder errorDesc) {
		if (!"NA".equals(errorCode)) {
			String errorRecord = fileName + StringConstants.TILD + line
					+ StringConstants.TILD + errorDesc.toString();
			try {
				readExcelFile.insertNotificationRow(errorCode, errorRecord);
			} catch (DCServiceException e) {
				EVL.sendException(e.getMessage(), EventType.FATAL, null, e,
						null);
			}
		}
	}

	/**
	 * 
	 * @param folder
	 * @return
	 */
	private File[] listFilesForFolder(final File folder) {
		return folder.listFiles();
	}

	/**
	 * @return boolean
	 */
	private boolean initializeParams() {
		return true;
	}
}
