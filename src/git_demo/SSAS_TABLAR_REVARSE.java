/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.ssas.util;

import com.erwin.cfx.connectors.ssas.files.util.SSAS_Files;
import com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Util;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.logger.HtmlLogger;
import com.erwin.cfx.connectors.logger.utility.HtmlLoggerUtils;
import com.erwin.cfx.connectors.ssas.connection.util.SSAS_CONNECTION_Util;
import com.erwin.cfx.connectors.ssas.files.util.SSAS_ArchiveFileUtil;
import com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Bean;
import com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_LEVEL_Type;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_LEVEL_Type.ERROR_TYPE;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_LEVEL_Type.FOOTER_TYPE;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_LEVEL_Type.HEADER_TYPE;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_LEVEL_Type.INFO_TYPE;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_LEVEL_Type.WARNING;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Type.BOTH_LOG;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Type.DETAILED_LOG;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Type.OVERVIEW_LOG;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Util.getTimestampForFooterLogging;
import static com.erwin.cfx.connectors.ssas.logger.util.SSAS_LOG_Util.getTimestampForHeaderLogging;
import com.erwin.cfx.connectors.ssas.metadata.util.SSAS_METADATA_Util;
import com.erwin.cfx.connectors.ssas.sql.util.SSAS_SQL_Util;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author Azaruddin mohammad
 */
public class SSAS_TABLAR_REVARSE {

    public static SSAS_Bean ssas_bean = null;

    public static String ssas_Table_Reverse_Parsing(Map<String, String> inputSSASPropertiesMap, MappingManagerUtil mmUtil, SystemManagerUtil smUtil) throws IOException {
        try {
            long startTime = System.currentTimeMillis();
            SET_SSAS_INPUTS(inputSSASPropertiesMap, mmUtil, smUtil);
            set_LogUtil_BeanData();
            //create live log Object
            createLiveLog();
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setStartTimestamp(new Timestamp(startTime).toString());
            ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());
            ssas_bean.getSSAS_LOG_Util().appendLogData(" =================================================\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" ================= SSAS TABULAR REVERSE ==========\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" =================================================\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Project Name : " + ssas_bean.getSSAS_PROJECT_NAME() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Version Control : " + ssas_bean.getSSAS_VERSION_TYPE() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Subject Details : " + ssas_bean.getSSAS_SUBJECT_NAME() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Source File Management : " + ssas_bean.getSSAS_SRC_FILE_MANAGEMENT() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Metadata Creation : " + ssas_bean.isSSAS_METADATA_OPTION() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Input File Option : " + ssas_bean.getSSAS_FILE_TYPE() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            if (ssas_bean.getSSAS_FILE_TYPE().equalsIgnoreCase("FilePath")) {
                ssas_bean.getSSAS_LOG_Util().appendLogData(" XMLA Files Path : " + ssas_bean.getSSAS_XMLA_PATH() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            }
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Metadata File Path : " + ssas_bean.getSSAS_META_PATH() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Log Type : " + ssas_bean.getSSAS_LOG_TYPE() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Log File Path : " + ssas_bean.getSSAS_LOG_PATH() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Archive Source File Path : " + ssas_bean.getSSAS_ARCHIVE_PATH() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" Special Characters Replacement : " + ssas_bean.getSSAS_SPL_CHAR_REPLACE() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            Map<String, String> ssas_SplCharMap = getSSASSpecialCharacters();
            if (!ssas_SplCharMap.isEmpty()) {
                ssas_bean.getSSAS_LOG_Util().appendLogData(" Special Characters  : " + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
                for (Map.Entry<String, String> map : ssas_SplCharMap.entrySet()) {
                    ssas_bean.getSSAS_LOG_Util().appendLogData("                      key : " + map.getKey() + " value : " + map.getValue() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
                }
            }
            ssasSetHeaderInfo(ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(" =================================================\n\n", "BOTH", SSAS_LOG_LEVEL_Type.INFO_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData("\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), "", ssas_bean);

            HashSet<String> ssas_FilesList = SSAS_Files.get_SSASFiles(ssas_bean.getSSAS_XMLA_PATH());
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setTotalFiles(ssas_FilesList.size());
            ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());
            try {
                for (String each_ssas_File : ssas_FilesList) {
                    ssas_bean.getsSAS_XMLA_Table().parse_SSAS_XMLA_JsonFile(each_ssas_File, ssas_bean);
                    int fileCount = ssas_bean.getSSAS_FILE_COUNT();
                    fileCount++;
                    ssas_bean.setSSAS_FILE_COUNT(fileCount);
                    ssas_bean.getSSAS_HTML_LOGGER_UTILS().setTotalFilesCompleted(ssas_bean.getSSAS_FILE_COUNT());
                    ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());
                }
            } catch (Exception e) {
                int errorCount = ssas_bean.getSSAS_ERROR_COUNT();
                errorCount++;
                ssas_bean.setSSAS_ERROR_COUNT(errorCount);
                StringWriter exception = new StringWriter();
                e.printStackTrace(new PrintWriter(exception));
                ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.parse_SSAS_XMLA_JsonFile() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
            }
            if (ssas_bean.getSSAS_SRC_FILE_MANAGEMENT().equalsIgnoreCase("ArchiveSourceFiles")) {
                try {
                    File directory = new File(ssas_bean.getSSAS_ARCHIVE_PATH());
                    File ssasFileDirectry = new File(ssas_bean.getSSAS_XMLA_PATH());
                    ssas_bean.getSSAS_ArchiveFileUtil().moveToArchive(ssasFileDirectry, directory);
                } catch (Exception e) {
                    int errorCount = ssas_bean.getSSAS_ERROR_COUNT();
                    errorCount++;
                    ssas_bean.setSSAS_ERROR_COUNT(errorCount);
                    StringWriter exception = new StringWriter();
                    e.printStackTrace(new PrintWriter(exception));
                    ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_SRC_FILE_MANAGEMENT() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                }
            } else if (ssas_bean.getSSAS_SRC_FILE_MANAGEMENT().equalsIgnoreCase("DeleteSourceFiles")) {
                try {
                    File ssasFileDirectry = new File(ssas_bean.getSSAS_XMLA_PATH());
                    if (ssasFileDirectry.isDirectory()) {
                        for (File file1 : ssasFileDirectry.listFiles()) {
                            if (file1.getName().contains("bim") || file1.getName().contains("xmla")) {
                                if (file1.delete()) {
                                    ssas_bean.getSSAS_LOG_Util().appendLogData(" " + file1.getName() + " deleted successfully" + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                                } else {
                                    ssas_bean.getSSAS_LOG_Util().appendLogData(" " + file1.getName() + " not deleted  " + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                                }
                            }
                        }
                    } else {
                        if (ssasFileDirectry.getName().contains("bim") || ssasFileDirectry.getName().contains("xmla")) {
                            if (ssasFileDirectry.delete()) {
                                ssas_bean.getSSAS_LOG_Util().appendLogData(" " + ssasFileDirectry.getName() + " deleted successfully" + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                            } else {
                                ssas_bean.getSSAS_LOG_Util().appendLogData(" " + ssasFileDirectry.getName() + " not deleted  " + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                            }
                        }
                    }
                } catch (Exception e) {
                    int errorCount = ssas_bean.getSSAS_ERROR_COUNT();
                    errorCount++;
                    ssas_bean.setSSAS_ERROR_COUNT(errorCount);
                    StringWriter exception = new StringWriter();
                    e.printStackTrace(new PrintWriter(exception));
                    ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_SRC_FILE_MANAGEMENT() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                }
            }
            ssas_bean.getSSAS_ArchiveFileUtil().deleteFile(ssas_bean);
            long endTime = System.currentTimeMillis();
            set_SSAS_Footer_Level_Data(ssas_bean, startTime, endTime);
            ssas_bean.getsSAS_TABLAR_REVARSE().updateTotalFiles_in_LiveLog(ssas_bean, startTime, endTime);
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.ssas_Table_Reverse_Parsing() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
        try {
            return FileUtils.readFileToString(new File(ssas_bean.getSSAS_LOG_FILE_PATH()));
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            return exception.toString();
        }
    }

    public static void createLiveLog() {
        try {
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(Calendar.getInstance().getTime());
            HashMap<String, String> htmlLoggerInfoMap = new HashMap<String, String>();
            htmlLoggerInfoMap.put("_loggerName", ssas_bean.getSSAS_DYNAMIC_LOGNAME() + ".html");
            htmlLoggerInfoMap.put("_supportFilesPath", ssas_bean.getSSAS_SUPPORT_FILE_PATH());
            htmlLoggerInfoMap.put("_connectorNm", ssas_bean.getSSAS_CONNECTER_NAME());
            htmlLoggerInfoMap.put("_hostUrl_fileName", ssas_bean.getSSAS_HOST_URL() + ssas_bean.getSSAS_DYNAMIC_LOGNAME() + "_" + timeStamp + ".html");
            htmlLoggerInfoMap.put("_recieverMailId", ssas_bean.getSSAS_RECIVED_MAIL_ID());
            htmlLoggerInfoMap.put("_ccList", ssas_bean.getSSAS_CC_LIST());

            HtmlLogger htmlLogger = new HtmlLogger(htmlLoggerInfoMap, true, true);
            HtmlLoggerUtils htmlLoggerUtils = new HtmlLoggerUtils(ssas_bean.getSSAS_CONNECTER_NAME());
            htmlLogger.htmlUpdateStatus(htmlLoggerUtils);
            ssas_bean.setSSAS_DYNAMIC_URL("");
            ssas_bean.setSSAS_HTML_LOGGER_UTILS(htmlLoggerUtils);
            ssas_bean.setSSAS_HTML_LOGGER(htmlLogger);
            ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());

        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.createLiveLog() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);

        }
    }

 

    public static void set_LogUtil_BeanData() {
        try {
            ssas_bean.getSSAS_LOG_Bean().setBOTH_LOG(BOTH_LOG);
            ssas_bean.getSSAS_LOG_Bean().setDETAIL_LOG(DETAILED_LOG);
            ssas_bean.getSSAS_LOG_Bean().setOVERVIEW_LOG(OVERVIEW_LOG);
            ssas_bean.getSSAS_LOG_Bean().setINFO_TYPE(INFO_TYPE);
            ssas_bean.getSSAS_LOG_Bean().setHEADER_TYPE(HEADER_TYPE);
            ssas_bean.getSSAS_LOG_Bean().setFOOTER_TYPE(FOOTER_TYPE);
            ssas_bean.getSSAS_LOG_Bean().setERROR_TYPE(ERROR_TYPE);
            ssas_bean.getSSAS_LOG_Bean().setWARNING(WARNING);
            ssas_bean.setSSAS_DELIMETER("#ERWIN#");
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.set_LogUtil_BeanData() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
    }

    public static void set_SSAS_Footer_Level_Data(SSAS_Bean ssas_bean, long startTime, long endTime) {
        try {
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "DIS::API_Project_creation_Call :: API_Response= \" Project Created \"\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Execution Start Time :" + new Timestamp(startTime) + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Total Number of Source Files :" + ssas_bean.getSSAS_FILE_COUNT() + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Parsed item Files :" + ssas_bean.getSSAS_FILE_COUNT() + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Number of Projects Created :" + ssas_bean.getSSAS_PROJECT_COUNT() + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "DIS::Number of Subjects created :" + ssas_bean.getSSAS_SUB_CONUNT() + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Number of Mappings created :" + ssas_bean.getSSAS_MAPPING_COUNT() + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Number of Errors :" + ssas_bean.getSSAS_ERROR_COUNT() + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Execution End Time :" + new Timestamp(endTime) + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForFooterLogging() + "Total Execution Time :" + ssas_bean.getsSAS_TABLAR_REVARSE().MillisToDayHrMinSec(endTime - startTime) + "\n", "BOTH", ssas_bean.getSSAS_LOG_Bean().getFOOTER_TYPE(), ssas_bean);

        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.set_SSAS_Footer_Level_Data() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
    }

    public void updateTotalFiles_in_LiveLog(SSAS_Bean ssas_bean, long startTime, long endTime) {
        try {
            // live Log  
            LinkedHashMap<String, String> upperStatCard = new LinkedHashMap<String, String>();
            upperStatCard.put("Total Mappings", "" + ssas_bean.getSSAS_MAPPING_COUNT());
            upperStatCard.put("Mappings Sucess", "" + ssas_bean.getSSAS_MAPPING_COUNT());
            upperStatCard.put("Mappings Failed", "" + ssas_bean.getSSAS_FAIL_MAP_COUNT());
            upperStatCard.put("TotalFile Count", "" + ssas_bean.getSSAS_FILE_COUNT());
            upperStatCard.put("TotalFile Error Count", "" + ssas_bean.getSSAS_ERROR_COUNT());
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setUpperStatData(upperStatCard);
            ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setEndTimestamp(new Timestamp(endTime).toString());
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setTotalExecutionTime(ssas_bean.getsSAS_TABLAR_REVARSE().MillisToDayHrMinSec(endTime - startTime));
//            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setStatusBar("Completed", "Green");
            ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());
            // pie chart
            HashMap<String, Integer> pieMap1 = new HashMap();
            pieMap1.put("Mappings Sucess", ssas_bean.getSSAS_MAPPING_COUNT());
            pieMap1.put("Mappings Failed", ssas_bean.getSSAS_FAIL_MAP_COUNT());
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setPieChartMap1(pieMap1);
            HashMap<String, Integer> pieMap2 = new HashMap();
            pieMap2.put("TotalFile Count", ssas_bean.getSSAS_FILE_COUNT());
            pieMap2.put("TotalFile Error Count", ssas_bean.getSSAS_ERROR_COUNT());
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setPieChartMap2(pieMap2);
            ssas_bean.getSSAS_HTML_LOGGER_UTILS().setMainLogTableXaxisLen("0");
            ssas_bean.getSSAS_HTML_LOGGER().htmlUpdateStatus(ssas_bean.getSSAS_HTML_LOGGER_UTILS());
            // Stop live Log
            ssas_bean.getSSAS_HTML_LOGGER().loggerStop();
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.updateTotalFiles_in_LiveLog() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
    }

    public static Map<String, String> getSSASSpecialCharacters() {
        Map<String, String> ssas_SpecialCharMap = new HashMap<>();
        String ssas_SplCharFilePath = ssas_bean.getSSAS_SPL_CHAR_FILE_PATH();
        try {
            if (ssas_bean.getSSAS_SPL_CHAR_REPLACE().equalsIgnoreCase("replace")) {
                try {
                    String splCharReplacelines = FileUtils.readFileToString(new File(ssas_SplCharFilePath), "UTF-8");
                    String[] linearr = splCharReplacelines.split("\n");
                    for (String connectionInfo : linearr) {
                        if (connectionInfo.split(";").length >= 1) {
                            String splChar = connectionInfo.split(";")[0];
                            String replacement = connectionInfo.split(";")[1];
                            ssas_SpecialCharMap.put(splChar, replacement);
                        }
                    }
                } catch (IOException e) {
                    StringWriter exception = new StringWriter();
                    e.printStackTrace(new PrintWriter(exception));
                    ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.get_SSAS_Special_Characters() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
                }
            }
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.get_SSAS_Special_Characters() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
        ssas_bean.setSSAS_SPL_CHERMAP(ssas_SpecialCharMap);
        return ssas_SpecialCharMap;
    }

    public static void ssasSetHeaderInfo(SSAS_Bean ssas_bean) {
        try {
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter :: Connector Name ::" + ssas_bean.getSSAS_CONNECTER_NAME() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter :: DIS Version :: " + ssas_bean.getSSAS_DIS_VERSION() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter :: projectName :: \"" + ssas_bean.getSSAS_PROJECT_NAME() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter :: subjectName :: \"" + ssas_bean.getSSAS_SUBJECT_NAME() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter :: Version Control :: \"" + ssas_bean.getSSAS_VERSION_TYPE() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter :: Source File Management :: " + ssas_bean.getSSAS_SRC_FILE_MANAGEMENT() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            if (ssas_bean.getSSAS_FILE_TYPE().equalsIgnoreCase("FilePath")) {
                ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Input File Option=”File Path” :: " + ssas_bean.getSSAS_XMLA_PATH() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            } else if (ssas_bean.getSSAS_FILE_TYPE().equals("UploadFiles")) {
                ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Input File Option=”File Upload” :: " + "\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            }
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Input File Option=”Uploaded file path” :: " + ssas_bean.getSSAS_XMLA_PATH() + "\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Metadata File Path :: \"" + ssas_bean.getSSAS_META_PATH() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);

            ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Log File Path :: \"" + ssas_bean.getSSAS_LOG_PATH() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            if (!ssas_bean.getSSAS_ARCHIVE_PATH().isEmpty()) {
                ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Archive File Path :: \"" + ssas_bean.getSSAS_ARCHIVE_PATH() + "\"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            } else {
                ssas_bean.getSSAS_LOG_Util().appendLogData(getTimestampForHeaderLogging() + "Input Parameter::Archive File Path :: \"" + "Archive file path is not provided!! \"\n", "BOTH", SSAS_LOG_LEVEL_Type.HEADER_TYPE, ssas_bean);
            }
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.ssasSetHeaderInfo() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
    }

    public static void SET_SSAS_INPUTS(Map<String, String> inputSSASPropertiesMap, MappingManagerUtil mmUtil, SystemManagerUtil smutil) {
        try {
            ssas_bean = new SSAS_Bean();
            ssas_bean.setSSAS_MMUTIL(mmUtil);
            ssas_bean.setSSAS_SMUTIL(smutil);
            ssas_bean.setSSAS_TABLE_BUSINESS_ENTITY(inputSSASPropertiesMap.get("TABLE_BUSINESSENTITY"));
            ssas_bean.setSSAS_DEF_SRC_ENV(inputSSASPropertiesMap.get("DEF_SRC_ENVNAME"));
            ssas_bean.setSSAS_DEF_SRC_SYS(inputSSASPropertiesMap.get("DEF_SRC_SYSNAME"));
            ssas_bean.setSSAS_LOG_TYPE(inputSSASPropertiesMap.get("LOG_FILE_INFO"));
            ssas_bean.setSSAS_DEF_SYS(inputSSASPropertiesMap.get("DEF_SYSNAME"));
            ssas_bean.setSSAS_ARCHIVE_PATH(inputSSASPropertiesMap.get("ARCHIVE_PATH"));
            if (!new File(inputSSASPropertiesMap.get("ARCHIVE_PATH")).exists()) {
                new File(inputSSASPropertiesMap.get("ARCHIVE_PATH")).mkdirs();
            }
            ssas_bean.setSSAS_FILE_TYPE(inputSSASPropertiesMap.get("FILE_DIRECTORY_OPTION"));
            if (!new File(inputSSASPropertiesMap.get("FILE_DIRECTORY_OPTION")).isDirectory()) {
                new File(inputSSASPropertiesMap.get("FILE_DIRECTORY_OPTION")).mkdirs();
            }
            ssas_bean.setSSAS_SUBJECT_NAME(inputSSASPropertiesMap.get("SUBJECT_DATA"));
            ssas_bean.setSSAS_SRC_FILE_MANAGEMENT(inputSSASPropertiesMap.get("SOURCE_FILE_MANAGEMENT"));
            ssas_bean.setSSAS_LOG_PATH(inputSSASPropertiesMap.get("LOG_PATH"));
            if (!new File(inputSSASPropertiesMap.get("LOG_PATH")).isDirectory()) {
                new File(inputSSASPropertiesMap.get("LOG_PATH")).mkdirs();
            }
            ssas_bean.setSSAS_META_PATH(inputSSASPropertiesMap.get("METADATA_DIRECTORY"));
            if (!new File(inputSSASPropertiesMap.get("METADATA_DIRECTORY")).isDirectory()) {
                new File(inputSSASPropertiesMap.get("METADATA_DIRECTORY")).mkdirs();
            }
            ssas_bean.setSSAS_MODEL_PATH(inputSSASPropertiesMap.get("MODEL_FILEPATH"));
            if (!new File(inputSSASPropertiesMap.get("MODEL_FILEPATH")).isDirectory()) {
                new File(inputSSASPropertiesMap.get("MODEL_FILEPATH")).mkdirs();
            }
            ssas_bean.setSSAS_MODEL_OPTION(inputSSASPropertiesMap.get("MODEL_CREATATION_INFO"));
            ssas_bean.setSSAS_SPL_CHAR_REPLACE(inputSSASPropertiesMap.get("SPL_CHAR_REPLACEMENT"));
            boolean metadataCreation = false;
            metadataCreation = Boolean.parseBoolean(inputSSASPropertiesMap.get("METADATA_CREATION"));
            ssas_bean.setSSAS_METADATA_OPTION(metadataCreation);
            ssas_bean.setSSAS_DIS_VERSION(inputSSASPropertiesMap.get("DIS_VERSION"));
            ssas_bean.setSSAS_SPL_CHAR_FILE_PATH(inputSSASPropertiesMap.get("SPL_CHER_FILEPATH"));
            if (!new File(inputSSASPropertiesMap.get("SPL_CHER_FILEPATH")).isDirectory()) {
                new File(inputSSASPropertiesMap.get("SPL_CHER_FILEPATH")).mkdirs();
            }
            ssas_bean.setSSAS_CONNECTER_NAME(inputSSASPropertiesMap.get("CONNECTOR_NAME"));
            ssas_bean.setSSAS_PROJECT_NAME(inputSSASPropertiesMap.get("PROJECT_NAME"));
            ssas_bean.setSSAS_VERSION_TYPE(inputSSASPropertiesMap.get("VERSION_CONTROL"));
            ssas_bean.setSSAS_XMLA_PATH(inputSSASPropertiesMap.get("XMLA_PATH"));
//            ssas_bean.setSSAS_MAPPING_TYPE(inputSSASPropertiesMap.get("MAPPING_TYPE"));
            ssas_bean.setSSAS_DELIMETER("#ERWIN#");
            ssas_bean.setSSAS_ERROR_COUNT(0);
            ssas_bean.setSSAS_SUB_CONUNT(0);
            ssas_bean.setSSAS_FILE_COUNT(0);
            ssas_bean.setSSAS_MAPPING_COUNT(0);
            SSAS_LOG_Util ssas_log_util = new SSAS_LOG_Util();
            ssas_bean.setSSAS_LOG_Util(ssas_log_util);
            String ssas_LogFile_path = ssas_bean.getSSAS_LOG_PATH() + File.separator + "SSAS_Log_" + getLogTimestamp() + ".txt";
            ssas_bean.setSSAS_LOG_FILE_PATH(ssas_LogFile_path);
            SSAS_METADATA_Util mETADATA_Util = new SSAS_METADATA_Util();
            ssas_bean.setmETADATA_Util(mETADATA_Util);
            SSAS_SQL_Util sQL_Util = new SSAS_SQL_Util();
            ssas_bean.setsQL_Util(sQL_Util);
            SSAS_CONNECTION_Util cONNECTION_Util = new SSAS_CONNECTION_Util();
            ssas_bean.setcONNECTION_Util(cONNECTION_Util);
            SSAS_TABLAR_REVARSE sSAS_TABLAR_REVARSE = new SSAS_TABLAR_REVARSE();
            ssas_bean.setsSAS_TABLAR_REVARSE(sSAS_TABLAR_REVARSE);
            SSAS_XMLA_Table sSAS_XMLA_Table = new SSAS_XMLA_Table();
            ssas_bean.setsSAS_XMLA_Table(sSAS_XMLA_Table);
            SSAS_LOG_Bean sSAS_LOG_Bean = new SSAS_LOG_Bean();
            ssas_bean.setSSAS_LOG_Bean(sSAS_LOG_Bean);
            boolean createHiddenTableMetadata = false;
            if (inputSSASPropertiesMap.containsKey("CREATE_HIDDEN_TABLE_METADATA")) {
                createHiddenTableMetadata = Boolean.parseBoolean(inputSSASPropertiesMap.get("CREATE_HIDDEN_TABLE_METADATA"));
            }
            ssas_bean.setSSAS_CREATE_HIDDEN_TABLE_METADATA(createHiddenTableMetadata);
            HashMap<String, String> allTabsMap = SyncupWithServerDBSchamaSysEnvCPT.getMap(ssas_bean.getSSAS_META_PATH(), "Tables");
            HashMap<String, String> allDBsMap = SyncupWithServerDBSchamaSysEnvCPT.getMap(ssas_bean.getSSAS_META_PATH(), "Databases");
            ssas_bean.setSSAS_ALL_DB_MAP(allDBsMap);
            ssas_bean.setSSAS_ALL_TABLE_MAP(allTabsMap);
            SSAS_ArchiveFileUtil archiveFileUtil = new SSAS_ArchiveFileUtil();
            ssas_bean.setSSAS_ArchiveFileUtil(archiveFileUtil);
            ssas_bean.setSSAS_CC_LIST(inputSSASPropertiesMap.get("CC_LIST"));
            ssas_bean.setSSAS_RECIVED_MAIL_ID(inputSSASPropertiesMap.get("RECEIVED_MAIL_ID"));
            ssas_bean.setSSAS_HOST_URL(inputSSASPropertiesMap.get("HOST_URL"));
            ssas_bean.setSSAS_SUPPORT_FILE_PATH(inputSSASPropertiesMap.get("SUPPORT_FILE_PATH"));
            ssas_bean.setSSAS_DYNAMIC_LOGNAME(inputSSASPropertiesMap.get("SSAS_DYNAMIC_LOGNAME"));
            ssas_bean.setSSAS_SRC_SHAREPOINT_ENV("SHARE POINT");
        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            ssas_bean.getSSAS_LOG_Util().appendLogData("Exception In SSAS_TABLAR_REVARSE.SET_SSAS_INPUTS() Method : " + exception + "\n", ssas_bean.getSSAS_LOG_Bean().getDETAIL_LOG(), ssas_bean.getSSAS_LOG_Bean().getERROR_TYPE(), ssas_bean);
        }
    }

    public Set<String> getMeasureColumns(String finalMeasure) {
        Set<String> cols = new LinkedHashSet<>();
        Pattern pattern2 = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-']*\\[.*?\\]");
        Matcher matcher = pattern2.matcher(finalMeasure);
        if (matcher.matches()) {
            while (matcher.find()) {
                String matchString = matcher.group();
                cols.add(matchString.split("\\[")[1].replace("'", "").replace("]", "").trim());
//                System.out.println(matchString);
            }
        } else {
            while (matcher.find()) {
                String matchString = matcher.group();
                cols.add(matchString.split("\\[")[1].replace("'", "").replace("]", "").trim());
//                System.out.println(matchString.split("\\[")[1].replace("'", "").replace("]", ""));
            }
        }
        return cols;
    }

    public static String getLogTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Date date = new Date();
        String logTimeStamp = sdf.format(date);
        return logTimeStamp;
    }

    public String MillisToDayHrMinSec(long milliseconds) {

        final long dy = TimeUnit.MILLISECONDS.toDays(milliseconds);
        final long hr = TimeUnit.MILLISECONDS.toHours(milliseconds)
                - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(milliseconds));
        final long min = TimeUnit.MILLISECONDS.toMinutes(milliseconds)
                - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(milliseconds));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(milliseconds)
                - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(milliseconds));
        final long ms = TimeUnit.MILLISECONDS.toMillis(milliseconds)
                - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(milliseconds));
        return (String.format("%d Days %d Hours %d Minutes %d Seconds %d Milliseconds", dy, hr, min, sec, ms));

    }

}
