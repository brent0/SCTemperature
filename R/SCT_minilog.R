#' @title SCT_minilog
#' @description Function that standardizes minilog formatted files
#' @param fn The data to process
#' @param ret List of already procesed SCHEADER data
#' @param uid Pass determined uid
#' @param lat Pass determined lat
#' @param lon Pass determined lon
#' @param depth Pass determined depth
#' @import lubridate
#' @return List of formatted data
#' @export
SCT_minilog = function(fn, ret, uid, lat, lon, depth){

miniold = F

ind = grep("Date\\(", readLines(fn, n = 50), ignore.case = TRUE)
if(length(ind)==0){
 # ind = grep("Date(", readLines(fn, n = 50), ignore.case = TRUE)
}
header = readLines(fn, n = ind)
if (any(grepl("<SCHEADER>", header)) &&
    any(grepl("</SCHEADER>", header))) {
  header = header[-(grep("<SCHEADER>", header):grep("</SCHEADER>", header))]
}
header = gsub("\t", "", header)
header = gsub("\\?C", "°C", header)
header = gsub("\\?F", "°F", header)
header = gsub(",,", "", header)

if (any(grepl("* ", header, fixed = T)))
  miniold = TRUE

data = read.csv(fn, skip = ind - 1)
if (ncol(data) == 1)
  data = read.delim(fn, skip = ind - 1)


#Expected to return data
if (miniold)
  ret$Project = trimws(unlist(strsplit(header[grep("Study ID", header, ignore.case = TRUE)], "="))[2])
else
  ret$Project = trimws(unlist(strsplit(header[grep("Study Description", header, ignore.case = TRUE)], ": "))[2])
if (miniold) {
  ret$RecorderType = trimws(unlist(strsplit(header[grep("ID=Min", header, ignore.case = TRUE)], "="))[2])
  ret$RecorderID = trimws(unlist(strsplit(header[grep("Serial Number", header, ignore.case = TRUE)], "="))[2])
}
else{
  topars = trimws(unlist(strsplit(header[grep("Source Device", header, ignore.case = TRUE)], ":"))[2])
  topars = unlist(strsplit(topars, "-"))
  ret$RecorderType = paste(topars[1], topars[2], topars[3], sep = "-")
  ret$RecorderID = topars[4]
}
if (miniold)
  ret$StartDate = trimws(unlist(strsplit(header[grep("Start Time", header, ignore.case = TRUE)], "="))[2])
else
  ret$StartDate = trimws(unlist(strsplit(header[grep("Study Start", header, ignore.case = TRUE)], ": "))[2])
if (miniold)
  ret$EndDate = trimws(unlist(strsplit(header[grep("Finish Time", header, ignore.case = TRUE)], "="))[2])
else
  ret$EndDate = trimws(unlist(strsplit(header[grep("Study Stop", header, ignore.case = TRUE)], ": "))[2])
if (miniold)
  ret$RecordingRate = trimws(unlist(strsplit(header[grep("Sample Period", header, ignore.case = TRUE)], "="))[2])
else
  ret$RecordingRate = trimws(unlist(strsplit(header[grep("Sample Interval", header, ignore.case = TRUE)], ": "))[2])
if (miniold)
  ret$File = basename(fn)
else
  ret$File = trimws(unlist(strsplit(header[grep("Source File", header, ignore.case = TRUE)], ": "))[2])

##Need to pull out UTC-3 or whatever and set to tz variable
timediffutc = NULL
tz = "America/Halifax"

if (any(grepl("UTC", header, fixed = T))) {
  timediffutc = trimws(unlist(strsplit(header[grep("UTC", header, ignore.case = TRUE)], "UTC"))[2])
  timediffutc = gsub(")", "", timediffutc)
  timediffutc = gsub(",", "", timediffutc)
  timediffutc = as.numeric(timediffutc) * -1
  timediffutc = hours(timediffutc)

  tz = "UTC"

}


ret$UnitsforTemp = "Unknown"
##If fahrenheit we should convert
if (any(grepl("°C", header, ignore.case = TRUE)))
  ret$UnitsforTemp = "Celsius"
if (any(grepl("°F", header, ignore.case = TRUE)))
  ret$UnitsforTemp = "Fahrenheit"

#NOT Expected to return data, but will try anyway
ret$Port = trimws(unlist(strsplit(header[grep("Port", header, ignore.case = TRUE)], ": "))[2])
LAT_DD = trimws(unlist(strsplit(header[grep("latitude", header, ignore.case = TRUE)], ": "))[2])
LON_DD = trimws(unlist(strsplit(header[grep("longitude", header, ignore.case = TRUE)], ": "))[2])
ret$Observed_Depth = trimws(unlist(strsplit(header[grep("Depth", header, ignore.case = TRUE)], ": "))[2])
ret$QAQC = trimws(unlist(strsplit(header[grep("QAQC", header, ignore.case = TRUE)], ": "))[2])
ret$Notes  = trimws(unlist(strsplit(header[grep("Notes", header, ignore.case = TRUE)], ": "))[2])


if (length(ret$Port) == 0)
  ret$Port = NULL
if (length(ret$Observed_Depth) == 0)
  ret$Observed_Depth = NULL
if (length(ret$QAQC) == 0)
  ret$QAQC = ""
if (length(ret$Notes) == 0)
  ret$Notes = ""
if (length(ret$RecorderType) == 0)
  ret$RecorderType = NULL
if (length(ret$RecorderID) == 0)
  ret$RecorderID = NULL


ret$StartDate = ymd_hms(ret$StartDate)

ret$EndDate = ymd_hms(ret$EndDate)

#NORMALISE DATA
#Convert date
T_DATE = "NA"


datecolumn = grep("date", names(data), ignore.case = T)
if (any(grepl("yyyy-mm-dd", header, ignore.case = TRUE))) {
  timecolumn = grep("time", names(data), ignore.case = T)
  if (any(grepl("hh:mm:ss", header, ignore.case = TRUE))) {
    ds = data[, datecolumn][1]

    if (grepl("-", ds)) {
      datedat = paste(data[, datecolumn], data[, timecolumn], sep = " ")
      gf = guess_formats(datedat, c("mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
      gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
      if(gf == "%m-%d-%Y %H:%M") T_DATE =  lubridate::mdy_hm(datedat)
      if(gf == "%m-%d-%Y %H:%M:%S") T_DATE =  lubridate::mdy_hms(datedat)
      if(gf == "%d-%m-%Y %H:%M:%S") T_DATE =  lubridate::dmy_hms(datedat)
      if(gf == "%d-%m-%Y %H:%M") T_DATE =  lubridate::dmy_hm(datedat)
      if(gf == "%Y-%m-%d %H:%M:%S") T_DATE =  lubridate::ymd_hms(datedat)
      if(gf == "%Y-%m-%d %H:%M") T_DATE =  lubridate::ymd_hm(datedat)

      if (is.null(timediffutc)) {}
      else{
        T_DATE = T_DATE + timediffutc
      }
    }
    if (grepl("/", ds)) {
      datedat = paste(data[, datecolumn], data[, timecolumn], sep = " ")
      gf = guess_formats(datedat, c("mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
      gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
      if(gf == "%m/%d/%Y %H:%M") T_DATE =  lubridate::mdy_hm(datedat)
      if(gf == "%m/%d/%Y %H:%M:%S") T_DATE =  lubridate::mdy_hms(datedat)
      if(gf == "%d/%m/%Y %H:%M:%S") T_DATE =  lubridate::dmy_hms(datedat)
      if(gf == "%d/%m/%Y %H:%M") T_DATE =  lubridate::dmy_hm(datedat)
      if(gf == "%Y/%m/%d %H:%M:%S") T_DATE =  lubridate::ymd_hms(datedat)
      if(gf == "%Y/%m/%d %H:%M") T_DATE =  lubridate::ymd_hm(datedat)

      if (is.null(timediffutc)) {}
      else{
        T_DATE = T_DATE + timediffutc
      }
    }
  }
}
if (miniold)
  tempcolumn = 3
else
  tempcolumn = grep("Temperature", names(data), ignore.case = T)

TEMP = as.numeric(data[, tempcolumn])
if (ret$UnitsforTemp == "Fahrenheit") {
  TEMP = ((TEMP - 32) * 5) / 9
  ret$UnitsforTemp = "Celsius"
}

latcolumn = grep("Latitude", names(data), ignore.case = T)
loncolumn = grep("Longitude", names(data), ignore.case = T)
if (length(latcolumn) > 0)
  LAT_DD = data[, latcolumn]
if (length(loncolumn) > 0)
  LON_DD = data[, loncolumn]

if (length(LAT_DD) == 0)
  LAT_DD = lat
if (length(LON_DD) == 0)
  LON_DD = lon

T_UID = rep(uid, length(TEMP))
if (length(LAT_DD) == 1)
  LAT_DD = rep(LAT_DD, length(TEMP))
if (length(LON_DD) == 1)
  LON_DD = rep(LON_DD, length(TEMP))

LAT_DD = as.numeric(LAT_DD)
LON_DD = as.numeric(LON_DD)
if(is.null(ret$Observed_Depth)){
  DEPTH_M = rep(NA, length(TEMP))
}
else{
  DEPTH_M = rep(ret$Observed_Depth, length(TEMP))
}
if(!is.null(depth)){
  DEPTH_M = rep(depth, length(TEMP))
}
depthcolumn = grep("Depth", names(data), ignore.case = T)
if(length(depthcolumn) > 0){
  DEPTH_M = data[, depthcolumn]
}
ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, DEPTH_M, stringsAsFactors =  F)
return(ret)
}
