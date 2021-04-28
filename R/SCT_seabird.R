#' @title SCT_seabird
#' @description Function that standardizes seabird formatted files
#' @param fn The data to process
#' @param ret List of already procesed SCHEADER data
#' @param uid Pass determined uid
#' @param lat Pass determined lat
#' @param lon Pass determined lon
#' @param depth Pass determined depth
#' @import lubridate
#' @return List of formatted data
#' @export
SCT_seabird = function(fn, ret, uid, lat, lon, depth){

  ind = grep("start sample number", readLines(fn, n = 100), ignore.case = TRUE)
  header = readLines(fn, n = ind)
  if (any(grepl("<SCHEADER>", header)) &&
      any(grepl("</SCHEADER>", header))) {
    header = header[-(grep("<SCHEADER>", header):grep("</SCHEADER>", header))]
  }
  header = gsub("\t", "", header)
  header = gsub("deg C", "°C", header)
  header = gsub("deg F", "°F", header)
  header = gsub(",,", "", header)

  data = read.csv(fn, skip = ind, header = F)
  if (ncol(data) == 1)
    data = read.delim(fn, skip = ind)
  names(data) = c("temperature", "pressure", "date", "time")

  #Expected to return data

  optheader = header[grep("** ", header, fixed = T)]
  optheader = gsub("** ", "", optheader, fixed = T)
  ret$Project = optheader[1]

  instr = header[grep("S>", header, fixed = T)+1][1]
  instr = unlist(strsplit(instr, "   "))
  ret$RecorderType = instr[1]
  ret$RecorderID = instr[2]

  ret$StartDate = trimws(unlist(strsplit(header[grep("start time", header, ignore.case = TRUE)], "="))[2])
  ret$EndDate = unlist(strsplit(header[grep("* ds", header, fixed = T)+1], "    "))[2]
  #ret$RecordingRate = trimws(unlist(strsplit(header[grep("sample interval", header, ignore.case = TRUE)], "="))[2])
  ret$File = trimws(unlist(strsplit(header[grep("Filename", header, ignore.case = TRUE)], "= "))[2])

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
  if (any(grepl("deg C", header, ignore.case = TRUE)))
    ret$UnitsforTemp = "Celsius"
  if (any(grepl("deg F", header, ignore.case = TRUE)))
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


  ret$StartDate = dmy_hms(ret$StartDate, tz = tz)

  ret$EndDate = dmy_hms(ret$EndDate, tz = tz)

  #NORMALISE DATA

  #Convert date
  T_DATE = "NA"


  datecolumn = grep("date", names(data), ignore.case = T)
  timecolumn = grep("time", names(data), ignore.case = T)

  T_DATE = lubridate::dmy_hms(paste(data[, datecolumn], data[, timecolumn], sep = " "), tz = tz)
  td = seconds_to_period(difftime(T_DATE[6], T_DATE[5], units = "secs"))
  ret$RecordingRate = sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td)) #ret$File = basename(fn)

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
  #Convert presure to depth
  DEPTH_M = decibar2depth(data$pressure, LAT_DD[1], Del=0 )

  ret$data = data.frame(T_UID, T_DATE, LAT_DD, LON_DD, TEMP, DEPTH_M, stringsAsFactors =  F)
  return(ret)
}
