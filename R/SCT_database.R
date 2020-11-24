#' @title  AddTempMetadata
#' @description  Internal function. Writes metadata to database
#' @import ROracle DBI
#' @export
AddTempMetadata = function(PID = NULL,
                           UID = NULL,
                           Project = NA,
                           Location = NA,
                           RecorderType = NA,
                           RecorderID = NA,
                           StartDate = NA,
                           EndDate = NA,
                           RecordingRate = NA,
                           Port = NA,
                           File = NA,
                           LAT.DD = NA,
                           LON.DD = NA,
                           Observed_Depth = NA,
                           UnitsforTemp = NA,
                           QAQC = "",
                           Notes = NA,
                           HaulDate_Start = NA,
                           HaulDate_End = NA) {

  Sys.setenv(TZ = "America/Halifax")
  Sys.setenv(ORA_SDTZ = "America/Halifax")
  drv = DBI::dbDriver("Oracle")
  con2 =  ROracle::dbConnect(drv,
                             username = oracle.snowcrab.user,
                             password = oracle.snowcrab.password,
                             dbname = oracle.snowcrab.server)

  if (is.null(pkg.env$M_UID)) {
    res = ROracle::dbSendQuery(con2, "select DISTINCT M_UID from SC_TEMPERATURE_META")
    res = ROracle::fetch(res, n = -1)
    #M_UID <<- res$M_UID
    assign('M_UID', res$M_UID, pkg.env)

  }

  if (is.null(pkg.env$T_UID)) {
    res2 <-
      ROracle::dbSendQuery(con2, "select DISTINCT T_UID from SC_TEMPERATURE_BASE")
    res2 <- ROracle::fetch(res2, n = -1)
    #T_UID <<- res2$T_UID
    assign('T_UID', res2$T_UID, pkg.env)

  }

  ROracle::dbDisconnect(con2)


  dx = data.frame(
    as.character(UID),
    as.character(PID),
    as.character(Project),
    as.character(Location),
    as.character(RecorderType),
    as.character(RecorderID),
    StartDate,
    EndDate,
    RecordingRate,
    as.character(Port),
    as.character(File),
    LAT.DD,
    LON.DD,
    as.numeric(as.character(Observed_Depth)),
    UnitsforTemp,
    QAQC,
    as.character(Notes),
    HaulDate_Start,
    HaulDate_End,
    stringsAsFactors = F
  )

  names(dx) = c(
    "M_UID",
    "PID",
    "PROJECT",
    "LOCATION",
    "RECORDER_TYPE",
    "RECORDER_ID",
    "START_DATE",
    "END_DATE",
    "RECORDING_RATE",
    "PORT",
    "FILE_S",
    "M_LAT_DD",
    "M_LON_DD",
    "OBSERVED_DEPTH_METERS",
    "UNITS_FOR_TEMP",
    "QAQC",
    "NOTES",
    "HAUL_DATE_START",
    "HAUL_DATE_END"
  )


  if (!UID %in% pkg.env$T_UID) {
    AddTempRawdata(File,
                   UID,
                   PID,
                   LAT.DD,
                   LON.DD,
                   Observed_Depth,
                   HaulDate_Start,
                   HaulDate_End,
                   dx)
    #T_UID <<- c(T_UID, UID)
    assign('T_UID', c(pkg.env$T_UID, UID), pkg.env)
  }

  if (!UID %in% pkg.env$M_UID) {
    con3 = ROracle::dbConnect(drv,
                              username = oracle.snowcrab.user,
                              password = oracle.snowcrab.password,
                              dbname = oracle.snowcrab.server)
    if(pkg.env$testwrite){

      ROracle::dbWriteTable(
        con3,
        name = "SC_TEMPERATURE_META_TEST",
        append = T,
        row.names = F,
        value = dx
      )
    }else{
      ROracle::dbWriteTable(
        con3,
        name = "SC_TEMPERATURE_META",
        append = T,
        row.names = F,
        value = dx
      )
    }
    # M_UID <<- c(M_UID, UID)
    assign('M_UID',  c(pkg.env$M_UID, UID), pkg.env)
    ROracle::dbDisconnect(con3)
  }



}
#' @title  AddTempRawdata
#' @description  Internal function. Writes rawdata to database
#' @import ROracle DBI
#' @export
AddTempRawdata = function(fn = NULL,
                          UID = NULL,
                          ProjectId = NULL,
                          Lat = NULL,
                          Lon = NULL,
                          Observed_Depth = NULL,
                          HaulDate_Start = NULL,
                          HaulDate_End = NULL,
                          metadx = NULL) {

  Sys.setenv(TZ = "America/Halifax")
  Sys.setenv(ORA_SDTZ = "America/Halifax")
  print("Add Raw Data")
  drv = DBI::dbDriver("Oracle")
  con4 = ROracle::dbConnect(drv,
                            username = oracle.snowcrab.user,
                            password = oracle.snowcrab.password,
                            dbname = oracle.snowcrab.server)

  autof = F

  if (is.null(pkg.env$M_UID)) {
    res = ROracle::dbSendQuery(con4, "select DISTINCT M_UID from SC_TEMPERATURE_META")
    res = ROracle::fetch(res, n = -1)
    #M_UID <<- res$M_UID
    assign('M_UID', res$M_UID, pkg.env)

  }

  if (is.null(pkg.env$T_UID)) {
    res2 = ROracle::dbSendQuery(con4, "select DISTINCT T_UID from SC_TEMPERATURE_BASE")
    res2 = ROracle::fetch(res2, n = -1)
    #T_UID <<- res2$T_UID
    assign('T_UID', res2$T_UID, pkg.env)

  }
  ROracle::dbDisconnect(con4)

  if (!file.exists(fn)) {
    allfilelist = list.files(path = raw.dir,
                             full.names = T,
                             recursive = TRUE)

    allfilelist = allfilelist[grep(ProjectId, allfilelist)]
    read = 1
    sub = NULL
    subs = NULL
    sube = NULL
    if (length(allfilelist) > 1) {
      warning(paste(
        "More than one raw file name match for projectID: ",
        ProjectId,
        sep = ""
      ))
      rea = which(grepl(".csv", allfilelist))
      if (length(rea) > 0)
        read = rea
    }

    if (grepl("CTS", metadx$PROJECT)) {
      allfilelist = list.files(path = raw.dir,
                               full.names = T,
                               recursive = TRUE)

      ctsmat =  paste("CTS", ".", unlist(strsplit(metadx$PORT, "CTS"))[2], sep = "")
      allfilelist = allfilelist[grep(ctsmat, allfilelist)]
      sub = metadx$RECORDER_ID
      subs = metadx$HAUL_DATE_START
      sube = metadx$HAUL_DATE_END
    }
  }
  else{
    allfilelist = fn
    read = 1
    sub = NULL
  }
  if (length(allfilelist) < 1) {
    stop(paste(
      "There was no matching raw data file for projectID: ",
      ProjectId,
      sep = ""
    ))
  }
  else{
    sta_cont = NULL
    afdat = NULL
    madat = NULL

    if (!UID %in% pkg.env$T_UID) {

      sta_cont = standardize.temp(
        allfilelist[read],
        uid = UID,
        lat = Lat,
        lon = Lon,
        depth = Observed_Depth,
        subset = sub
      )

      if (is.na(HaulDate_Start))
        HaulDate_Start = NULL
      if (is.na(HaulDate_End))
        HaulDate_End = NULL

      if (!is.null(HaulDate_End)) {
        madat = trim_to_dates(sta_cont$data, HaulDate_Start,	HaulDate_End, TRUE)
        if (nrow(madat) == 0) {
          madat = auto_filter(
            da = sta_cont$data,
            lat = Lat,
            lon = Lon,
            uid = UID
          )
          sta_cont$QAQC = paste(sta_cont$QAQC, "Auto_Filtered", sep = " ")
        }
        else{
          sta_cont$QAQC = paste(sta_cont$QAQC, "UserDef_Informed", sep = " ")
        }
      }
      else{
        madat = auto_filter(
          da = sta_cont$data,
          lat = Lat,
          lon = Lon,
          uid = UID
        )
        sta_cont$QAQC = paste(sta_cont$QAQC, "Code_Informed", sep = " ")

      }
      indcomplete = NULL
      dxfin = NULL
      moredata = T
      while(moredata){
        dx = sta_cont$data

        rx = click.temp(dx, madat, indcomplete, UID)
        moredata = rx$again
        rx = rx$res
        indcomplete = c(indcomplete, which(dx$T_DATE==rx$start):which(dx$T_DATE==rx$end))
        sta_cont$QAQC = paste(sta_cont$QAQC, "Plot_Clicked_Filtered", sep = " ")
        dx = trim_to_dates(dx, rx$start,	rx$end, FALSE)
        dxfin = rbind(dxfin, dx)
      }
      sta_cont$deployments = nrow(rx)

      drv = DBI::dbDriver("Oracle")

      con5 = ROracle::dbConnect(drv,
                                username = oracle.snowcrab.user,
                                password = oracle.snowcrab.password,
                                dbname = oracle.snowcrab.server)

      if(pkg.env$testwrite){

        ROracle::dbWriteTable(
          con5,
          name = "SC_TEMPERATURE_BASE_TEST",
          append = T,
          row.names = F,
          value = dxfin
        )
      }else{
        ROracle::dbWriteTable(
          con5,
          name = "SC_TEMPERATURE_BASE",
          append = T,
          row.names = F,
          value = dxfin
        )
      }
      alluid = paste0(unique(dx$T_UID))
      # T_UID <<- c(T_UID, alluid)
      assign('T_UID', c(pkg.env$T_UID, alluid), pkg.env)
      ROracle::dbDisconnect(con5)
    }

    ##Will not get executed untill auto functions complete

    for (k in 1:sta_cont$deployments) {
      UID = alluid[k]

      if (!UID %in% pkg.env$M_UID) {
        ##Select which is of greater importance


        if (!is.null(metadx$PID))
          sta_cont$PID = metadx$PID

        if (!is.na(metadx$PROJECT))
          sta_cont$Project = metadx$PROJECT

        if (!is.na(metadx$LOCATION))
          sta_cont$Location = metadx$LOCATION

        if (!is.na(metadx$RECORDER_TYPE))
          sta_cont$RecorderType = metadx$RECORDER_TYPE

        if (!is.na(metadx$RECORDER_ID))
          sta_cont$RecorderID = metadx$RECORDER_ID

        if (!is.na(metadx$PORT)) {
          sta_cont$Port = metadx$PORT
        } else if (is.null(sta_cont$Port)) {
          sta_cont$Port = NA
        }

        if (!is.na(metadx$FILE_S)) {
          sta_cont$File = metadx$FILE_S
        } else if (is.null(sta_cont$File)) {
          sta_cont$File = NA
        }

        if (!is.na(metadx$M_LAT_DD))
          sta_cont$LAT.DD = metadx$M_LAT_DD

        if (!is.na(metadx$M_LON_DD))
          sta_cont$LON.DD = abs(as.numeric(metadx$M_LON_DD)) * -1

        if (!is.na(metadx$OBSERVED_DEPTH_METERS)) {
          sta_cont$Observed_Depth = metadx$OBSERVED_DEPTH_METERS
        } else if (is.null(sta_cont$Observed_Depth)) {
          sta_cont$Observed_Depth = NA
        }


        if (is.null(sta_cont$RecordingRate))
          sta_cont$RecordingRate = metadx$RECORDING_RATE
        if (is.null(sta_cont$UnitsforTemp))
          sta_cont$UnitsforTemp = metadx$UNITS_FOR_TEMP
        if (is.null(sta_cont$StartDate))
          sta_cont$StartDate = metadx$START_DATE
        if (is.null(sta_cont$EndDate))
          sta_cont$EndDate = metadx$END_DATE

        rqaqc = ""
        if (!is.na(metadx$QAQC))
          rqaqc = trimws(paste(rqaqc, metadx$QAQC, sep = " "))
        if (!is.na(sta_cont$QAQC))
          rqaqc = trimws(paste(rqaqc, sta_cont$QAQC, sep = " "))

        rnotes = ""

        if (!is.na(metadx$NOTES))
          rnotes = trimws(paste(rnotes, metadx$NOTES, sep = " "))
        if (!is.na(sta_cont$Notes))
          rnotes = trimws(paste(rnotes, sta_cont$Notes, sep = " "))

        AddTempMetadata(
          PID = sta_cont$PID,
          UID = UID,
          Project = sta_cont$Project,
          Location = sub(",,", "", sta_cont$Location),
          RecorderType = sta_cont$RecorderType,
          RecorderID = sta_cont$RecorderID,
          StartDate = sta_cont$StartDate,
          EndDate = sta_cont$EndDate,
          RecordingRate = sub(",,", "", sta_cont$RecordingRate),
          Port = sta_cont$Port,
          File = sub(",,", "", sta_cont$File),
          LAT.DD = sta_cont$LAT.DD,
          LON.DD = sta_cont$LON.DD,
          Observed_Depth = sta_cont$Observed_Depth,
          UnitsforTemp = sta_cont$UnitsforTemp,
          QAQC = rqaqc,
          Notes = sta_cont$Notes,
          HaulDate_Start = rx$start[k],
          HaulDate_End = rx$end[k]
        )
        # M_UID <<- c(M_UID, UID)
        assign('M_UID', c(pkg.env$M_UID, UID), pkg.env)
      }

    }
    #}#End while loop that checks for more bottom profiles in file
  }

}

#' @title read_merge_table
#' @description  Provided as an example for presentation
#' @import ROracle DBI
#' @return data from oracle table
read_merge_table = function() {
  drv = DBI::dbDriver("Oracle")

  con = ROracle::dbConnect(drv,
                           username = oracle.snowcrab.user,
                           password = oracle.snowcrab.password,
                           dbname = oracle.snowcrab.server)
  res <- ROracle::dbSendQuery(con, "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
  res <- ROracle::dbSendQuery(con, "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SSXFF'")
  res <- ROracle::dbSendQuery(con, "ALTER SESSION SET  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SSXFF TZR'")
  res <- ROracle::dbReadTable(con, "SC_TEMP_MERGE")

  return(res)
}


#' @title Verified.appendData
#' @description  Update maintables with temp table data. Only do this after manual verrification.
#' @import ROracle DBI tcltk
#' @return TRUE on success
#' @export
Verified.appendData = function() {
  drv <- DBI::dbDriver("Oracle")
  con8 <<-
    ROracle::dbConnect(drv,
                       username = oracle.snowcrab.user,
                       password = oracle.snowcrab.password,
                       dbname = oracle.snowcrab.server)

  wri <- tkmessageBox(
    title = "Continue?",
    message = "Answering yes will add all data from temperature test tables to temperature main tables. Are you sure?",
    icon = "info",
    type = "yesno"
  )
  if (grepl("yes", wri)) {
    warning("Writing to main tables")
    res = ROracle::dbSendQuery(con8, "INSERT INTO SC_TEMPERATURE_META SELECT * FROM SC_TEMPERATURE_META_TEST")
    warning("METADATA")
    ROracle::dbCommit(con8)
    res2 = ROracle::dbSendQuery(con8, "INSERT INTO SC_TEMPERATURE_BASE SELECT * FROM SC_TEMPERATURE_BASE_TEST")
    warning("BASEDATA")
    ROracle::dbCommit(con8)
    return(TRUE)
  }
  else{
    warning("You chose not to append  data")
    return(FALSE)
  }
}
