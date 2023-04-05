#' @title acoustic_file_handler
#' @description  Internal function. Acoustic data is structured in opposition to project structure so this seperate handler was created to process and write data to the oracle tables
#' @param fn The data to process
#' @param fn The place to write the plot
#' @import lubridate ggplot2 scales
#' @return True or False
#' @export
acoustic_file_handler = function(data, fn){

  #handle date formatting issues here first
  gf = guess_formats(data$date, c("mdy", "dmy", "ymd"))
  gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])
  if(gf == "%m-%d-%Y") data$date = mdy(data$date)
  if(gf == "%m/%d/%Y") data$date = mdy(data$date)
  if(gf == "%d-%m-%Y") data$date = dmy(data$date)
  if(gf == "%d/%m/%Y") data$date = dmy(data$date)
  if(gf == "%Y-%m-%d") data$date = ymd(data$date)

  data$date = data$date + hms("12:00:00")

  gf = guess_formats(data$deploy_date,  c("mdy", "dmy", "ymd", "mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
  gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])

  if(gf == "%m-%d-%Y") data$deploy_date = mdy(data$deploy_date)
  if(gf == "%d-%m-%Y") data$deploy_date = dmy(data$deploy_date)
  if(gf == "%Y-%m-%d") data$deploy_date = ymd(data$deploy_date)
  if(gf == "%d/%m/%Y") data$deploy_date = dmy(data$deploy_date)
  if(gf == "%m-%d-%Y %H:%M") data$deploy_date = mdy_hm(data$deploy_date)
  if(gf == "%m/%d/%Y %H:%M") data$deploy_date = mdy_hm(data$deploy_date)
  if(gf == "%m-%d-%Y %H:%M:%S") data$deploy_date = mdy_hms(data$deploy_date)
  if(gf == "%d-%m-%Y %H:%M:%S") data$deploy_date = dmy_hms(data$deploy_date)
  if(gf == "%d-%m-%Y %H:%M") data$deploy_date = dmy_hm(data$deploy_date)
  if(gf == "%Y-%m-%d %H:%M:%S") data$deploy_date = ymd_hms(data$deploy_date)
  if(gf == "%Y-%m-%d %H:%M") data$deploy_date = ymd_hm(data$deploy_date)
  if(gf == "%d/%m/%Y %H:%M") data$deploy_date = dmy_hm(data$deploy_date)
  if(gf == "%Y-%m-%d %H:%M:%OS") data$deploy_date = ymd_hms(data$deploy_date)
  #handle the case where not haul date provided
  if (!any(as.character(data$recover_date) != "") || all(is.na(data$recover_date))) {
    warning("No recover date provided")
  }
  else{

    gf = guess_formats(data$recover_date, c("mdy", "dmy", "ymd", "mdy_HM", "mdy_HMS","dmy_HMS", "dmy_HM", "ymd_HMS", "ymd_HM"))
    gf = names(sort(table(gf),decreasing=TRUE)[1:3][1])

    if(gf == "%m-%d-%Y") data$recover_date = mdy(data$recover_date)
    if(gf == "%d-%m-%Y") data$recover_date = dmy(data$recover_date)
    if(gf == "%Y-%m-%d") data$recover_date = ymd(data$recover_date)
    if(gf == "%d/%m/%Y") data$recover_date = dmy(data$recover_date)
    if(gf == "%m-%d-%Y %H:%M") data$recover_date = mdy_hm(data$recover_date)
    if(gf == "%m/%d/%Y %H:%M") data$recover_date = mdy_hm(data$recover_date)
    if(gf == "%m-%d-%Y %H:%M:%S") data$recover_date = mdy_hms(data$recover_date)
    if(gf == "%d-%m-%Y %H:%M:%S") data$recover_date = dmy_hms(data$recover_date)
    if(gf == "%d-%m-%Y %H:%M") data$recover_date = dmy_hm(data$recover_date)
    if(gf == "%Y-%m-%d %H:%M:%S") data$recover_date = ymd_hms(data$recover_date)
    if(gf == "%Y-%m-%d %H:%M") data$recover_date = ymd_hm(data$recover_date)
    if(gf == "%d/%m/%Y %H:%M") data$recover_date = dmy_hm(data$recover_date)
    if(gf == "%Y-%m-%d %H:%M:%OS") data$recover_date = ymd_hms(data$recover_date)
  }

  data = data[order(data$date), ]
  mmpid = split(data, data$station_name)
  for (k in 1:length(mmpid)) {
    mm_sub = mmpid[[k]]
    print(as.character(mm_sub$station_name[1]))
    #In case where deployment date is same as record date
    if(length(unique(mm_sub$deploy_date)) > 50){ #Pick some number of unreasonable deployments
      mm_sub$deploy_date = min(mm_sub$deploy_date)
    }
    mdpid = split(mm_sub, as.character(mm_sub$deploy_date))
    #loop through each deployment
    for (j in 1:length(mdpid)) {
      md_sub = mdpid[[j]]

      #Remove min and max entries and duplicates
      ind = which(
        as.character(md_sub$description) == "Temperature" |
          as.character(md_sub$description) == "Average temperature" |
          as.character(md_sub$description) == "ambient_mean_deg_c" |
          as.character(md_sub$description) == "ambient_deg_c"
      )
      md_sub = md_sub[ind, ]
      md_sub = md_sub[!duplicated(paste(as.character(md_sub$date), md_sub$receiver, sep = "-")), ]

      if (length(ind) == 0) {
        print("No 'Temperatrue', 'ambient_mean_deg_c' or 'Average temperature' feilds")
        next()
      }

      muid = paste(as.character(md_sub$station_name[1]),
                   as.character(length(which(
                     grepl(as.character(md_sub$station_name[1]), pkg.env$T_UID)
                   )) + 1),
                   sep = "-")
      loc = NA
      pro = NA
      if (grepl("CBS", as.character(md_sub$station_name[1]))) {
        loc = "Cabot Strait"
        pro = "OTN Cabot Strait"
      }
      if (grepl("HFX", as.character(md_sub$station_name[1]))) {
        loc = "South of Halifax"
        pro = "OTN Halifax"
      }
      if (grepl("HFX", as.character(md_sub$station_name[1]))) {
        loc = "South of Halifax"
        pro = "OTN Halifax"
      }
      if (grepl("hfx", as.character(md_sub$project[1]))) {
        loc = "South of Halifax"
        pro = "OTN Halifax"
      }
      if (grepl("cbs", as.character(md_sub$project[1]))) {
        loc = "Cabot Strait"
        pro = "OTN Cabot Strait"
      }
      if (grepl("sabmpa", as.character(md_sub$project[1]))) {
        loc = "SABMPA"
        pro = "SABMPA"
      }
      if (grepl("V2LSCF", as.character(md_sub$catalognumber[1]))) {
        loc = "SABMPA"
        pro = "V2LSCF"
      }

      if(as.character(mm_sub$station_name[1]) == "N117"){
        print(md_sub)
      }

      span = interval(md_sub$date[1], md_sub$date[2])

      rate = as.character(as.period(span, unit = "hours"))
      rate = unlist(strsplit(rate, " "))
      rate = gsub("[^0-9.-]", "", rate)
      rate = paste(rate[1], rate[2], rate[3], sep = ":")
      units = "Celsius"

      if (any(grepl("deg_f", md_sub$description))) {
        md_sub$data = ((as.numeric(as.character(
          md_sub$data
        )) - 32) * 5) / 9
        units = "Celsius"

      }
      if (any(grepl("deg_c", md_sub$description)))
        units = "Celsius"

      hauldate = md_sub$recover_date
      hauldate = max(hauldate)
      #handle the case where not haul date provided
      if(is.na(hauldate)) hauldate = max(md_sub$date)

      deploydate = md_sub$deploy_date
      deploydate = min(deploydate)
      #handle the case where not deploy date provided
      if(is.na(deploydate)) deploydate = min(md_sub$date)

      dx = data.frame(
        muid,
        as.character(md_sub$station_name[1]),
        pro,
        loc,
        unlist(strsplit(
          as.character(md_sub$receiver[1]), "-"
        ))[1],
        as.character(md_sub$rcv_serial_no[1]),
        deploydate,
        hauldate,
        rate,
        NA,
        fn,
        md_sub$dep_lat[1],
        md_sub$dep_long[1],
        NA,
        units,
        "Temp_Only One_Day_Buffer_Exclusion",
        NA,
        deploydate + days(1),
        hauldate - days(1),
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


      muiddx = rep(muid, nrow(md_sub))


      basedat = data.frame(
        muiddx,
        md_sub$date,
        md_sub$dep_lat,
        md_sub$dep_long,
        md_sub$data,
        stringsAsFactors =  F
      )

      names(basedat) = c("T_UID", "T_DATE", "LAT_DD", "LON_DD", "TEMP")
      basedat$DEPTH_M = NA

      if (dx$HAUL_DATE_END > dx$HAUL_DATE_START) {
        basedat = trim_to_dates(basedat, dx$HAUL_DATE_START, dx$HAUL_DATE_END, FALSE)

      }
      else{
        print("Not Enough DATA")
        next()
      }

      if (nrow(basedat) < 1) {
        print("Not Enough DATA")
        next()

      }

      if (!muid %in% pkg.env$M_UID) {
        drv = DBI::dbDriver("Oracle")
        con3 =  ROracle::dbConnect(
          drv,
          username = oracle.snowcrab.user,
          password = oracle.snowcrab.password,
          dbname = oracle.snowcrab.server
        )
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
        # M_UID <<- c(M_UID, muid)
        assign('M_UID', c(pkg.env$M_UID, muid), pkg.env)
        ROracle::dbDisconnect(con3)
      }

      if (!muid %in% pkg.env$T_UID) {
        drv <- DBI::dbDriver("Oracle")
        con5 =
          ROracle::dbConnect(
            drv,
            username = oracle.snowcrab.user,
            password = oracle.snowcrab.password,
            dbname = oracle.snowcrab.server
          )

        if(pkg.env$testwrite){

          ROracle::dbWriteTable(
            con5,
            name = "SC_TEMPERATURE_BASE_TEST",
            append = T,
            row.names = F,
            value = basedat
          )
        }else{
          ROracle::dbWriteTable(
            con5,
            name = "SC_TEMPERATURE_BASE",
            append = T,
            row.names = F,
            value = basedat
          )
        }
        alluid = paste0(unique(basedat$T_UID))
        #T_UID <<- c(T_UID, alluid)
        assign('T_UID', c(pkg.env$T_UID, alluid), pkg.env)
        ROracle::dbDisconnect(con5)
      }
      basedat$T_DATE = as.POSIXct(basedat$T_DATE)
      p = ggplot(data = basedat, main = muid, aes(x = T_DATE, y = TEMP)) + geom_line() + ggtitle(muid) + xlab("Date") + ylab("Temp °C")
      p = p + scale_x_datetime(labels = date_format("%Y-%m-%d"),
                               breaks = date_breaks("months")) + theme(axis.text.x = element_text(angle = 45))
      if (!dir.exists(file.path(dirname(fn), "ReceiverPlots")))
        dir.create(file.path(dirname(fn), "ReceiverPlots"))
      ggsave(
        p,
        width = 10,
        height = 10,
        filename = file.path(
          dirname(fn),
          "ReceiverPlots",
          paste(muid, ".pdf", sep = "")
        )
      )
    }
  }
}
