TOT_FIRMS = 12317; # I have these many from CRSP/COMPUSTAT in the period 1990-2020

# root_dir = "F:/";
root_dir = "\\\\LAPTOP-59MM2PIF\\Factiva\\";


# only run the below once in a while (say a month?). Stop downloading when you run this!
if(FALSE) {
  tmp = list.files(path = paste0(root_dir, "html"), pattern = "*.html", recursive = T, full.names = T);
  tmp = data.table(file = tmp);
  tmp[, file := str_replace_all(file, "/", "\\\\")];
  # mtime
  tmp[, mtime := file.mtime(file)];
  # save mtime as integer
  tmp[, mtime := as.integer(mtime)];
  # size_kb
  tmp[, size_kb := file.size(file)];
  tmp[, size_kb := round(size_kb/1e3)];
  # num_art
  tmp[, num_art := str_split(file, "_", simplify = T)[,8]];
  tmp[, num_art := str_replace_all(num_art, ".html", "") %>% as.integer];
  # start and end date
  tmp[, st_date := str_split(file, "_", simplify = T)[,3] %>% as.Date(format = "%Y%m%d")];
  tmp[, en_date := str_split(file, "_", simplify = T)[,5] %>% as.Date(format = "%Y%m%d")];
  # cusip
  tmp[, cusip := str_split(file, "\\\\", simplify = T)[,6]];
  # RECHECK
  tmp[, RECHECK := str_detect(file, "RECHECK") %>% as.integer];
  
  # For each file, keep the latest entry!
  setorder(tmp, file, -mtime);
  # update tmp
  tmp = tmp[, .SD[1], by = file];

  # cusip and start_date must be unique
  del = tmp[, .N, .(cusip, st_date)][N > 1, -"N"];
  setkey(tmp, cusip, st_date);
  if(nrow(del) > 0) {
    # delete html files with duplicate start or end dates
    tmp[del, unique(file)] %>% file.remove;
    tmp[del, unique(file)] %>% str_replace("\\.html", "\\.RData") %>% file.remove;
    # remove completed tag
    tmp[del, unique(cusip)] %>% paste0(root_dir, "html\\", "__", ., "__.COMPLETE") %>% file.remove;
    # update tmp
    tmp = tmp[!del];
  }
  
  # cusip and end_date must be unique
  del = tmp[, .N, .(cusip, en_date)][N > 1, -"N"];
  setkey(tmp, cusip, en_date);
  if(nrow(del) > 0) {
    # delete html files with duplicate start or end dates
    tmp[del, unique(file)] %>% file.remove;
    tmp[del, unique(file)] %>% str_replace("\\.html", "\\.RData") %>% file.remove;
    # remove completed tag
    tmp[del, unique(cusip)] %>% paste0(root_dir, "html\\", "__", ., "__.COMPLETE") %>% file.remove;
    # update tmp
    tmp = tmp[!del];
  }

  # save for future use!
  setkey(tmp, NULL);
  setorder(tmp, mtime);
  fwrite(tmp, paste0(root_dir, "html_history.csv"));
}


dt = fread(paste0(root_dir, "html_history.csv"));

# use the plot below to detect outliers
# dt[num_art > 0 & num_art <= 100, .(size_kb, num_art)] %>% plot
setorder(dt, mtime);
dt[, cum_num_art := cumsum(num_art)];
dt[, cum_size_kb := cumsum(size_kb)];


TAGS_EXTRACTED_files = list.files(paste0(root_dir, "tags"), pattern = "*.csv", recursive = F);
VERIFIED_files = list.files(paste0(root_dir, "csv"), pattern = "*.csv", recursive = F);
COMPLETED_files = list.files(paste0(root_dir, "html"), pattern = "*__.COMPLETE", recursive = F);
FAILED_files = list.files(paste0(root_dir, "html"), pattern = "*__.FAILED", recursive = F);

RECHECK_files = dt[RECHECK == 1, file];
RECHECK_dwnlds = dt[RECHECK == 1, sum(ceiling(num_art/100) - 1)]; # number of htmls to be downloaded. Note that I have subtracted 1 since the first 100 articles are already present in the RECHECK file

num_TAGS_EXTRACTED = TAGS_EXTRACTED_files %>% len;
num_VERIFIED = VERIFIED_files %>% len;
num_COMPLETED = COMPLETED_files %>% len;
num_FAILED = FAILED_files %>% len;
num_POSSIBLE = TOT_FIRMS - num_FAILED;

# convert integer mtime to POSIXct time
dt[, mtime := as.POSIXct(mtime, origin = origin, tz = Sys.timezone())];
attr(dt$mtime, "tzone") = Sys.timezone();
dt[, st_date := as.Date(st_date)];
dt[, en_date := as.Date(en_date)];

tot_size = dt[, round(sum(size_kb)/1e6, 3)];

tot_days = difftime(max(dt$mtime), min(dt$mtime), units = "days") %>% as.numeric %>% round(1);

curr_time = strptime(date(), format = "%a %b %d %H:%M:%S %Y");
curr_time = as.POSIXct(curr_time, tz = Sys.timezone());

# files downloaded in last 24 hours
time_start = curr_time - 3600*24;
time_end   = curr_time - 3600*0;

# time_start = strptime("20210301 22:00:00", format = "%Y%m%d %H:%M:%S");
# time_end = curr_time;

n_days = difftime(time_end, time_start, units = "days") %>% as.numeric;

n_html = round(dt[mtime %between% c(time_start, time_end), .N] / n_days)
n_art = round(dt[mtime %between% c(time_start, time_end), sum(num_art)] / n_days)
n_size = round(dt[mtime %between% c(time_start, time_end), sum(size_kb)] / n_days)

n_art_tot = dt[, sum(num_art)]

n_art_per_comp_rough = dt[cusip %in% substr(COMPLETED_files, 3, 10), sum(num_art)] / num_COMPLETED;
n_comp_per_day_rough = n_art/n_art_per_comp_rough;

more_days_cusip = round( (num_POSSIBLE - num_COMPLETED) / n_comp_per_day_rough, 1);

a = options("warn")$warn;
options(warn = -1);
txt = system(paste0("fsutil volume diskfree ", root_dir), intern = T);
options(warn = a);
free_disk_space = txt[len(txt)] %>% stringr::str_extract("\\d+\\.\\d+\\s[T|G|M]B");
total_disk_space = txt[len(txt)-1] %>% stringr::str_extract("\\d+\\.\\d+\\s[T|G|M]B");

cat('* Current time: ', as.character(curr_time), '\n', '* Getting roughly ', n_html, ' html file downloads per day (based on last ', round(n_days, 2), ' days of data download)', ' containing roughly ', n_art, ' news articles.', '\n', '* As of now, ', round(n_art_tot/1e6, 3), ' Mn artciles (as ', dt[,.N], ' html files) totalling disk size of ', tot_size,' GB have been downloaded. This includes duplicates and RECHECK html files.', '\n', '* A total of ', num_COMPLETED, ' cusips (', num_VERIFIED, ' verified and converted to csv and tags/text extracted for ', num_TAGS_EXTRACTED, ' files out of those) have been fully downloaded while ', num_FAILED, ' have failed in ', tot_days, ' days of download activity.', '\n', '* If we consider total firms to be ', num_POSSIBLE, ' then it should take roughly ', more_days_cusip, ' more days and ', round((num_POSSIBLE/num_COMPLETED)*tot_size, 1), ' GB of disk space for download to finish.', '\n', '* Out of ', dt[,.N], ' html files ', len(RECHECK_files), ' require manual downlaods as they have > 100 artciles. Overall ', RECHECK_dwnlds, ' html file needs to be downloaded!', '\n', '* Currently running at ', free_disk_space, ' of free disk space out of ', total_disk_space, ' of total disk space.', '\n', sep = '');


# detailed plot
setorder(dt, mtime);
tmp = dt[, .(num_art = sum(num_art)), by = date(mtime)][order(date)];
# all dates
tmp = merge(tmp,
            data.table(date = seq(first(tmp$date), last(tmp$date), by = 1)),
            by = "date", all.y = T);
tmp[, keep := ifelse(is.na(num_art), FALSE, TRUE)];
tmp[is.na(num_art), num_art := 0];
tmp[, date := paste0(date, " 00:00:00 IST") %>% as.POSIXct];
tmp[, num_art_perc := 100*num_art / sum(num_art, na.rm = T)];
tmp[, num_art_perc := round(num_art_perc, 2)];
tmp[, week := week(date)];
tmp[, avg_num_art := mean(num_art, na.rm = T), week];
tmp[, week := NULL];
tmp = tmp[keep == TRUE];
tmp[, keep := NULL];

y_rng = c(dt$cum_num_art[1], last(dt$cum_num_art)) / 1e6;
y_max = 0.5*y_rng[2];
tmp[, del := num_art_perc*y_max / max(tmp$num_art_perc)];
y_st = y_rng[1] + 0.25*y_rng[2];
y_en = y_st + tmp$del;

org_mar = par("mar");
par(mar = c(5,5,2,5)) # for extra margin on right y-axis
plot(dt$mtime, dt$cum_num_art/1e6, type = "l", lwd = 2, col = "blue", xlab = "Date", ylab = "Cumulative Articles (Mn)", main = "Download timeline", xlim = c(tmp$date[1], last(tmp$date) + 24*3600 - 1),
  panel.first = rect(xleft = tmp$date, ybottom = y_st, xright = tmp$date + 24*3600, ytop = y_en, col = RColorBrewer::brewer.pal(7, "BrBG"), border = NA));
# text(x = tmp$date + 12*3600, y = y_en + 1, labels = sprintf("%0.1f %%", tmp$num_art_perc));
text(x = tmp$date + 12*3600, y = y_en + 1, labels = sprintf("%0.1f K", tmp$num_art/1e3), cex = 0.50, srt = 90);
text(x = tmp$date + 12*3600, y = y_st - 1, labels = weekdays(tmp$date, abbreviate = T), cex = 0.50, srt = 90);
# alert mails
abline(v = as.POSIXct("2021-03-24 11:18:00"), col = "gray80", lwd = 2);
abline(v = as.POSIXct("2021-03-25 11:19:00"), col = "gray80", lwd = 2);
abline(h = 0, col = "red", lwd = 1, lty = 2);
par(new = TRUE);
plot(tmp$date, tmp$avg_num_art/1e3, type = "b", lwd = 2, col = "red", xlab = NA, ylab = NA, axes = F);
axis(4);
mtext(side = 4, line = 3, text = "Weekly Avg. Downloads (in 000's)");
par(mar = org_mar);

# dt[, .(mtime, cum_size_kb/1e6)] %>% plot(type = "l", xlab = "time", ylab = "Cumulative Size (GB)", main = "Storage Used timeline");


# histogram
# dt[, day_hour := strftime(dt$mtime, format = "%Y-%m-%d %H:00:00") %>% as.POSIXct];
# tmp = dt[, .(num_art = sum(num_art)), day_hour][order(day_hour)];
# dt[, day_hour := NULL];
# barplot(tmp$num_art, names.arg = tmp$day_hour);


