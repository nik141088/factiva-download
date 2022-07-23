setwd("C:/Users/nikhi/Dropbox/research/news_edgar_and_twitter/extraction_code/extract_factiva_news_articles/");

STOP_ON_ERROR = F;
EXTRA_SLEEP = F;

# diff directories
# root_dir = "F:/";
root_dir = "\\\\LAPTOP-59MM2PIF\\Factiva\\";
html_dir = paste0(root_dir, "html/");
csv_dir = paste0(root_dir, "csv/");
df_fails_dir = paste0(root_dir, "df_fails/");

REVERSE = fread(paste0(root_dir, "REVERSE")) %>% as.integer;

sample = fread(paste0(root_dir, "essentials/factiva_sample_firms.csv"), sep = "_");

# forever failed file
forever_failed_file = paste0(root_dir, "failed_files_forever.RData");

start_date = as.Date("1990-01-01");
end_date = as.Date("2020-12-31");
TOT_DAYS = (difftime(end_date, start_date, units = "days") + 1) %>% as.integer;

cusips = sample$cusip;

# to keep track of failed files
failed_files = data.table(file = character(), reason = character(), cusip = character(), tic = character(),
                          num_art = integer(), num_html = integer(), checked_at = POSIXct());

# save error files for future reference
if(file.exists(forever_failed_file)) {
  failed_files_forever = readRDS(forever_failed_file);
} else {
  failed_files_forever = data.table(file = character(), reason = character(), cusip = character(), tic = character(),
                            num_art = integer(), num_html = integer(), checked_at = POSIXct());
  saveRDS(failed_files_forever, forever_failed_file, compress = F);
}


# only proceed to save csv if html_error is FALSE
html_error = F;

for(z in 1:len(cusips)) {
  
  # sleep for 100 ms. This is to reduce heating!
  Sys.sleep(0.1 * ifelse(EXTRA_SLEEP, 1, 0));
  
  if(REVERSE == 1) {
    cnt = len(cusips) - z + 1;
  } else {
    cnt = z;
  }
  
  c = cusips[cnt]; # current cusip
  html_error = F; # to begin with
  
  # different file types
  csv_file = paste0(root_dir, "csv/", c, ".csv");
  csv_failed = paste0(root_dir, "csv__", c, "__.FAILED");
  completed_file = paste0(root_dir, "html/", "__", c, "__.COMPLETE")
  hole_file = paste0(root_dir, "html/", "__", c, "__.HOLES");
  cusip_html_dir = paste0(root_dir, "html/", c, "/");
  
  
  # check if a csv file already exists
  if(file.exists(csv_file)) {
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " csv already exists!", "\n", sep = "");
    # go to next
    next;
  }
  
  # check whether the csv file has failed. In future we need to delete all such files and re-run this!
  if(file.exists(csv_failed)) {
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " csv already FAILED!", "\n", sep = "");
    # go to next
    next;
  }

  # If completed_file doesn't exists then download is not yet complete!
  if(!file.exists(completed_file)) {
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " all htmls not yet downloaded!", "\n", sep = "");
    # completed file doesn't exists. Download not yet finish. Go to next.
    next;
  }
  
  # If hole_file exists then we should let python to first download the missing files
  if(file.exists(hole_file)) {
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " - hole file exists. Skipping this cusip!", "\n", sep = "");
    # hole file exists. Download not yet finish. Go to next.
    next;
  }
  
  all_files = list.files(cusip_html_dir, pattern = "*.html", full.names = T);
  if(len(all_files) == 0) {
    # remove completed file
    if(file.exists(completed_file)) {
      cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " Removing completed file!", "\n", sep = "");
      file.remove(completed_file);
    }
    next;
  }
  
  N = len(all_files);
  
  tmp = stringr::str_split(all_files, "_", simplify = T) %>% as.data.table;
  tmp[, RECHECK := FALSE];
  if(sum(c("V9", "V10", "V11", "V12") %in% colnames(tmp)) > 0) {
    # this will happen only with __RECHECK__ files
    tmp[V10 != "", RECHECK := TRUE];
    tmp[, `:=`(V9 = NULL, V10 = NULL, V11 = NULL, V12 = NULL)]
  }
  
  tmp = cbind(all_files, tmp, stringr::str_split(tmp$V1, "/", simplify = T) %>% as.data.table);
  names(tmp) = paste0("V", 1:ncol(tmp));
  tmp = tmp[, .(file = V1, start_date = V4, end_date = V6, num_art = V9, cusip = V12, tic = V13, RECHECK = V10)];
  tmp[, num_art := as.integer(substr(num_art, 1, 5))];
  
  tmp[, start_date := as.Date(start_date, "%Y%m%d")];
  tmp[, end_date := as.Date(end_date, "%Y%m%d")];
  
  # Make sure that there are no overalapping dates. This will most likely happen when multiple pocesses write on the same file
  setorder(tmp, start_date);
  tmp[, next_start_date := LEAD(start_date, 1)];
  del_idx = c();
  next_cnt = 1; # to begin with
  j = 1;
  while(j + next_cnt <= N) {
    # is the next file start_date bigger than curr file end_date?
    if(tmp$start_date[j + next_cnt] > tmp$end_date[j]) {
      # this is expected behavior. Increase j by `next_cnt` and not `1`. In the case an overlapping file is found, j needs to be increased by the number of such files found!
      j = j + next_cnt;
      next_cnt = 1;
    } else {
      cat("Error processing html file: ", tmp$file[j + next_cnt], " for cusip: ", c, " and i: ", j, " out of ", nrow(tmp),
          ". The file overlaps with an earlier file: ", tmp$file[j], ". Marking this for deletion.", "\n", sep = "");
      if(STOP_ON_ERROR) {
        stop("1: some error occured!");
      }
      # store the failed html file
      ff.file = tmp$file[j + next_cnt];
      ff.reason = "overlap";
      ff.cusip = c;
      ff.tic = tmp$tic[1];
      ff.num_art = NA;
      ff.num_html = nrow(tmp);
      ff.checked_at = Sys.time();
      failed_files = rbind(failed_files, list(ff.file, ff.reason, ff.cusip, ff.tic, ff.num_art, ff.num_html, ff.checked_at));
      # flag the issue
      html_error = T;

      # it makes more sense to delete these files here itself
      file.remove(ff.file);
      
      # this should not happen. It also means that `j+1` file has overlapping dates and must be marked for deletion
      del_idx = c(del_idx, j + next_cnt);
      # For the next search we should compare the start date of next to next file with current file end date
      next_cnt = next_cnt + 1;
    }
  }
  
  # remove the files with indices del_idx from tmp and update N
  if(len(del_idx) > 0) {
    tmp = tmp[-del_idx];
    tmp[, next_start_date := NULL];
    N = nrow(tmp);
    # remove the csv file
    if(file.exists(csv_file)) {
      cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " Removing csv file!", "\n", sep = "");
      file.remove(csv_file);
    }
  }
  
  # make sure that the dates in tmp span the entire 1990-2020 duration.
  setorder(tmp, start_date);
  all_dates = c();
  for(j in 1:N) {
    all_dates = c(all_dates, seq(tmp$start_date[j], tmp$end_date[j], by = 1));
  }
  # at this point the date must be sorted without duplicates
  all_dates = unique(all_dates);
  all_dates = as.Date(all_dates);
  if( (min(all_dates) != start_date) | (max(all_dates) != end_date) | len(all_dates) != TOT_DAYS | (len(which(duplicated(all_dates))) != 0)) {
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " All dates not available! Possible holes!", "\n", sep = "");
    # create hole file: comment the below. Only let python check_holes() function decide whether there are holes or not!
    # cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " Creating hole file!", "\n", sep = "");
    # file.create(hole_file);
    next;
  }

  # placeholder for RData files to be used for html conversion
  tmp[, tmp_dt_storage := stringr::str_replace(file, "html$", "RData")];
  
  # delete all RData files that do not have a corresponding html file
  stored_files = list.files(cusip_html_dir, pattern = "*.RData", full.names = T);
  failed_files_rdata = stringr::str_replace(failed_files$file, "html$", "RData");
  delete_files = setdiff(stored_files, tmp$tmp_dt_storage);
  delete_files = union(delete_files, failed_files_rdata);
  file.remove(delete_files);
  remove(stored_files, failed_files_rdata, delete_files);
  
  # atleast one file with more than 100 files in html. Need to go over this file again after fixing it!
  if(tmp[num_art > 100, .N] > 0) {
    # set html_error flag to TRUE
    html_error = TRUE;
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " some htmls have more than 100 articles. Fix it!", "\n", sep = "");
    # If all non-RECHECK html files have a corresponding RData file, then there is nothing to be done
    if(tmp[RECHECK == F, file.exists(tmp_dt_storage) %>% prod] == 1) {
      next;
    }
  }
  

  # main loop
  for(i in 1:N) {
    
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " and i: ", i, " out of ", nrow(tmp), "\n", sep = "");
    
    # To gain speed; read the stored file at the end and in the condition that all previous files should be error free!
    if(file.exists(tmp$tmp_dt_storage[i])) {
      # html already processed! Proceed further
      cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " and i: ", i, " RData file exists! Skipping it now!", "\n", sep = "");
      next;
    }
    
    if(tmp$num_art[i] == 0) {
      # skip this and store 0 for this
      dt = data.table(cusip = tmp$cusip[i], tic = tmp$tic[i], start_date = tmp$start_date[i], end_date = tmp$end_date[i], doc_id = NA_character_, tag = NA_character_, content = NA_character_, content_len = 0);
      # save dt in a temporary RData file in the html directory so that the conversion is not repeated in each iteration
      saveRDS(dt, tmp$tmp_dt_storage[i]);
      next;
    }
    
    if(tmp$num_art[i] > 100) {
      # skip this
      html_error = TRUE;
      next;
    }
    
    df = TRY_CATCH(expression = XML::readHTMLTable(tmp$file[i]), print.attempts = F, max.attempts = 1, ret_val = NULL);
    
    # sleep for 500 ms. This is to reduce heating!
    Sys.sleep(0.5 * ifelse(EXTRA_SLEEP, 1, 0));
    
    if(is.null(df)) {
      cat("Error processing html file: ", tmp$file[i], " for cusip: ", c, " and i: ", i, " out of ", nrow(tmp),
          ". df is NULL.", "\n", sep = "");
      if(STOP_ON_ERROR) {
        stop("2: some error occured!");
      }
      # rather than breaking out of loop, try to find if there are other html files that might fail for this cusip
      # store the failed html file
      ff.file = tmp$file[i];
      ff.reason = "df_NULL";
      ff.cusip = c;
      ff.tic = tmp$tic[i];
      ff.num_art = NA;
      ff.num_html = nrow(tmp);
      ff.checked_at = Sys.time();
      failed_files = rbind(failed_files, list(ff.file, ff.reason, ff.cusip, ff.tic, ff.num_art, ff.num_html, ff.checked_at));
      # flag the issue
      html_error = T;
      # go to next html file for the same cusip, but mark it fail
      next;
    }
    
    n = len(df);
    # df is a list of tables. df[[1]] is the page headline, df[[n-1]] is the summary tables, df[[n]] is factiva copyright. df[[2]] to df[[n-2]] are the articles.
    # the min. size of df must be 4: one for page headline, one for summary table, one for copyright and atleast one for articles
    if(n < 4) {
      cat("Error processing html file: ", tmp$file[i], " for cusip: ", c, " and i: ", i, " out of ", nrow(tmp),
          ". df has only ", n, " elements. Must have atleast 4.", "\n", sep = "");
      if(STOP_ON_ERROR) {
        stop("3: some error occured!");
      }
      # rather than breaking out of loop, try to find if there are other html files that might fail for this cusip
      # store the failed html file
      ff.file = tmp$file[i];
      ff.reason = "df_too_few_elems";
      ff.cusip = c;
      ff.tic = tmp$tic[i];
      ff.num_art = n;
      ff.num_html = nrow(tmp);
      ff.checked_at = Sys.time();
      failed_files = rbind(failed_files, list(ff.file, ff.reason, ff.cusip, ff.tic, ff.num_art, ff.num_html, ff.checked_at));
      # flag the issue
      html_error = T;
      # go to next html file for the same cusip, but mark it fail
      next;
    }
    
    # the below must be in reverse order
    df[[n]]   = NULL; # factiva copyright
    df[[n-1]] = NULL; # summary of downloaded html file
    df[[1]]   = NULL; # `Dow Jones Factiva` heading
    
    n = len(df);
    names(df) = NULL;
    
    
    # If some members of df, i.e. articles, have more than 2 or less than 2 columns, then save those in a separate folder with the porper cusip, tic and date information. These can be checked later. Also reduce `n` by the number of df entries having wrong number of columns
    # all `n` members of df must have two columns
    idx = which(sapply(df, ncol) != 2); # indices of df having issues
    tmp_fname = stringr::str_split(tmp$file[i], "/", simplify = T)[3];
    tmp_fname = stringr::str_replace(tmp_fname, ".html$", "");
    if(len(idx) > 0) {
      cat("Error processing html file: ", tmp$file[i], " for cusip: ", c, " and i: ", i, " out of ", nrow(tmp), ". ", len(idx),  " members of df (i.e. articles) have less (or more) than the required two columns. Storing the failed articles in directory: ", df_fails_dir, " and moving on with parsing the remaining articles.", "\n", sep = "");
      if(STOP_ON_ERROR) {
        stop("4: some error occured!");
      }
      for(idx_i in idx) {
        df_fname = paste0(df_fails_dir, c, "----SEP----", tmp_fname, "----SEP----", "df_failed_", idx_i, ".RData");
        saveRDS(df[[idx_i]], df_fname);
      }
      # remove all the failed df entries from df
      df = df[-idx];
      # reduce n and tmp$num_art
      n = len(df);
      tmp$num_art[i] = tmp$num_art[i] - len(idx);
      # If reducing `n` makes it 0 then we need to stop further processing
      if(n == 0) {
        # skip this and store 0 for this
        dt = data.table(cusip = tmp$cusip[i], tic = tmp$tic[i], start_date = tmp$start_date[i], end_date = tmp$end_date[i], doc_id = NA_character_, tag = NA_character_, content = NA_character_, content_len = 0);
        # save dt in a temporary RData file in the html directory so that the conversion is not repeated in each iteration
        saveRDS(dt, tmp$tmp_dt_storage[i]);
        next;
      }
    }
    
    
    if(tmp$num_art[i] <= 100 & (tmp$num_art[i] != n)) {
      cat("Error processing html file: ", tmp$file[i], " for cusip: ", c, " and i: ", i, " out of ", nrow(tmp),
          ". No. of articles in file: ", n, " do not match with num_art: ", tmp$num_art[i], ".\n", sep = "");
      if(STOP_ON_ERROR) {
        stop("5: some error occured!");
      }
      # COND.1: If the failure has happened atleast 3 times before then save the file. Most likely number of articles are different and the download looks fine!
      # COND.2: Also make sure last three num_art saved in failed_files_forever are same. This will make sure that we are not facing issues with each download fetching diff. number of articles.
      if( (failed_files_forever[file == tmp$file[i] & reason == "df_num_art_mismatch", .N] >= 3) &
          (failed_files_forever[file == tmp$file[i] & reason == "df_num_art_mismatch"][order(-checked_at)][1:3, unique(num_art)] %>% len == 1)) {
        # do-nothing!  
        cat("Since the file has failed multiple (>= 3) times before, I am letting it pass.", "\n", sep = "");
      } else {
        # Let the above pass if the difference is only 1 and the number of articles downloaded are >= 30
        if(tmp$num_art[i] >= 30 & (tmp$num_art[i] - n <= 2)) {
          # do nothing
          cat("Since the difference is small, I am letting it pass.", "\n", sep = "");
        } else {
          # rather than breaking out of loop, try to find if there are other html files that might fail for this cusip
          # store the failed html file
          ff.file = tmp$file[i];
          ff.reason = "df_num_art_mismatch";
          ff.cusip = c;
          ff.tic = tmp$tic[i];
          ff.num_art = n;
          ff.num_html = nrow(tmp);
          ff.checked_at = Sys.time();
          failed_files = rbind(failed_files, list(ff.file, ff.reason, ff.cusip, ff.tic, ff.num_art, ff.num_html, ff.checked_at));
          # flag the issue
          html_error = T;
          # go to next html file for the same cusip, but mark it fail
          next;
        }
      }
    }
    
    # Each df[[i]] is a L[i]x2 table and the last row contains the document id for df[[i]]
    L = sapply(df, nrow);
    
    doc_id = sapply(1:n, function(i) df[[i]][L[i], 2]);
    doc_id = stringr::str_split(doc_id, " ", simplify = T)[,2];
    
    # rbind all dfs
    dt = lapply(1:n, function(d) cbind(tmp$cusip[i], tmp$tic[i], tmp$start_date[i], tmp$end_date[i], doc_id[d], df[[d]])) %>% rbindlist;
    names(dt) = c("cusip", "tic", "start_date", "end_date", "doc_id", "tag", "content");
    dt[, content_len := nchar(content)];
    
    # save dt in a temporary RData file in the html directory so that the conversion is not repeated in each iteration
    saveRDS(dt, tmp$tmp_dt_storage[i]);

  }
  # end of main loop
  i = 0;
  
  
  if(html_error == T) {
    # mark this cusip as failed
    cat("cnt: ", cnt, " / ", nrow(sample), " cusip: ", c, " Creating FAILED file!", "\n", sep = "");
    file.create(csv_failed);
    # go to next cusip
    next;
  }
  
  # load stored files in final
  stored_files = list.files(cusip_html_dir, pattern = "*.RData", full.names = T);
  final = lapply(stored_files, readRDS) %>% rbindlist;
  setorder(final, start_date);
  
  # modify the content a little bit for these tags
  tags = c("CO", "IN", "IPC", "IPD", "NS", "RE");
  final[tag %in% tags, content := stringr::str_replace_all(content, " \\| ", "----NEXT----")];
  final[tag %in% tags, content_len := nchar(content)];
  
  # save it
  fwrite(final, csv_file);
  # sleep for 500 ms. This is to reduce heating!
  Sys.sleep(0.5 * ifelse(EXTRA_SLEEP, 1, 0));
  # remove all tmp files
  if(len(stored_files) > 0) {
    file.remove(stored_files);
  }

}


# keep a copy for future diagnosis
failed_files_forever = rbind(failed_files_forever, failed_files);
saveRDS(failed_files_forever, forever_failed_file, compress = F);

# now delete all failed html files
file.remove(failed_files$file);
# need to remove the above html files from history file as well
dt = fread(paste0(root_dir, "html_history.csv"));
del = dt[file %in% str_replace_all(failed_files$file, "/", "\\\\"), .(file)];
setkey(dt, file);
dt = dt[!del];
fwrite(dt, paste0(root_dir, "html_history.csv"));

remove(failed_files);
# also delete .FAILED files
list.files(root_dir, "csv__[A-Z,0-9]{8}__.FAILED", full.names = T) %>% file.remove;












