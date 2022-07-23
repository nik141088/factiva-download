# code to put all factiva CSVs in a DB
source("~/.RProfile")
library(DBI)

DEVICE = NULL;
serial_num = system("wmic bios get serialnumber", intern = T);
serial_num = serial_num[2] %>% strsplit("  ") %>% {.[[1]][1]};

# only execute this if machine is Acer-lappy
if(serial_num != "NHQ3HSI007926041133400") {
  stop("This machine is not Acer-lappy. Can't proceed!")
}

root_dir = "\\\\LAPTOP-59MM2PIF\\Factiva\\";
csv_dir = paste0(root_dir, "csv/");
setwd(root_dir)

# this file contains high level identifty data
sample = fread(paste0(root_dir, "essentials/factiva_sample_firms.csv"), sep = "_");
sample[, cusip := str_pad(cusip, 8, "left", "0")];

# all CSV files
all_files = list.files(csv_dir, "*.csv", full.names = T);

# DB file
DB_file = "F:/factiva_DB/factiva.db"

# function to fetch from DB
fetch_from_DB = function(con = DBCON, query, n = -1) {
  res = dbSendQuery(con, query)
  data = dbFetch(res, n)
  setDT(data)
  dbClearResult(res)
  # dbDisconnect(con)
  return(data)
}

# you can table info and column using: PRAGMA TABLE_INFO('table_name')
# you can check for all indices using: PRAGMA INDEX_LIST('table_name')

# Read K files at a time and then write to DB
K = len(all_files);
N = ceiling(len(all_files)/ K) %>% as.integer;
N_st = seq(1, len(all_files), by = K)
N_en = seq(K, len(all_files), by = K)
if(is.na(N_en[N])) {
  N_en = c(N_en, len(all_files))
}

for(i in 1:N) {
  
  idx = N_st[i]:N_en[i];
  
  cusips = str_split(all_files[idx], "/", simplify = T)[,2] %>% str_replace_all("\\.csv", "");
  
  cat(paste0("Into factiva_write_to_db.R for files: ", paste0(cusips, collapse = "; ")), "Current iteration: ", i, "/", N, "\n");

  done_files = paste0("F:/factiva_DB/done/", cusips, ".done_file")
  
  # check done file. If exists go to next
  done_idx = which(file.exists(done_files));
  if(TRUE & len(done_idx) > 0) {
    cat(paste0("Done files for these cusips exists. Moving to others!: ", paste0(cusips[done_idx], collapse = "; "), "\n"));
    idx = idx[-done_idx]
    cusips = cusips[-done_idx]
    done_files = done_files[-done_idx]
  }
  
  # is there any file left?
  if(len(idx) == 0) {
    cat("Moving to next idx.", "\n");
    next;
  }
  
  # check list of cusips from DB. If exists go to next
  # the below takes a lot of time. Keep it FALSE. After all data is pushed to DB,
  # run it once with TRUE to push any cusip that might have been missed! Thus run
  # it with K = len(all_files)
  if(FALSE & file.exists(DB_file)) {
    DBCON = dbConnect(RSQLite::SQLite(), DB_file);
    dt = fetch_from_DB(DBCON, "select distinct(cusip) from factiva")
    dbDisconnect(DBCON);
    
    db_idx = which(cusips %in% dt$cusip)
    if(len(db_idx) > 0) {
      # some cusips are present in DB
      cat(paste0("Creating done file for cusips: ", cusips[db_idx], "\n"));
      file.create(paste0("F:/factiva_DB/done/", cusips[db_idx], ".done_file"))
      idx = idx[-db_idx]
      cusips = cusips[-db_idx]
      done_files = done_files[-db_idx]
    }
    
    # is there any file left?
    if(len(idx) == 0) {
      cat("Moving to next idx.", "\n");
      next;
    }
    
  }
  
  class_type = rep("character", 8);
  class_type[3:4] = "Date";

  # read csv file and clean it
  dt = lapply(all_files[idx], fread, colClasses = class_type) %>% rbindlist;
  dt[, content_len := as.integer(content_len)];
  Sys.sleep(1);
  dt[, cusip := str_pad(cusip, 8, "left", "0")];
  # remove entries with NA content
  dt = dt[!is.na(content) & content != ""];
  # discard the article test; only keep lead para
  dt = dt[tag != "TD"];
  # make sure there is some data in dt
  if(nrow(dt) == 0) {
    next;
  }
  
  # separate tag content
  dt[, num_tags := stringr::str_count(dt$content, "----NEXT----") + 1];
  final = data.table(doc_id = rep(dt$doc_id, dt$num_tags),
                     tag = rep(dt$tag, dt$num_tags),
                     tag_content = stringr::str_split(dt$content, "----NEXT----") %>% unlist);
  final = unique(final);
  # add start and end dates from dt. Merge using (doc_id, tag) pair
  final = merge(final,
                dt[, .(doc_id, tag, cusip, tic, start_date, end_date)],
                by = c("doc_id", "tag"),
                all.x = T, allow.cartesian = T);
  # add conm
  MRG(final, sample[, .(cusip, conm = comp_conm)], by = "cusip");
  setorder(final, cusip, doc_id, tag);
  
  # the below are multi-tags for a document, i.e. each document can have many such tags
  multi_content_tags = c("CO", "IN", "NS", "RE");
  final[tag %in% multi_content_tags,
        factiva_code := stringr::str_split(tag_content, "\\s+:\\s+", simplify = T)[,1]];
  final[tag %in% multi_content_tags,
        name := stringr::str_split(tag_content, "\\s+:\\s+", simplify = T)[,2]];
  final[, tag_content := coalesce(name, tag_content)];
  final[, name := NULL];
  # remove entries with NA content
  final = final[!is.na(tag_content) & tag_content != ""];
  # at this point keep a unique copy wrt cusip, doc_id, tag, tag_content
  setorder(final, cusip, doc_id, tag, tag_content)
  final = final[, .SD[1], by = .(cusip, doc_id, tag, tag_content)]
  
  # now only focus on tags that are unique for a doc and can be used for filtering
  single_content_tags = c("PD", "SN", "WC", "LA");
  tmp = final[tag %in% single_content_tags, .(cusip, doc_id, tag, tag_content)];
  # at this point keep a unique copy wrt cusip, doc_id, tag
  setorder(tmp, cusip, doc_id, tag)
  tmp = tmp[, .SD[1], by = .(cusip, doc_id, tag)]
  tmp = dcast(tmp, cusip + doc_id ~ tag, value.var = "tag_content");
  # transform PD and WC
  tmp[, PD := as.Date(PD, format = "%d %B %Y")];
  tmp[, WC := stringr::str_extract(WC, "\\d+\\b") %>% as.integer];
  
  # add back to final
  MRG(final, tmp, c("cusip", "doc_id"));
  
  # remove rows of single_content_tags and document_num tag: AN
  final = final[!(tag %in% c(single_content_tags, "AN"))];
  
  # change names
  setnames(final, c("LA", "PD", "SN", "WC"), c("language", "pub_date", "source", "word_count"))
  
  # sanity check
  final = final[pub_date >= start_date & pub_date <= end_date];
  
  # drop start and end dates
  final[, `:=`(start_date = NULL, end_date = NULL)];
  
  # set NA factiva_tag to ""
  final[is.na(factiva_code), factiva_code := ""];
  
  # remove tmp and dt
  remove(tmp, dt);
  
  Sys.sleep(1);
  

  
  # create DB if it do not exists
  if(!file.exists(DB_file)) {
    
    # connect to DB
    DBCON = dbConnect(RSQLite::SQLite(), DB_file)
    
    cat(paste0("DB doesn't exists! Creating at: ", DB_file), "\n");
    
    # create table
    dbCreateTable(DBCON, "factiva", fields = final)
    
  } else {
    
    # connect to DB
    DBCON = dbConnect(RSQLite::SQLite(), DB_file)
    
  }
  
  # write to db
  cat(paste0("Writing ", round(object.size(final)/2^20, 2),
             " MBs into DB using cusips: ", paste0(cusips, collapse = ", ")), "\n");
  dbWriteTable(DBCON, "factiva", final, append = T)
  
  # close connection
  cat(paste0("Disconnecting from DB: ", DB_file), "\n");
  dbDisconnect(DBCON);
  
  # create done file
  cat(paste0("Creating done files for cusip: ", paste0(cusips, collapse = ", ")), "\n");
  file.create(done_files);
  
  Sys.sleep(1);
  
}



DBCON = dbConnect(RSQLite::SQLite(), DB_file)


# create index for DB
# create singular index for the following columns
index_cols = c("doc_id", "cusip", "tic", "conm",
               "tag", "tag_content", "factiva_code",
               "language", "pub_date", "source", "word_count");

for(c in index_cols) {
  index_name = c
  index_cols = paste0(c, collapse = ", ")
  
  if(index_name %in% fetch_from_DB(DBCON, "PRAGMA INDEX_LIST(factiva)")$name) {
    cat("index:", index_name, " for factiva DB using column(s):", index_cols, "already exists. Moving to next!", "\n");
    next;
  }

  cat("Creating index:", index_name, " for factiva DB table for column(s):", index_cols, "\n");
  
  query = paste0("create index ", index_name, " on ", "factiva", "(", index_cols, ");");
  res = dbSendQuery(DBCON, query)
  dbClearResult(res)
}



# multi column index
multi_index_cols = c("tag", "cusip", "pub_date", "source", "word_count");

n = len(multi_index_cols)
possible_permutations = lapply(2:n, function(r) gtools::permutations(n, r))

cnt = 0
for(perm in possible_permutations[1]) {
 
  for(i in 1:nrow(perm)) {
    
    cnt = cnt + 1
    
    selected_cols = multi_index_cols[perm[i,]]
    
    index_name = paste0(selected_cols, collapse = "_")
    index_cols = paste0(selected_cols, collapse = ", ")
    
    if(index_name %in% fetch_from_DB(DBCON, "PRAGMA INDEX_LIST(factiva)")$name) {
      cat("Count:", cnt, "index:", index_name, " for factiva DB using column(s):", index_cols, "already exists. Moving to next!", "\n");
      next;
    }
      
      
    cat("Count:", cnt, "Creating index:", index_name, " for factiva DB table for column(s):", index_cols, "\n");
    
    query = paste0("create index ", index_name, " on ", "factiva", "(", index_cols, ");");
    res = dbSendQuery(DBCON, query)
    dbClearResult(res)
    
    Sys.sleep(300)

  }
   
}


dbDisconnect(DBCON);






