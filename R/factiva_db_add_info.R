library(DBI);

# DB file
DB_file = "F:/factiva_DB/factiva.db"

# connect to DB
DBCON = dbConnect(RSQLite::SQLite(), DB_file)

# function to fetch from DB
fetch_from_DB = function(con = DBCON, query, n = -1) {
  res = dbSendQuery(con, query)
  data = dbFetch(res, n)
  setDT(data)
  dbClearResult(res)
  # dbDisconnect(con)
  return(data)
}

N = fetch_from_DB(DBCON, "select max(_rowid_) as N from factiva limit 1")$N
Sys.time()
cols = fetch_from_DB(DBCON, "select * from factiva limit 0") %>% names
Sys.time()

# date range
min_PD = fetch_from_DB(DBCON, "select min(pub_date) as min_date from factiva")$min_date %>% as.Date
Sys.time()
max_PD = fetch_from_DB(DBCON, "select max(pub_date) as min_date from factiva")$min_date %>% as.Date
Sys.time()
# count by tag
N_by_tag = fetch_from_DB(DBCON, "select tag, count(*) as num_art from factiva group by tag")
Sys.time()

# In DB the following tags appear only once per document: HD, SC, CY. This is based on chaecking manually for 2-3 firms

# count by column list
art_by_date = fetch_from_DB(DBCON, "select pub_date, count(*) as num_art from factiva where tag = 'SC' group by pub_date");
Sys.time()
art_by_date[, pub_date := as.Date(pub_date)]
Sys.time()
art_by_cusip = fetch_from_DB(DBCON, "select cusip, count(*) as num_art from factiva where tag = 'SC' group by cusip");
Sys.time()
art_by_source = fetch_from_DB(DBCON, "select source, count(*) as num_art from factiva where tag = 'SC' group by source");
Sys.time()
art_by_word_count = fetch_from_DB(DBCON, "select word_count, count(*) as num_art from factiva where tag = 'SC' group by word_count");
Sys.time()

# count by tag list
art_by_ind = fetch_from_DB(DBCON, "select tag_content as industry, count(*) as num_art from factiva where tag = 'IN' group by tag_content")
Sys.time()
art_by_factiva_CO = fetch_from_DB(DBCON, "select tag_content as factiva_co, count(*) as num_art from factiva where tag = 'CO' group by tag_content")
Sys.time()
art_by_subject = fetch_from_DB(DBCON, "select tag_content as subject, count(*) as num_art from factiva where tag = 'NS' group by tag_content")
Sys.time()
art_by_region = fetch_from_DB(DBCON, "select tag_content as region, count(*) as num_art from factiva where tag = 'RE' group by tag_content")
Sys.time()


add_info = list(DB_size = N,
                columns = cols,
                DB_size_by_tags = N_by_tag,
                num_art_by_date = art_by_date,
                num_art_by_cusip = art_by_cusip,
                num_art_by_source = art_by_source,
                num_art_by_word_count = art_by_word_count,
                num_art_by_ind = art_by_ind,
                num_art_by_factiva_CO = art_by_factiva_CO,
                num_art_by_subject = art_by_subject,
                num_art_by_region = art_by_region)


saveRDS(add_info, "F:/factiva_DB/add_info.RData")



dbDisconnect(DBCON);



