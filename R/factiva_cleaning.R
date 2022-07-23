library(doParallel);
n_cores = detectCores();
# stopCluster(cl)


# tag_dir = "\\\\LAPTOP-59MM2PIF\\Factiva\\tags/"
tag_dir = "D:/tags/"

all_files = list.files(tag_dir, "*.csv", full.names = T);
set.seed(123);
all_files = all_files[sample(1:len(all_files), len(all_files))];

class_type = rep("character", 8);
class_type[4:5] = "Date";

# Read K files at a time and then create a master dataset
K = 25;
N = ceiling(len(all_files)/ K) %>% as.integer;

# divide N into chunks of n_cores
N_unit = 4*n_cores;
N_times = as.integer(ceiling(N/N_unit));
N_st = seq(1, N_unit*N_times, by = N_unit)
N_en = seq(N_unit, N_unit*N_times, by = N_unit)
N_en[N_times] = min(N_en[N_times], N)

news = extra_info = unq_list = data.table();

cl = makeCluster(n_cores);
registerDoParallel(cl);
# the below loop took 2 hours and 40 minutes
for(n in 1:N_times) {
  
  cat("n:", n, "starting loop!", "\n");

  time_taken = system.time(
    news_n <- foreach(i = N_st[n]:N_en[n]) %dopar% {
      
      cat("Working on i: ", i, "at ", as.character(Sys.time()), "\n");
      
      st_idx = K*(i-1) + 1;
      en_idx = K*i;
      en_idx = min(en_idx, len(all_files)); # corner case
      
      dt = lapply(all_files[st_idx:en_idx], fread, colClasses = class_type) %>% rbindlist;
      
      # the below are multi-tags for a document, i.e. each document can have many such tags
      tmp = dt[tag %in% c("CO", "IN", "NS", "RE"), .(tag, tag_content)] %>% unique;
      
      tmp[, code := stringr::str_split(tag_content, "\\s+:\\s+", simplify = T)[,1]];
      tmp[, name := stringr::str_split(tag_content, "\\s+:\\s+", simplify = T)[,2]];
      tmp[, tag_content := NULL];
      
      unq_list = unique(tmp);
      
      # save doc_id alog with CO, IN, NS and RE tags
      tmp = dt[tag %in% c("CO", "IN", "NS", "RE"), .(doc_id, tag, tag_content)];
      tmp[, code := stringr::str_split(tag_content, "\\s+:\\s+", simplify = T)[,1]];
      tmp[, tag_content := NULL];
      tmp = tmp %>% unique;
      
      extra_info = unique(tmp);
      
      # now only focus on tags that are unique for a doc like headline, source etc
      tmp = dt[tag %in% c("PD", "SN", "HD", "WC")];
      
      # Now we are left with only 4 tags: headline (HD), date (PD), publication name (SN) and, word count (WC). All these are unique for a document!
      tmp = dcast(tmp, cusip + tic + conm + start_date + end_date + doc_id ~ tag, value.var = "tag_content");
      
      # transform PD and WC
      tmp[, PD := as.Date(PD, format = "%d %B %Y")];
      tmp[, WC := stringr::str_extract(WC, "\\d+\\b") %>% as.integer];
      
      # add to news
      # tmp
      list(unq_list, extra_info, tmp)
      
    }
  )
  cat("computation completed in user:", time_taken[1], "system:", time_taken[2], "elapsed:", time_taken[3], "\n");
  
  unq_list_n   = lapply(news_n, `[[`, 1) %>% rbindlist
  extra_info_n = lapply(news_n, `[[`, 2) %>% rbindlist
  news_n       = lapply(news_n, `[[`, 3) %>% rbindlist
  
  # global update
  news       = rbind(news,       news_n);
  extra_info = rbind(extra_info, extra_info_n);
  unq_list   = rbind(unq_list,   unq_list_n);
  # keep unique entries
  extra_info = unique(extra_info);
  unq_list = unique(unq_list);
  cat("global vars updated!", "\n");
  remove(news_n, extra_info_n, unq_list_n);
  
}
stopCluster(cl);






# same code may be associated wth slightly different names
unq_list = unq_list[, .SD[1], by = .(tag, code)];
unq_list[, tag_code := paste0(tag, "_", code)];
unq_list = unq_list[name != ""];
setorder(unq_list, tag_code);
unq_list[, tag_code_num := 1:.N];

# save for future
fwrite(unq_list, "D:/factiva_cleaned/unq_list.csv");
# unq_list = fread("D:/factiva_cleaned/unq_list.csv");




# save for future
fwrite(news, "D:/factiva_cleaned/news.csv");
# news = fread("D:/factiva_cleaned/news.csv");


# unique document ids
unq_doc = news[, unique(doc_id)];
unq_doc = data.table(doc_id = unq_doc);
setorder(unq_doc, doc_id);
unq_doc[, doc_id_num := 1:.N];

# save for future
fwrite(unq_doc, "D:/factiva_cleaned/unq_doc.csv");
# unq_doc = fread("D:/factiva_cleaned/unq_doc.csv");






# extra_info
extra_info = extra_info %>% unique;
extra_info[, tag_code := paste0(tag, "_", code)];
extra_info = extra_info[tag_code %in% unq_list$tag_code];

# merge doc id num and tag code num
setorder(extra_info, doc_id, tag_code);
MRG(extra_info, unq_doc, by = "doc_id");
gc();
MRG(extra_info, unq_list[, .(tag_code, tag_code_num)], by = "tag_code");
gc();
# add tag names for ease of usage
MRG(extra_info, unq_list[, .(tag_code_num, name)], by = "tag_code_num");
gc();

setorder(extra_info, doc_id_num, tag_code_num);
# save for future
fwrite(extra_info, "D:/factiva_cleaned/extra_info.csv");
# extra_info = fread("D:/factiva_cleaned/extra_info.csv");










# We can code the information in `extra_info` i.e. which document has what tags AND which tags had what documents in a sparse matrix.
M = Matrix::sparseMatrix(i = extra_info$doc_id_num,
                         j = extra_info$tag_code_num,
                         x = T,
                         dims = c(nrow(unq_doc), nrow(unq_list)));
saveRDS(M, "D:/factiva_cleaned/sparse_extra_info.RData", compress = F);













