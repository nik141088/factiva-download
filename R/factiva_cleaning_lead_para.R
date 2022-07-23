library(doParallel);
n_cores = detectCores();
# stopCluster(cl)


# tags_dir = "\\\\LAPTOP-59MM2PIF\\Factiva\\tags/"
tags_dir = "D:/tags/"

all_files = list.files(tags_dir, "*.csv", full.names = T);
set.seed(123);
all_files = all_files[sample(1:len(all_files), len(all_files))];

# Read K files at a time and then create a master dataset
K = 25;
N = ceiling(len(all_files)/ K) %>% as.integer;

# divide N into chunks of n_cores
N_unit = 4*n_cores;
N_times = as.integer(ceiling(N/N_unit));
N_st = seq(1, N_unit*N_times, by = N_unit)
N_en = seq(N_unit, N_unit*N_times, by = N_unit)
N_en[N_times] = min(N_en[N_times], N)

lead_para = data.table();

cl = makeCluster(n_cores);
registerDoParallel(cl);
# the below takes roughly 5 minutes
for(n in 1:N_times) {
  
  cat("n:", n, "starting loop!", "\n");
  
  time_taken = system.time(
    lead_para_n <- foreach(i = N_st[n]:N_en[n]) %dopar% {
      
      cat("Working on i: ", i, "at ", as.character(Sys.time()), "\n");
      
      st_idx = K*(i-1) + 1;
      en_idx = K*i;
      en_idx = min(en_idx, len(all_files)); # corner case
      
      dt = lapply(all_files[st_idx:en_idx], fread, select = c("doc_id", "tag", "tag_content")) %>% rbindlist;
      
      # only keep lead para tag
      dt = dt[tag == "LP", .(doc_id, LP = tag_content)];
      
      # keep unique leading paras only
      dt = unique(dt);
      dt

    }
  )
  cat("computation completed in user:", time_taken[1], "system:", time_taken[2], "elapsed:", time_taken[3], "\n");
  

  # global update
  lead_para = rbind(lead_para, rbindlist(lead_para_n));
  lead_para = unique(lead_para);
  cat("global vars updated!", "\n");
  remove(lead_para_n);
  
}
stopCluster(cl);

setorder(lead_para, doc_id);
lead_para = lead_para[, .SD[1], by = doc_id];

# save for future
fwrite(lead_para, "D:/factiva_cleaned/lead_para.csv");
# lead_para = fread("D:/factiva_cleaned/lead_para.csv");











