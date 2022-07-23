# Extract factiva company tags from processed files

# diff directories
# root_dir = "F:/";
root_dir = "\\\\LAPTOP-59MM2PIF\\Factiva\\";
csv_dir = paste0(root_dir, "csv/");
tag_dir = paste0(root_dir, "tags/");
text_dir = paste0(root_dir, "text/");

verified_files = list.files(csv_dir, "*.csv");
verified_cusips = substr(verified_files, 1, 8);

sample = fread(paste0(root_dir, "essentials/factiva_sample_firms.csv"), sep = "_");

# All tags in tag_dir must have a cusip that is present in verified_cusips. For instance if I delete a cusip from verified_files then it should be deleted from tags as well
tag_files = list.files(tag_dir, "*.csv");
tag_cusip = substr(tag_files, 6, 13);
idx = which(!(tag_cusip %in% verified_cusips));
if(len(idx) > 0) {
  file.remove(paste0(tag_dir, tag_files[idx]));
}

# Each tag file must be computed after its verified file is computed. In other words, tag file mtime must be larger than the corresponding verified file mtime.
verified_mtime = file.mtime(paste0(csv_dir, tag_cusip, ".csv"));
tag_mtime = file.mtime(paste0(tag_dir, tag_files));
idx = (verified_mtime > tag_mtime)
idx = which(idx == T)
if(len(idx) > 0) {
  stop("Verify first before deleting!");
  file.remove(paste0(tag_dir, tag_files[idx]));
  file.remove(paste0(text_dir, str_replace_all(tag_files[idx], "tags", "text")));
}


cnt = 0;
for(c in verified_cusips) {

  cnt = cnt + 1;
  
  fname = paste0(tag_dir, "tags_", c, ".csv");
  text_file = paste0(text_dir, "text_", c, ".csv");
  
  cat("Working on cnt: ", cnt, " / ", len(verified_cusips), " cusip: ", c, "\n", sep = "");
  
  if(file.exists(fname)) {
    cat("Tags already created for cusip: ", c, "\n", sep = "");
    next;
  }
  
  dt = fread(paste0(csv_dir, c, ".csv"), colClasses = "character");
  # remove entries with NA content
  dt = dt[!is.na(content) & content != ""];
  # make sure there is some data in dt
  if(nrow(dt) == 0) {
    cat("No data available for cusip: ", c, "\n", sep = "");
    next;
  }
  dt[, num_tags := stringr::str_count(dt$content, "----NEXT----") + 1];
  final = data.table(doc_id = rep(dt$doc_id, dt$num_tags),
                     tag = rep(dt$tag, dt$num_tags),
                     tag_content = stringr::str_split(dt$content, "----NEXT----") %>% unlist);
  # add start and end dates from dt. Merge using (doc_id, tag) pair
  MRG(final, dt[, .(doc_id, tag, cusip, tic, start_date, end_date)], by = c("doc_id", "tag"));
  final = final[, .(cusip, tic, conm = sample[cusip == c, comp_conm], start_date, end_date, doc_id, tag, tag_content)];
  setorder(final, doc_id, tag);
  
  # save it
  fwrite(final, fname);
  
  cat("Tags cusip: ", c, " saved in file: ", fname, "\n", sep = "");

  # save text separately
  final = final[tag %in% c("LP", "TD"), .(doc_id, tag, tag_content)] %>% dcast(doc_id ~ tag, value.var = "tag_content");
  fwrite(final, text_file);
  
  cat("Text cusip: ", c, " saved in file: ", text_file, "\n", sep = "");
  
}




