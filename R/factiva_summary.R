unq_list = fread("D:/factiva_cleaned/unq_list.csv");
news = fread("D:/factiva_cleaned/news.csv");
unq_doc = fread("D:/factiva_cleaned/unq_doc.csv");
extra_info = fread("D:/factiva_cleaned/extra_info.csv");
lead_para = fread("D:/factiva_cleaned/lead_para.csv");


# News articles by source, year and word counts
dt = news[, .SD[1], .SDcols = c("PD", "WC", "SN", "HD"), by = doc_id];
dt[, year := year(PD)];
dt[, WC_100 := as.integer(ceiling(WC/100))];
dt[, HD_wc := stringr::str_count(HD, "\\S+")];

news_by_year = dt[year %in% 1990:2020, .N, year][order(year)];
news_by_words = dt[WC <= 10000, .N, WC_100][order(WC_100)];
top50_news_source = dt[, .N, SN][order(-N)][1:50];
news_by_headline_words = dt[HD_wc <= 50, .N, HD_wc][order(HD_wc)];

# plot
pdf(file = "D:/factiva_cleaned/summary/news_by_year.pdf", width = 10, height = 6);
plot(news_by_year, type = "b", lwd = 2, xlab = "Year", ylab = "Number of News Articles", col = "blue");
dev.off();

pdf(file = "D:/factiva_cleaned/summary/news_by_words.pdf", width = 10, height = 6);
plot(news_by_words, type = "b", lwd = 2, xlab = "Num of Words in Artcile (in 100s)", ylab = "Number of News Articles", col = "blue");
dev.off();

pdf(file = "D:/factiva_cleaned/summary/top50_news_source.pdf", width = 14, height = 7);
par(mar = c(15, 5, 4, 2));
barplot(top50_news_source$N, names.arg = top50_news_source$SN, las = 2);
par(mar = c(5, 4, 4, 2));
dev.off();

pdf(file = "D:/factiva_cleaned/summary/news_by_headline_words.pdf", width = 10, height = 6);
plot(news_by_headline_words, type = "b", lwd = 2, xlab = "Num of Words in Headline", ylab = "Number of News Articles", col = "blue");
dev.off();


remove(dt, news); gc();


# num articles by industry, subject and region
tmp = extra_info[, .N, tag_code];
MRG(tmp, unq_list[, .(tag_code, tag, code, name)], by = "tag_code");
tmp[, tag_code := NULL];
tmp = tmp[, .(tag, code, name, num_news = N)];

tmp[, nchar_code := nchar(code)];
setorder(tmp, tag, code);

news_by_comp = tmp[tag == "CO", -c("tag", "nchar_code"), with = F][order(-num_news)];
news_by_ind  = tmp[tag == "IN", -c("tag", "nchar_code"), with = F];
news_by_sub  = tmp[tag == "NS", -c("tag", "nchar_code"), with = F];
news_by_reg  = tmp[tag == "RE", -c("tag", "nchar_code"), with = F][order(-num_news)];

# save
fwrite(news_by_comp, "D:/factiva_cleaned/summary/news_by_comp.csv");
fwrite(news_by_ind, "D:/factiva_cleaned/summary/news_by_ind.csv");
fwrite(news_by_sub, "D:/factiva_cleaned/summary/news_by_sub.csv");
fwrite(news_by_reg, "D:/factiva_cleaned/summary/news_by_reg.csv");

