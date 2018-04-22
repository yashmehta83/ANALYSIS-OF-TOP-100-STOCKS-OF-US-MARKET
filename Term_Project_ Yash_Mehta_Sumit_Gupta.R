### Collecting Storing and Retrieving Data
### R code file for Term Project - Analysis of top 100 Stocks from US market
### Term Project by - Yash Mehta and Sumit Gupta
### (Refer Project report for details and explanations)

##Install and include package required for data scraping - rvest and R2HTML
#install.packages("rvest")
#install.packages("R2HTML")
#install.packages("sqldf")
library(rvest)
library(R2HTML)
library(stringr)
library(RSQLite)
library(sqldf)
library(ggplot2)

##-------------- Data Collecting--------------

## Read the csv files scraped using "Instant Data Scraper" for top 100 stocks of two categories
top_100_by_alpha<- read.csv("top-100-stocks-top.csv", header=T) ## Top 100 by Wtd Alpha
top_by_price_vol<- read.csv("price-volume-leaders.csv", header = T) ## Top 200 by Price Volume

Company_fifty_two_week_price<- unique(rbind(top_100_by_alpha[1:2],top_by_price_vol[1:2]))

## Scarpping data from barchart.com

## Get 52 week highest and lowest values for each
### This a Time consuming part and hence the code will take a few minuites to run the code since the code is 
### scraping close to 300 web pages.
fetch_price<- function(company_tag)
{
  
  webURL<- gsub(" ","",paste("https://www.barchart.com/stocks/quotes/",company_tag))
  webdata<- read_html(webURL)
  Period_table<- html_table(html_nodes(webdata, "table")[[1]],fill = T )[3,]
  pos<- gregexpr(pattern=' ',Period_table[1,2])
  fifty_two_week_low<- as.numeric(gsub(",","",substring(Period_table[1,2],1,pos[[1]][1]-1)))
  pos<- gregexpr(pattern=' ',Period_table[1,4])
  fifty_two_week_high<- as.numeric(gsub(",","",substring(Period_table[1,4],1,pos[[1]][1]-1)))
  df<- c(fifty_two_week_low,fifty_two_week_high)
  return(df)
}

for(i in 1:nrow(Company_fifty_two_week_price))
{
  fifty_two_week_price<- fetch_price(Company_fifty_two_week_price[i,1])
  Company_fifty_two_week_price$Fifty_Two_Week_Low[i]<- fifty_two_week_price[1]
  Company_fifty_two_week_price$Fifty_Two_Week_high[i]<- fifty_two_week_price[2]
  Company_fifty_two_week_price$fifty_Two_Week_Change[i]<-((fifty_two_week_price[2]-fifty_two_week_price[1])/fifty_two_week_price[1])
 
}

## Scrapper for fundamentals, ratios, growth and per share info
scraperFunc<- function(Company_Tag)
{
  #Company_Tag<- "TWNKW"
  CompanyFundamentalURL<- gsub(" ","",paste("https://www.barchart.com/stocks/quotes/",Company_Tag,"/profile"))
  CompanyData<- read_html(CompanyFundamentalURL)
  tables_data<-  html_table(html_nodes(CompanyData, "table"),fill = T )
  return(tables_data)
}
## Declaring dataframes to store the data after scaping
Fundamentals<- data.frame(company=character(),
                          market_Capitalization= double(),
                          shares_Outstanding = double(),
                          annual_Sales = double(),
                          annual_Net_Income = double(),
                          thirtysix_Month_Beta = double(),
                          percentage_Insider_Shareholder = double(),
                          percentage_Institutional_Shareholder = double(),stringsAsFactors=FALSE)

ratios <- data.frame( company=character(),
                      price_per_Earnings= double(),
                      price_per_Earnings_Fwd= double(),
                      price_per_Earnings_Growth= double(),
                      return_on_Equity = double(),
                      return_on_Assets = double(),
                      profit_Margin = double(),
                      debt_per_Equity = double(),
                      price_per_sales = double(),
                      price_per_Cash_Flow = double(),
                      price_per_Book= double(),
                      bookValue_per_Share = double(),
                      interest_Coverage= double(),stringsAsFactors=FALSE)

per_Share_Info <- data.frame( company=character(),
                              most_Recent_Earnings = double(),
                              next_Earnings_Date = character(),
                              earnings_per_Share = double(),
                              EPS_Growth_vs_Prev_Qtr= double(),
                              EPS_Growth_vs_Prev_Year= double(),
                              annual_Dividend_Rate =double(),
                              annual_Dividend_Yield =double(),
                              NA1 = character(),
                              NA2= character(),
                              NA3= character(),
                              divident_Payout_Ratio = double(),
                              NA4 = character(),stringsAsFactors=FALSE)

growth<- data.frame(company=character(),
                    one_year_return = double(),
                    three_year_return= double(),
                    five_year_return = double(),
                    five_year_revenue_growth = double(),
                    five_year_earnings_growth = double(),
                    five_year_dividend_growth = double(),stringsAsFactors=FALSE)

## Scapping data for fundamentals and others
### This a Time consuming part and hence code will take a few minuites to run the code since the code is 
### scraping close to 300 web pages.
for(i in 1:nrow(Company_fifty_two_week_price))
{
  company<-Company_fifty_two_week_price[i,1]
  company<-as.character(company)
  all_tables<- scraperFunc(company[1])
  if(length(all_tables)>0) 
  {
    if(nrow(all_tables[[1]][2])==7) ## Checking if the correct table is being scrapped
    {
      Fundamentals[i,]<- cbind(as.character(company[1]),t(all_tables[[1]][2]))
    }
  }
  if(length(all_tables)>1) 
  {
    if(nrow(all_tables[[2]][2])==6) ## Checking if the correct table is being scrapped
    {
      growth[i,]<- cbind(company,t(all_tables[[2]][2]))
    }
  }
  if(length(all_tables)>2) 
  {
    if(nrow(all_tables[[3]][2])==12) ## Checking if the correct table is being scrapped
    {
      per_Share_Info[i,]<- cbind(company,t(all_tables[[3]][2]))
    }
  }
  if(length(all_tables)>3)
  {
    if(nrow(all_tables[[4]][2])==12) ## Checking if the correct table is being scrapped
    {
      ratios[i,]<- cbind(company,t(all_tables[[4]][2]))
    }
  }
}

## ----------------Data Storing and Cleaning ------------------------------------

## Removing rows with NA values
Fundamentals<- na.omit(Fundamentals)


## creating the tables in the database
sql_1 <- "
CREATE TABLE companies (
company_Tag TEXT PRIMARY KEY,
company_Name TEXT DEFAULT Name
)"

sql_2<-"CREATE TABLE stocks_By_Price_Vol(
price_Vol_Rank INTEGER PRIMARY KEY AUTOINCREMENT,
company TEXT,
last_traded_Price NUMBER,
One_Day_Price_Change NUMBER,
One_Day_Price_Change_Percent NUMBER,
volume_Traded NUMBER,
price_Volume NUMBER,
fifty_Two_Week_High NUMBER,
fifty_Two_Week_Low NUMBER,
fifty_Two_Week_Percent_Change NUMBER,
FOREIGN KEY (company) REFERENCES companies(company_Tag))"

sql_3<- "CREATE TABLE stocks_By_Alpha_Value(
Wtd_Alpha_Rank INTEGER PRIMARY KEY AUTOINCREMENT,
company TEXT,
last_traded_Price NUMBER,
One_Day_Price_Change NUMBER,
One_Day_Price_Change_Percent NUMBER,
wtd_Alpha NUMBER,
fifty_Two_Week_High NUMBER,
fifty_Two_Week_Low NUMBER,
fifty_Two_Week_Percent_Change NUMBER,
FOREIGN KEY (company) REFERENCES companies(company_Tag))"

sql_7<-"CREATE TABLE Fundamentals(
fundamentals_id INTEGER PRIMARY KEY AUTOINCREMENT,
company TEXT,
market_Capitalization NUMBER,
shares_Outstanding NUMBER,
annual_Sales NUMBER,
annual_Net_Income NUMBER,
thirtysix_Month_Beta NUMBER,
percentage_Insider_Shareholder NUMBER,
percentage_Institutional_Shareholder NUMBER,
FOREIGN KEY (company) REFERENCES companies(company_Tag))"

sql_4<-"CREATE TABLE ratios(
ratios_id INTEGER PRIMARY KEY AUTOINCREMENT,
price_per_Earnings NUMBER,
price_per_Earnings_Fwd NUMBER,
price_per_Earnings_Growth NUMBER,
return_on_Equity NUMBER,
return_on_Assets NUMBER,
profit_Margin NUMBER,
debt_per_Equity NUMBER,
price_per_sales NUMBER,
price_per_Cash_Flow NUMBER,
price_per_Book NUMBER,
bookValue_per_Share NUMBER,
interest_Coverage NUMBER,
fundamentals INTEGER,
FOREIGN KEY (fundamentals) REFERENCES Fundamentals(fundamentals_id))"

sql_5<-"CREATE TABLE per_Share_Info(
per_Share_id INTEGER PRIMARY KEY AUTOINCREMENT,
most_Recent_Earnings NUMBER,
next_Earnings_Date DATE,
earnings_per_Share NUMBER,
EPS_Growth_vs_Prev_Qtr NUMBER,
EPS_Growth_vs_Prev_Year NUMBER,
annual_Dividend_Rate NUMBER,
annual_Dividend_Yield NUMBER,
divident_Payoiut_Ratio NUMBER,
fundamentals INTEGER,
FOREIGN KEY (fundamentals) REFERENCES Fundamentals(fundamentals_id))"

sql_6<- "CREATE TABLE growth(
growth_id INTEGER PRIMARY KEY AUTOINCREMENT,
one_year_return NUMBER,
three_year_return NUMBER,
five_year_return NUMBER,
five_year_revenue_growth NUMBER,
five_year_earnings_growth NUMBER,
five_year_dividend_growth NUMBER,
fundamentals INTEGER,
FOREIGN KEY (fundamentals) REFERENCES Fundamentals(fundamentals_id))"

# Create an SQLite database using above queries
con <- dbConnect(SQLite(), dbname = "StocksProjectDB.sqlite")
dbListTables(con)
dbSendQuery(conn=con,sql_1)
dbSendQuery(conn=con,sql_2)
dbSendQuery(conn=con,sql_3)
dbSendQuery(conn=con,sql_4)
dbSendQuery(conn=con,sql_5)
dbSendQuery(conn=con,sql_6)
dbSendQuery(conn=con,sql_7)

# preparing data to insert in database
## this includes the cleaning part and making data fit to store in db.

# Companies table data
Company_db<- subset(Company_fifty_two_week_price, select=c(Symbol, Name))
colnames(Company_db)<- c("company_Tag","company_Name")

#price volume table data
Price_vol_table<- top_by_price_vol[c(-2,-8)]
Price_vol_table$Fifty_two_week_high<- Company_fifty_two_week_price[match(Price_vol_table$Symbol,Company_fifty_two_week_price$Symbol),4]
Price_vol_table$Fifty_two_week_low<- Company_fifty_two_week_price[match(Price_vol_table$Symbol,Company_fifty_two_week_price$Symbol),3]
Price_vol_table$Fifty_two_week_change<- Company_fifty_two_week_price[match(Price_vol_table$Symbol,Company_fifty_two_week_price$Symbol),5]
colnames(Price_vol_table)<- c("company",
                              "last_traded_Price",
                              "One_Day_Price_Change",
                              "One_Day_Price_Change_Percent",
                              "volume_Traded",
                              "price_Volume",
                              "fifty_Two_Week_High",
                              "fifty_Two_Week_Low",
                              "fifty_Two_Week_Percent_Change")

#wtd Alpla tables
Wtd_Alpha_table<- top_100_by_alpha[c(-2,-10)]
colnames(Wtd_Alpha_table)<- c("company",
                              "last_traded_Price",
                              "One_Day_Price_Change",
                              "One_Day_Price_Change_Percent",
                              "wtd_Alpha",
                              "fifty_Two_Week_High",
                              "fifty_Two_Week_Low",
                              "fifty_Two_Week_Percent_Change")

#Fundamentals table
Fundamentals<- cbind.data.frame(1:nrow(Fundamentals),Fundamentals)
colnames(Fundamentals)<- c("Fundamentals_id",colnames(Fundamentals[2:ncol(Fundamentals)]))
colnames(Fundamentals)<-c("fundamentals_id","company",
                          "market_Capitalization",
                          "shares_Outstanding",
                          "annual_Sales",
                          "annual_Net_Income",
                          "thirtysix_Month_Beta",
                          "percentage_Insider_Shareholder",
                          "percentage_Institutional_Shareholder")
# Column wise data cleaning
Fundamentals$market_Capitalization<- as.numeric(gsub(",","",as.character(Fundamentals$market_Capitalization)))
Fundamentals$shares_Outstanding<-as.numeric(gsub(",","",as.character(Fundamentals$shares_Outstanding)))

Fundamentals$annual_Sales<-gsub(",","",as.character(Fundamentals$annual_Sales))
Fundamentals$annual_Sales<-gsub(" K","000",as.character(Fundamentals$annual_Sales))
Fundamentals$annual_Sales<-as.numeric(gsub(" M","000000",as.character(Fundamentals$annual_Sales)))

Fundamentals$annual_Net_Income<-gsub(",","",as.character(Fundamentals$annual_Net_Income))
Fundamentals$annual_Net_Income<-gsub(" K","000",as.character(Fundamentals$annual_Net_Income))
Fundamentals$annual_Net_Income<-as.numeric(gsub(" M","000000",as.character(Fundamentals$annual_Net_Income)))

Fundamentals$thirtysix_Month_Beta<-as.numeric(Fundamentals$thirtysix_Month_Beta)
Fundamentals$percentage_Insider_Shareholder<- as.numeric(gsub("%","",as.character(Fundamentals$percentage_Insider_Shareholder)))/100
Fundamentals$percentage_Institutional_Shareholder<- as.numeric(gsub("%","",as.character(Fundamentals$percentage_Institutional_Shareholder)))/100

#Ratios table
tmp <- sapply(1:nrow(Fundamentals), function(fundamentals_id) {
  aa <- Fundamentals[fundamentals_id,]
  idx = which(ratios$company == aa$company)
  ratios[idx, "fundamentals"] <<- fundamentals_id
  return(NULL)
})
ratios<- ratios[c(-1)]
ratios<-na.omit(ratios)
colnames(ratios)<- c("price_per_Earnings",
                     "price_per_Earnings_Fwd",
                     "price_per_Earnings_Growth",
                     "return_on_Equity",
                     "return_on_Assets",
                     "profit_Margin",
                     "debt_per_Equity",
                     "price_per_sales",
                     "price_per_Cash_Flow",
                     "price_per_Book",
                     "bookValue_per_Share",
                     "interest_Coverage",
                     "fundamentals")

# Column wise data cleaning
for (i in 1:nrow(ratios)){
  for (j in 1:ncol(ratios)){
    ratios[i,j] <- gsub(",","",ratios[i,j])
  }
}

#Per share info table

tmp <- sapply(1:nrow(Fundamentals), function(fundamentals_id) {
  aa <- Fundamentals[fundamentals_id,]
  idx = which(per_Share_Info$company == aa$company)
  per_Share_Info[idx, "fundamentals"] <<- fundamentals_id
  return(NULL)
})
per_Share_Info<-per_Share_Info[c(-1,-9,-10,-11,-13)]
per_Share_Info<- na.omit(per_Share_Info)
colnames(per_Share_Info)<- c("most_Recent_Earnings",
                             "next_Earnings_Date",
                             "earnings_per_Share",
                             "EPS_Growth_vs_Prev_Qtr",
                             "EPS_Growth_vs_Prev_Year",
                             "annual_Dividend_Rate",
                             "annual_Dividend_Yield",
                             "divident_Payoiut_Ratio",
                             "fundamentals")
for (i in 1:nrow(per_Share_Info)){
  for (j in 1:ncol(per_Share_Info)){
    per_Share_Info[i,j] <- gsub(",","",per_Share_Info[i,j])
  }
}

per_Share_Info$most_Recent_Earnings<- as.numeric(str_split(as.character(per_Share_Info$most_Recent_Earnings)," on ")[[1]][1])
per_Share_Info$EPS_Growth_vs_Prev_Qtr<- as.numeric(gsub("%","",as.character(per_Share_Info$EPS_Growth_vs_Prev_Qtr)))/100
per_Share_Info$EPS_Growth_vs_Prev_Year<- as.numeric(gsub("%","",as.character(per_Share_Info$EPS_Growth_vs_Prev_Year)))/100
per_Share_Info$annual_Dividend_Yield<- as.numeric(gsub("%","",as.character(per_Share_Info$annual_Dividend_Yield)))/100



#Growth table

tmp <- sapply(1:nrow(Fundamentals), function(fundamentals_id) {
  aa <- Fundamentals[fundamentals_id,]
  idx = which(growth$company == aa$company)
  growth[idx, "fundamentals"] <<- fundamentals_id
  return(NULL)
})
growth<- na.omit(growth)
growth<- growth[c(-1)]

colnames(growth)<- c("one_year_return" ,
                     "three_year_return",
                     "five_year_return",
                     "five_year_revenue_growth",
                     "five_year_earnings_growth",
                     "five_year_dividend_growth",
                     "fundamentals")

for (i in 1:nrow(growth))
{
  for (j in 1:(ncol(growth)-1))
  {
   growth[i,j]<-gsub("%","",as.character(growth[i,j]))
   growth[i,j]<-as.numeric(gsub(",","",as.character(growth[i,j])))/100
  }
}

## Inserting Data in DB
dbWriteTable(conn=con, name="companies",Company_db,append=T,row.names=F, colnames=F)
dbWriteTable(conn=con, name="stocks_By_Price_Vol",Price_vol_table, append=T,row.names=F, colnames=F)
dbWriteTable(conn=con, name="stocks_By_Alpha_Value",Wtd_Alpha_table, append=T,row.names=F, colnames=F)
dbWriteTable(conn=con, name="Fundamentals",Fundamentals, append=T,row.names=F, colnames=F)
dbWriteTable(conn=con, name="ratios",ratios, append=T,row.names=F, colnames=F)
dbWriteTable(conn=con, name="per_Share_Info",per_Share_Info, append=T,row.names=F, colnames=F)
dbWriteTable(conn=con, name="growth",growth, append=T,row.names=F, colnames=F)


##------------- Data Retrieving -----------------------
##Data Retrival Queries
Companies_price_vol_Fundamental<- dbGetQuery(con, "SELECT
                                                    (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                                                    market_Capitalization,
                                                    shares_Outstanding,
                                                    annual_Sales,
                                                    annual_Net_Income,
                                                    thirtysix_Month_Beta,
                                                    percentage_Insider_Shareholder,
                                                    percentage_Institutional_Shareholder
                                                    FROM stocks_By_Price_Vol, companies,Fundamentals
                                                    WHERE stocks_By_Price_Vol.company=companies.company_Tag
                                                    AND Fundamentals.company=companies.company_Tag")

Companies_price_vol_Ratios<- data.frame(dbGetQuery(con,"SELECT 
                                        (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                                        price_per_Earnings,
                                        price_per_Earnings_Fwd,
                                        price_per_Earnings_Growth,
                                        return_on_Equity,
                                        return_on_Assets,
                                        profit_Margin,
                                        debt_per_Equity,
                                        price_per_sales,
                                        price_per_Cash_Flow,
                                        price_per_Book,
                                        bookValue_per_Share,
                                        interest_Coverage
                                        FROM ((stocks_By_Price_Vol JOIN companies ON stocks_By_Price_Vol.company=companies.company_Tag) 
                                        JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                                        JOIN ratios ON ratios.fundamentals= Fundamentals.fundamentals_id"))

Companies_price_vol_Growth<- dbGetQuery(con,"SELECT 
                                         (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                                         one_year_return,
                                         three_year_return,
                                         five_year_return,
                                         five_year_revenue_growth,
                                         five_year_earnings_growth,
                                         five_year_dividend_growth
                                       
                                        FROM ((stocks_By_Price_Vol JOIN companies ON stocks_By_Price_Vol.company=companies.company_Tag) 
                                        JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                                        JOIN growth ON growth.fundamentals= Fundamentals.fundamentals_id")

Data_Price_vol_ALL<-dbGetQuery(con,
                               "SELECT
                               (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                               one_year_return,
                               three_year_return,
                               five_year_return,
                               five_year_revenue_growth,
                               five_year_earnings_growth,
                               five_year_dividend_growth
                               price_per_Earnings,
                               price_per_Earnings_Fwd,
                               return_on_Equity,
                               return_on_Assets,
                               profit_Margin,
                               debt_per_Equity,
                               price_per_sales,
                               price_per_Cash_Flow,
                               price_per_Book,
                               bookValue_per_Share,
                               interest_Coverage,
                               market_Capitalization,
                               shares_Outstanding,
                               annual_Sales,
                               annual_Net_Income,
                               thirtysix_Month_Beta,
                               percentage_Insider_Shareholder,
                               percentage_Institutional_Shareholder
                               FROM ((((stocks_By_Price_Vol JOIN companies ON stocks_By_Price_Vol.company=companies.company_Tag) 
                               JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                               JOIN growth ON growth.fundamentals= Fundamentals.fundamentals_id)
                               JOIN ratios ON ratios.fundamentals=Fundamentals.fundamentals_id)")
                               

Companies_wtd_alpha_Fundamental<- dbGetQuery(con, "SELECT
                                             (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                                             market_Capitalization,
                                             shares_Outstanding,
                                             annual_Sales,
                                             annual_Net_Income,
                                             thirtysix_Month_Beta,
                                             percentage_Insider_Shareholder,
                                             percentage_Institutional_Shareholder
                                             FROM stocks_By_Alpha_Value, companies,Fundamentals
                                             WHERE stocks_By_Alpha_Value.company=companies.company_Tag
                                             AND Fundamentals.company=companies.company_Tag")

Companies_wtd_alpha_Ratios<- data.frame(dbGetQuery(con,"SELECT 
                                                   (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                                                   price_per_Earnings,
                                                   price_per_Earnings_Fwd,
                                                   price_per_Earnings_Growth,
                                                   return_on_Equity,
                                                   return_on_Assets,
                                                   profit_Margin,
                                                   debt_per_Equity,
                                                   price_per_sales,
                                                   price_per_Cash_Flow,
                                                   price_per_Book,
                                                   bookValue_per_Share,
                                                   interest_Coverage
                                                   FROM ((stocks_By_Alpha_Value JOIN companies ON stocks_By_Alpha_Value.company=companies.company_Tag) 
                                                   JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                                                   JOIN ratios ON ratios.fundamentals= Fundamentals.fundamentals_id"))

Companies_wtd_alpha_Growth<- dbGetQuery(con,"SELECT 
                                        (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                                        one_year_return,
                                        three_year_return,
                                        five_year_return,
                                        five_year_revenue_growth,
                                        five_year_earnings_growth,
                                        five_year_dividend_growth
                                        FROM ((stocks_By_Alpha_Value JOIN companies ON stocks_By_Alpha_Value.company=companies.company_Tag) 
                                        JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                                        JOIN growth ON growth.fundamentals= Fundamentals.fundamentals_id")

Data_wtd_alpha_ALL<-dbGetQuery(con,
                               "SELECT 
                               (fifty_Two_Week_High+fifty_Two_Week_Low)/2 as Avg_Price,
                               one_year_return,
                               three_year_return,
                               five_year_return,
                               five_year_revenue_growth,
                               five_year_earnings_growth,
                               five_year_dividend_growth
                               price_per_Earnings,
                               price_per_Earnings_Fwd,
                               return_on_Equity,
                               return_on_Assets,
                               profit_Margin,
                               debt_per_Equity,
                               price_per_sales,
                               price_per_Cash_Flow,
                               price_per_Book,
                               bookValue_per_Share,
                               interest_Coverage,
                               market_Capitalization,
                               shares_Outstanding,
                               annual_Sales,
                               annual_Net_Income,
                               thirtysix_Month_Beta,
                               percentage_Insider_Shareholder,
                               percentage_Institutional_Shareholder
                               FROM ((((stocks_By_Alpha_Value JOIN companies ON stocks_By_Alpha_Value.company=companies.company_Tag) 
                               JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                               JOIN growth ON growth.fundamentals= Fundamentals.fundamentals_id)
                               JOIN ratios ON ratios.fundamentals=Fundamentals.fundamentals_id)")

Companies_wtd_alpha_pershare<- dbGetQuery(con,"SELECT 
                                        EPS_Growth_vs_Prev_Qtr,
                                        EPS_Growth_vs_Prev_Year,
                                        annual_Dividend_Rate,
                                        annual_Dividend_Yield,
                                        divident_Payoiut_Ratio
                                        FROM ((stocks_By_Alpha_Value JOIN companies ON stocks_By_Alpha_Value.company=companies.company_Tag) 
                                        JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                                        JOIN per_Share_Info ON per_Share_Info.fundamentals= Fundamentals.fundamentals_id")

Companies_price_vol_pershare<- dbGetQuery(con,"SELECT 
                                          EPS_Growth_vs_Prev_Qtr,
                                          EPS_Growth_vs_Prev_Year,
                                          annual_Dividend_Rate,
                                          annual_Dividend_Yield,
                                          divident_Payoiut_Ratio
                                          FROM ((stocks_By_Price_Vol JOIN companies ON stocks_By_Price_Vol.company=companies.company_Tag) 
                                          JOIN Fundamentals ON Fundamentals.company=companies.company_Tag) 
                                          JOIN per_Share_Info ON per_Share_Info.fundamentals= Fundamentals.fundamentals_id")



##------------------- Data Analysis------------------------------------------

## For Stocks by price volume
Data_Price_vol_ALL <- data.frame(lapply(Data_Price_vol_ALL, function(x) {as.numeric(gsub("N/A",0, x))}))
Companies_price_vol_Fundamentall<- data.frame(lapply(Companies_price_vol_Fundamental,function(x) {as.numeric(gsub("N/A",0, x))}))              
Companies_price_vol_Ratios<- data.frame(lapply(Companies_price_vol_Ratios, function(x) {as.numeric(gsub("N/A",0, x))}))
Companies_price_vol_Growth<- data.frame(lapply(Companies_price_vol_Growth, function(x) {as.numeric(gsub("N/A",0, x))}))

Companies_price_vol_Ratios[is.na(Companies_price_vol_Ratios)] <- 0
Data_Price_vol_ALL[is.na(Data_Price_vol_ALL)]<-0
model_1<- lm(Avg_Price~.,data=Companies_price_vol_Fundamental) ## Using Linear regression on Fundamentals Data
summary(model_1)
model_2<- lm(Avg_Price~., data=Companies_price_vol_Ratios) ## Using linear regression on Ratios Data
summary(model_2)
model_3<- lm(Avg_Price~., data=Companies_price_vol_Growth) ## Using linear regression on Growth data
summary(model_3)

model_4<- lm(Avg_Price~.,data=Data_Price_vol_ALL) ## Linear regression on combination of all three
summary(model_4)

# Applying Forward selection Technique to identify the most important main effects

model.empty <- lm(Avg_Price~1, data=Data_Price_vol_ALL) #The model with an intercept ONLY.
model.full <- lm(Avg_Price~.,data=Data_Price_vol_ALL) #The model with ALL variables.
scope <- list(lower = formula(model.empty), upper = formula(model.full))
forwardAIC = step(model.empty, scope, direction = "forward", k = 2)

# Forming linear model for the final result obtained by forward AIC
model_SSS<- lm(Avg_Price  ~ bookValue_per_Share
               + return_on_Assets 
               + shares_Outstanding 
               + market_Capitalization     
               + interest_Coverage 
               + profit_Margin 
               + price_per_sales 
               + thirtysix_Month_Beta 
               + five_year_earnings_growth
               + percentage_Insider_Shareholder
               + percentage_Institutional_Shareholder
               , data=Data_Price_vol_ALL)
summary(model_SSS)


## Repeting the above models for Stocks by Alpha value

Data_wtd_alpha_ALL <- data.frame(lapply(Data_wtd_alpha_ALL, function(x) {as.numeric(gsub("N/A",0, x))}))
Companies_wtd_alpha_Fundamental<- data.frame(lapply(Companies_wtd_alpha_Fundamental,function(x) {as.numeric(gsub("N/A",0, x))}))              
Companies_wtd_alpha_Ratios<- data.frame(lapply(Companies_wtd_alpha_Ratios, function(x) {as.numeric(gsub("N/A",0, x))}))
Companies_wtd_alpha_Growth<- data.frame(lapply(Companies_wtd_alpha_Growth, function(x) {as.numeric(gsub("N/A",0, x))}))

Companies_wtd_alpha_Ratios[is.na(Companies_wtd_alpha_Ratios)] <- 0
Data_wtd_alpha_ALL[is.na(Data_wtd_alpha_ALL)]<-0
model_1w<- lm(Avg_Price~.,data=Companies_wtd_alpha_Fundamental)
summary(model_1w)
model_2w<- lm(Avg_Price~., data=Companies_wtd_alpha_Ratios)
summary(model_2w)
model_3w<- lm(Avg_Price~., data=Companies_wtd_alpha_Growth)
summary(model_3w)
model_4w<- lm(Avg_Price~.,data=Data_wtd_alpha_ALL)
summary(model_4w)

#par(mfrow = c(2,2))                  
#plot(model_SSS)

# Forward selection Technique for wtd alpha

model.empty <- lm(Avg_Price~1, data=Data_wtd_alpha_ALL) #The model with an intercept ONLY.
model.full <- lm(Avg_Price~.,data=Data_wtd_alpha_ALL) #The model with ALL variables.
scope <- list(lower = formula(model.empty), upper = formula(model.full))
forwardAIC = step(model.empty, scope, direction = "forward", k = 2)
# Forming linear model for the final result obtained by forward AIC
model_WWW<- lm(Avg_Price ~  percentage_Institutional_Shareholder + annual_Net_Income + 
                 price_per_Cash_Flow + three_year_return + profit_Margin + 
                 price_per_Earnings_Fwd,
                 data= Data_wtd_alpha_ALL)
summary(model_WWW)

par(mfrow = c(2,2))                  
plot(model_WWW)

## Comparing the ONE YEAR AND THREE YEAR return of the two types. Only for first 50 stcoks of each category

first_fifty_pw<- data.frame(Companies_price_vol_Growth$one_year_return[1:50],Companies_price_vol_Growth$three_year_return[1:50])
first_fifty_wa<- data.frame (Companies_wtd_alpha_Growth$one_year_return[1:50],Companies_wtd_alpha_Growth$three_year_return[1:50])
fifty_fifty<-data.frame(1:nrow(first_fifty_pw),first_fifty_pw,first_fifty_wa)

ggplot(fifty_fifty, aes(X1.nrow.first_fifty_pw.),xlab="Stock",ylab="Growth") + 
  geom_line(aes(y = Companies_price_vol_Growth.one_year_return.1.50., colour = "Price Volume")) + 
  geom_line(aes(y = Companies_wtd_alpha_Growth.one_year_return.1.50., colour = "Wtd Alpha"))+
  xlab("Stocks") +
  ylab("One Year Return (%)") 
  ggtitle("Stocks % return in 1 Year")

ggplot(fifty_fifty, aes(X1.nrow.first_fifty_pw.)) + 
  geom_line(aes(y = Companies_price_vol_Growth.three_year_return.1.50., colour = "Price Volume")) + 
  geom_line(aes(y = Companies_wtd_alpha_Growth.three_year_return.1.50.,  colour = "Wtd Alpha"))+
  xlab("Stocks") +
  ylab("Three Year Return (%)")+
  ggtitle("Stocks % return in 3 Years")
  
## Comparing the EPS for the two types
val<-c(1:50)
eps_growth<- data.frame(val,Companies_wtd_alpha_pershare$EPS_Growth_vs_Prev_Qtr[1:50],
                        Companies_price_vol_pershare$EPS_Growth_vs_Prev_Qtr[1:50])
ggplot(eps_growth, aes(eps_growth$val)) + 
  geom_line(aes(y = Companies_price_vol_pershare.EPS_Growth_vs_Prev_Qtr.1.50., colour = "Price Vol"))+
  geom_line(aes(y = Companies_wtd_alpha_pershare.EPS_Growth_vs_Prev_Qtr.1.50., colour = "Wtd Alpha"))+
  xlab("Stocks") +
  ylab("EPS over Previous Quarter")+
  ggtitle("Change in Earnings Per Share")

#Analysis Conclusions:

# 1. The Percentage Institutional Shareholder and Profit Margin are the common factors across both the categories 
  #  and one should consider these parameters while investing in the stock market. 
  #  Profit margin had negative slope and Percentage institutional shareholders has positive slope.
  
# 2. The variation in stock value does not on depend only on fundamentals, growth or the ratios 
  # table but rather on the combination of all three.

# 3. Through this analysis, we can say that top 50 stocks of wtd alpha category have higher probability of 
  #  providing better returns on investments compared to the top 50 stocks of price volume category. 
  #  Also, the change in earnings per stock remains same across both the categories.

## ---------------End of R-code File--------------------------------------------------------------------------------
  
