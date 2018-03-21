library(RPostgreSQL)
library(sqldf)
library(plotly)
library(dplyr)
library(lubridate)
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,dbname="zoto",
                 host = "zoto-prod.cy2ctjb1mxfz.eu-west-1.redshift.amazonaws.com", port = 5439,
                 user = "developers", password = "apKu4iMr")



paste0("select t1.user_id,t1.t_amount,t1.this_hour,t2.prev,t1.this_hour+coalesce(t2.prev,0) total from (select user_id,count(1) this_hour,max(transaction_amount) t_amount from agg.transaction_raw where status='SA200'
and created_on between", '2017-12-11 12:00:00' and '2017-12-11 13:00:00' and json_extract_path_text(request,'service','type') in ('RECHG_TOP','RECHG','')  group by 1) t1
       left join (select user_id,count(1) prev from agg.transaction_raw where status='SA200' and created_on <
                    '2017-12-11 12:00:00' and json_extract_path_text(request,'service','type') in ('RECHG_TOP','RECHG','') group by 1) t2
       on t1.user_id=t2.user_id)

dat<-sqldf("select t1.user_id,t1.t_amount,t1.this_hour,t2.prev,t1.this_hour+coalesce(t2.prev,0) total from (select user_id,count(1) this_hour,max(transaction_amount) t_amount from agg.transaction_raw where status='SA200' and created_on between
'2017-12-11 12:00:00' and '2017-12-11 13:00:00' and json_extract_path_text(request,'service','type') in ('RECHG_TOP','RECHG','')  group by 1) t1
      left join (select user_id,count(1) prev from agg.transaction_raw where status='SA200' and created_on <
'2017-12-11 12:00:00' and json_extract_path_text(request,'service','type') in ('RECHG_TOP','RECHG','') group by 1) t2
      on t1.user_id=t2.user_id",connection=con)
dat$prob<-dat$total/sum(dat$total)
percentile<-ecdf(dat$t_amount)
dat$prob2<-dat$prob*(1+0.3*percentile(dat$t_amount))
dat$freq<-round(dat$prob2*sum(dat$total),digits=0)
draw<-rep(dat$user_id,dat$freq)
lucky<-numeric()
for (i in 1:round(0.2*nrow(dat),digits=0))
{if(i==1)
{x<-sample(draw,1,replace = F)
  lucky<-c(lucky,x)}
  if(i>1)
  {x<-sample(draw[-which(draw %in% lucky)],1,replace=F)
  lucky<-c(lucky,x)}}
#lucky<-sample(draw,0.2*nrow(dat),replace = F)

lucky_data<-dat[dat$user_id %in% lucky,]
lucky<-lucky_data$user_id
mod<-make.lp(0,length(lucky))
coff<-lucky_data$t_amount^-1
set.objfn(mod, -1*coff)
for(i in 1:length(lucky))
  {
  g<-numeric(length(lucky))
  g[c(1:length(g))]<-0
  g[i]<-1
  add.constraint(mod,g,"<=",100)
  add.constraint(mod,g,">=",5)
        g[i]<-coff[i]
        if(0.05*lucky_data$t_amount[i]<50) 
          add.constraint(mod,g,">=",0.05)
          
         add.constraint(mod,g,"<=",1)
}
g[c(1:length(g))]<-1
add.constraint(mod,g,"<=",length(lucky)*50)
solve(mod)
get.variables(mod)
lucky_data$cb<-get.variables(mod)
add.constraint(mod, c(1,0,0,0), ">=", 10)
add.constraint(mod, c(0,1,0,0), ">=", 10)
add.constraint(mod, c(0,0,1,0), ">=", 10)
add.constraint(mod, c(0,0,0,1), ">=", 10)
add.constraint(mod, c(1,1,1,1), "<=", 100)
add.constraint(mod, c(coff[1],0,0,0), "<=", 1)
add.constraint(mod, c(0,coff[2],0,0), "<=", 1)
add.constraint(mod, c(0,0,coff[3],0), "<=", 1)
add.constraint(mod, c(0,0,0,coff[4]), "<=", 1)

