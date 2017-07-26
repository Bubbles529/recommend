library(data.table)
library(parallel)
ptm<-proc.time()
ratings<-fread("ratings.csv")
k<-c(5)
ratings<-ratings[,.(user_id,movie_id)]
ratings<-cbind(ratings,group_id=rep(1:8,len=nrow(ratings)))
recall_sum<-0
precision_sum<-0
cl <- parallel::makeCluster(4)
for(i in c(1:8)){
  testData<-ratings[group_id==1,.(user_id,movie_id)]
  trainData<-ratings[group_id!=1,.(user_id,movie_id)]
  #数据预处理
  train_count<-trainData[,.(sqrt=sqrt(.N)),by=user_id]
  #计算每个用户
  user_id <- unique(testData[,user_id])
  result<-parLapply(cl,user_id,function(user){
    test_movie<-testData[user_id==user,movie_id]#测试集
    user_movie<-trainData[user_id==user,movie_id]#用户喜欢
    train_rate<-trainData[movie_id %in% user_movie,.N,by=user_id]#与训练集交集个数
    
    relevantUser<-train_rate[train_count,on="user_id"][order(-N/sqrt)][2:k+1,user_id]#前K个相关用户
    recommend_movie<-unique(trainData[user_id %in% relevantUser,movie_id])
    recommend_movie<-setdiff(recommend_movie,user_movie)#推荐的电影
    inter_movie_count<-length(intersect(recommend_movie,test_movie))#正确推荐
    c(inter_movie_count,length(recommend_movie))
  })
  stopCluster(cl)
  recall_sum<-recall_sum+sum(result[1,])/nrow(testData)#召回率
  precision_sum<-precision_sum+sum(result[1,])/sum(result[2,])#准确率
  print(recall_sum/i)
  print(precision_sum/i)
  print(proc.time()-ptm)
}
