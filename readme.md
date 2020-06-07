# udacity data engineering nano degree program 笔记

## intro

这个 repo 是 udacity data engineering 课程的笔记

## basics

* 运行 jupyter notebook
	```bash
	python -m jupyter notebook
	```


* 运行 postgresql

	```bash
	psql -U postgres -p 5434
	```
	
	
## projects

* 1 data model postgres `/projects/1_data_model_postgres`

* 2 data model Cassandra `/projects/2_data_model_cassandra`

* 3 data warehorse（redshift - postgresql）`/projects/3_data_warehouse`

* 4 data lakes (spark): `/projects/4_data_lake`
	
	- 尝试在 aws 的 emr 上跑，但是太慢了，所以后面就改为在 workspace 中运行了
	- 另外aws-emr 上以sql 的方式执行sql 查询会 报错，没有解决
	

* 5 pipeline(airflow)  在 github 上

https://github.com/harryhare/udacity_data_engineering_pipeline

*  capstone 在 github 上

https://github.com/harryhare/udacity_data_engineering_capstone_project


## 其他

* 删掉不慎提交的包含密码的文件
	```bash
	# 删除包括历史
	git filter-branch --force --index-filter 'git rm --cached --ignore-unmatch 文件相对路径' --prune-empty --tag-name-filter cat -- --all
	# 同步到远程
	git push origin master --force
	```

	