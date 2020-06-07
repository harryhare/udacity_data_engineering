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

* 3 data warehorse（redshift - postgresql）

* 4 data lakes (spark): 在 `/projects/4_spark`
	
	- 尝试在 aws 的 emr 上跑，但是太慢了，所以后面就改为在 workspace 中运行了
	- 另外aws-emr 上以sql 的方式执行sql 查询会 报错，没有解决
	

* 5 pipeline(airflow)  在 github 上

https://github.com/harryhare/airflow_playground

*  capstone 在 github 上

https://github.com/harryhare/udacity_data_engineer_capstone_project
	