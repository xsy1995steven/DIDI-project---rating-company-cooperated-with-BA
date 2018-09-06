if [ x$1 == x ] ; then
        V_DATE=`date -d '-1 day' "+%Y%m%d" `
else
        V_DATE=$1
fi
V_DATE_=`date -d "0 day ago $V_DATE" +%Y-%m-%d`
V_DATE90=`date -d "90 day ago $V_DATE" +%Y%m%d`
V_DATE90_=`date -d "90 day ago $V_DATE" +%Y-%m-%d`
V_DATE60=`date -d "60 day ago $V_DATE" +%Y%m%d`
V_DATE60_=`date -d "60 day ago $V_DATE" +%Y-%m-%d`
V_DATE30=`date -d "30 day ago $V_DATE" +%Y%m%d`
V_DATE30_=`date -d "30 day ago $V_DATE" +%Y-%m-%d`
echo $V_DATE_
echo $V_DATE


#数据仓库数据层级 dwd:数据明细层 csum:数据汇总层 ads:数据应用层 dim:数据维度 rpt:数据报表层
LEVER="ads"
#数据库：am_dw：后市场数据仓库 atsplch_rpt:后市场数据报表库
DATA_BASE="am_dw"
#表名，各层表名规定：数据层级_业务主题_数据表名
TABLE_NAME="ads_zs_cp_score_result"
#数据存储路径前缀
DATA_PATH_PREFIX="/user/supply_chain/data/dw/${LEVER}"
#数据存储路径
V_PATH_DIR=${DATA_PATH_PREFIX}/${TABLE_NAME}/'dt'=${V_DATE}

# 程序路径 /user/supply_chain/program/ds3/ads/ads_zs_cp_score_result/ads_zs_cp_score_result.sh
# 数据路径 /user/supply_chain/data/dw/ads/ads_zs_cp_score_result

create_sql="
set mapred.job.queue.name=root.kuaicheshiyebu-houshichangyewuxian.amdwdev;
CREATE EXTERNAL TABLE  am_dw.ads_zs_cp_score_result (
company_id           string COMMENT 'company的id',
company_name         string COMMENT 'cp名字',
city_id                  string COMMENT 'cp所在城市id',
city_name               string COMMENT 'cp所在城市名字',
valid_offer_ratio           string COMMENT '近一个月有效方案占比',
offer_num     string COMMENT '上架方案数',
avg_fetch_period     string COMMENT '提车周期',
gmv_cp           string COMMENT '当月交易GMV',
remit_ratio string COMMENT '代扣代缴车辆占比',
order_city_ratio    string COMMENT '月日均交易量环比城市整体环比',
order_num       string COMMENT '当月订单交易量',
num_withdraw     string COMMENT '近一个月异常退车辆',
num_car_rent    string COMMENT '当月在租车辆数',
avg_contact_period                string COMMENT 'leads跟进周期',
rate         string COMMENT 'cp评分'
)
COMMENT '租售CP打分结果'
PARTITIONED BY (dt string)
row format delimited 
fields terminated by '\t'
stored AS TEXTFILE 
LOCATION
'/user/supply_chain/data/dw/ads/ads_zs_cp_score_result'
;"

tmp_table_sql="
drop table am_temp.dwd_cp_info_rate;
set mapred.job.queue.name=root.kuaicheshiyebu-houshichangyewuxian.amdwdev; 
create table am_temp.dwd_cp_info_rate as
select 
    order_id,
    order_time,
    status,
    gmv,
    city_id,
    fetch_period,
    end_lease_type,
    actual_end_time,
    is_remit,
    offer_type,
    contact_period,
    company_id
from 
    am_dw.dwd_order_rent_base
where 
    dt='${V_DATE}' and 
    substr(order_time,1,10)>'${V_DATE30_}';
"
echo ${tmp_table_sql}
hive -e "${tmp_table_sql}"


get_cp_features="
set mapred.job.queue.name=root.kuaicheshiyebu-houshichangyewuxian.amdwdev; 
select 
    table1.company_id as company_id,
    table1.company_name as company_name,
    table1.city_id as city_id,
    table1.city_name as city_name,
    table2.valid_offer_ratio as valid_offer_ratio,
    table3.offer_num as offer_num,
    table4.avg_fetch_period as avg_fetch_period,
    table5.gmv_cp as gmv_cp,
    table6.num_remit*1.00/table9.num_car_rent as remit_ratio,
    table7.order_city_ratio as order_city_ratio, 
    table7.order_num as order_num,
    table8.num_withdraw as num_withdraw,
    table9.num_car_rent as num_car_rent,
    table10.avg_contact_period as avg_contact_period
from
    (select
        distinct 
        company_id,
        company_name,
        city_name,
        city_id 
    from 
        am_dw.dwd_rent_company_base  
    where 
        dt='${V_DATE}'
    ) table1
left join 
    (
    select 
        (num_valid_offer*1.00/num_offer) as valid_offer_ratio,
        a.company_id as company_id
    from
        (
        select 
            count(distinct offer_id) as num_valid_offer,
            company_id
        from 
            am_dw.dwd_rent_offer_base
        where 
            dt='${V_DATE}' and
            offer_state = 1
        group by 
            company_id
        )a
        left join 
        (
        select 
            count(distinct offer_id) as num_offer,
            company_id
        from 
            am_dw.dwd_rent_offer_base
        where 
            dt='${V_DATE}'
        group by 
            company_id
        )b
        on a.company_id = b.company_id
    )table2 on table1.company_id = table2.company_id
left join
    (
    select 
        distinct 
        company_id, 
        offer_num 
    from 
        am_dw.dwd_rent_company_base
    where 
    dt = '${V_DATE}'
    )table3 on table1.company_id = table3.company_id
left join
    (
    select 
        avg(fetch_period) as avg_fetch_period,
        company_id
    from 
        am_temp.dwd_cp_info_rate
    group by 
        company_id
    )table4 on table1.company_id = table4.company_id
left join
    (
    select 
        sum(gmv) as gmv_cp,
        company_id
    from 
        am_temp.dwd_cp_info_rate
    where 
        status <> 120 and 
        status <> 130
    group by 
        company_id
    ) table5 on table1.company_id = table5.company_id
left join
    (
    select 
        count(distinct car_id) as num_remit,
        company_id
    from 
        am_dw.dwd_order_rent_base
    where 
        is_remit=1 and
        status=60 
    group by 
        company_id
    ) table6 on table1.company_id = table6.company_id
left join
    (
    select 
    (order_num*1.00/order_num_last_month)/(order_num_city*1.00/order_num_city_last_month) as order_city_ratio,c.company_id as company_id,c.order_num as order_num
    from
        (
        select 
        count(distinct order_id) as order_num,company_id
        from am_temp.dwd_cp_info_rate_0724
        group by company_id
        )c
    left join
        (
        select
        count(distinct order_id) as order_num_last_month,company_id
        from am_dw.dwd_order_rent_base
        where dt='${V_DATE}' and substr(order_time,1,10)>'${V_DATE60_}' and substr(order_time,1,10)<='${V_DATE30_}'
        group by company_id
        )cc
    on c.company_id = cc.company_id
    left join
        (
        select
        distinct company_id, city_id
        from am_temp.dwd_cp_info_rate_0724
        order by company_id
        )d
    on c.company_id = d.company_id
    left join
        (
        select 
        count(distinct order_id) as order_num_city,city_id
        from am_temp.dwd_cp_info_rate_0724
        group by city_id
        )e
    on d.city_id=e.city_id
    left join
        (
        select
        count(distinct order_id) as order_num_city_last_month,city_id
        from am_dw.dwd_order_rent_base
        where dt='${V_DATE}' and substr(order_time,1,10)>'${V_DATE60_}' and substr(order_time,1,10)<='${V_DATE30_}'
        group by city_id
        )ee
    on d.city_id = ee.city_id
    ) table7 on table1.company_id = table7.company_id
left join
    (
    select
        count(distinct order_id) as num_withdraw,
        company_id
    from 
        am_dw.dwd_order_rent_base
    where 
        substr(actual_end_time,1,10) > '${V_DATE30_}' and
        end_lease_type = 2
    group by company_id
    ) table8 on table1.company_id = table8.company_id
left join
    (
    select 
        count(distinct car_id) as num_car_rent,
        company_id
    from 
        am_dw.dwd_order_rent_base
    where 
        status=60 
    group by 
        company_id
    ) table9 on table1.company_id = table9.company_id
left join
    (
    select 
        avg(contact_period) as avg_contact_period,
        company_id
    from 
        am_temp.dwd_cp_info_rate
    group by 
        company_id
    ) table10 on table1.company_id = table10.company_id
;"

echo ${get_cp_features}
hive -e "${get_cp_features}">dwd_rent_cp_rate.csv
python ads_zs_cp_score_result.py

load_data="
ALTER TABLE am_dw.ads_zs_cp_score_result DROP IF EXISTS PARTITION(dt='${V_DATE}');
load data local inpath 'ads_zs_cp_score_result.csv' overwrite into table am_dw.ads_zs_cp_score_result partition(dt='${V_DATE}'); 
"

echo ${load_data}
hive -e "${load_data}"



line_num=`$HADOOP_HOME/bin/hadoop fs -cat  ${V_PATH_DIR}/* | wc -l`

echo ${line_num}
if [ $line_num -eq 0 ]
    then
        echo "FATAL  $V_PATH_DIR is empty"
    else
        echo "NOTICE  success"
        $HADOOP_HOME/bin/hadoop fs -touchz ${V_PATH_DIR}/_SUCCESS
      
fi

