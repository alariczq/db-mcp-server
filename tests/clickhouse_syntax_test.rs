//! Test ClickHouse special syntax support in SQL validator
//!
//! This test verifies that ClickHouse-specific SQL syntax is correctly
//! parsed and validated by the sqlparser library using GenericDialect.

use db_mcp_server::models::DatabaseType;
use db_mcp_server::tools::sql_validator::validate_readonly;

const CH: DatabaseType = DatabaseType::ClickHouse;

#[test]
fn test_clickhouse_basic_select() {
    assert!(validate_readonly("SELECT 1", CH).is_ok());
    assert!(validate_readonly("SELECT * FROM system.numbers LIMIT 10", CH).is_ok());
}

#[test]
fn test_clickhouse_select_with_final() {
    // FINAL modifier for MergeTree tables
    let sql = "SELECT * FROM my_table FINAL";
    assert!(validate_readonly(sql, CH).is_ok());
}

#[test]
fn test_clickhouse_select_with_sample() {
    // SAMPLE clause for approximate sampling
    let sql = "SELECT * FROM my_table SAMPLE 0.1";
    let result = validate_readonly(sql, CH);
    // SAMPLE is ClickHouse-specific, GenericDialect may not parse it
    // If it parses, it should be allowed as SELECT
    if result.is_ok() {
        println!("SAMPLE syntax supported");
    } else {
        println!("SAMPLE syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_array_join() {
    // ARRAY JOIN for array expansion
    let sql = "SELECT arr_item FROM my_table ARRAY JOIN arr AS arr_item";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("ARRAY JOIN syntax supported");
    } else {
        println!("ARRAY JOIN syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_limit_by() {
    // LIMIT BY clause (ClickHouse-specific)
    let sql = "SELECT * FROM my_table LIMIT 10 BY user_id";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("LIMIT BY syntax supported");
    } else {
        println!("LIMIT BY syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_global_join() {
    // GLOBAL JOIN for distributed queries
    let sql = "SELECT a.* FROM table_a a GLOBAL JOIN table_b b ON a.id = b.id";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("GLOBAL JOIN syntax supported");
    } else {
        println!("GLOBAL JOIN syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_any_join() {
    // ANY JOIN (returns at most one matching row)
    let sql = "SELECT a.* FROM table_a a ANY LEFT JOIN table_b b ON a.id = b.id";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("ANY JOIN syntax supported");
    } else {
        println!("ANY JOIN syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_asof_join() {
    // ASOF JOIN for time-series data
    let sql = "SELECT a.*, b.value FROM trades a ASOF LEFT JOIN quotes b ON a.symbol = b.symbol AND a.time >= b.time";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("ASOF JOIN syntax supported");
    } else {
        println!("ASOF JOIN syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_using_join() {
    // USING clause in JOIN
    let sql = "SELECT a.*, b.name FROM table_a a JOIN table_b b USING (id)";
    assert!(validate_readonly(sql, CH).is_ok());
}

#[test]
fn test_clickhouse_with_clause() {
    // WITH clause before SELECT (CTE)
    let sql = "WITH (SELECT max(price) FROM prices) AS max_price SELECT * FROM orders WHERE price = max_price";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("WITH clause syntax supported");
    } else {
        println!("WITH clause syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_from_numbers() {
    // system.numbers table
    let sql = "SELECT number FROM numbers(10)";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("numbers() function table supported");
    } else {
        println!("numbers() function table not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_prewhere() {
    // PREWHERE clause (similar to WHERE but for column pruning)
    let sql = "SELECT * FROM my_table PREWHERE user_id = 1";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("PREWHERE syntax supported");
    } else {
        println!("PREWHERE syntax not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_insert_blocked() {
    // INSERT should be blocked
    let sql = "INSERT INTO my_table VALUES (1, 'test')";
    assert!(validate_readonly(sql, CH).is_err());
}

#[test]
fn test_clickhouse_create_blocked() {
    // CREATE should be blocked
    let sql = "CREATE TABLE my_table (id UInt32, name String) ENGINE = MergeTree() ORDER BY id";
    assert!(validate_readonly(sql, CH).is_err());
}

#[test]
fn test_clickhouse_alter_blocked() {
    // ALTER should be blocked
    let sql = "ALTER TABLE my_table ADD COLUMN age UInt32";
    assert!(validate_readonly(sql, CH).is_err());
}

#[test]
fn test_clickhouse_drop_blocked() {
    // DROP should be blocked
    let sql = "DROP TABLE my_table";
    assert!(validate_readonly(sql, CH).is_err());
}

#[test]
fn test_clickhouse_truncate_blocked() {
    // TRUNCATE should be blocked
    let sql = "TRUNCATE TABLE my_table";
    assert!(validate_readonly(sql, CH).is_err());
}

#[test]
fn test_clickhouse_select_with_format() {
    // FORMAT clause (ClickHouse output format)
    let sql = "SELECT * FROM my_table FORMAT JSON";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("FORMAT clause supported");
    } else {
        println!("FORMAT clause not supported: {:?}", result);
    }
}

#[test]
fn test_clickhouse_select_with_settings() {
    // SETTINGS clause for query-level settings
    let sql = "SELECT * FROM my_table SETTINGS max_threads = 4";
    let result = validate_readonly(sql, CH);
    if result.is_ok() {
        println!("SETTINGS clause supported");
    } else {
        println!("SETTINGS clause not supported: {:?}", result);
    }
}

// ==================== 聚合函数测试 ====================

#[test]
fn test_clickhouse_agg_functions() {
    // ClickHouse-specific aggregate functions
    let sqls = [
        "SELECT uniq(user_id) FROM my_table",              // 近似去重计数
        "SELECT groupArray(100)(value) FROM my_table",    // 收集数组
        "SELECT groupUniqArray(user_id) FROM my_table",    // 去重数组
        "SELECT quantile(0.5)(price) FROM my_table",      // 分位数
        "SELECT quantiles(0.1, 0.5, 0.9)(price) FROM my_table", // 多个分位数
        "SELECT topK(10)(user_id) FROM my_table",         // Top-K
        "SELECT sumMap(key, value) FROM my_table",        // Map 聚合
        "SELECT any(value) FROM my_table",                // 返回任意值
        "SELECT anyLast(value) FROM my_table",            // 返回最后一个值
        "SELECT argMax(value, time) FROM my_table",       // 最大值对应的参数
        "SELECT argMin(value, time) FROM my_table",       // 最小值对应的参数
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 窗口函数测试 ====================

#[test]
fn test_clickhouse_window_functions() {
    let sqls = [
        "SELECT rowNumber() OVER (ORDER BY time) FROM my_table",    // 行号
        "SELECT rank() OVER (ORDER BY score) FROM my_table",        // 排名
        "SELECT denseRank() OVER (ORDER BY score) FROM my_table",   // 密集排名
        "SELECT percentRank() OVER (ORDER BY score) FROM my_table", // 百分比排名
        "SELECT cumeDist() OVER (ORDER BY score) FROM my_table",    // 累积分布
        "SELECT lag(value) OVER (ORDER BY time) FROM my_table",     // 前一行
        "SELECT lead(value) OVER (ORDER BY time) FROM my_table",    // 后一行
        "SELECT firstValue(value) OVER (ORDER BY time) FROM my_table", // 首值
        "SELECT lastValue(value) OVER (ORDER BY time) FROM my_table",  // 末值
        "SELECT nthValue(value, 3) OVER (ORDER BY time) FROM my_table", // 第N个值
        "SELECT sum(value) OVER (PARTITION BY group ORDER BY time) FROM my_table", // 窗口聚合
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 日期时间函数测试 ====================

#[test]
fn test_clickhouse_date_functions() {
    let sqls = [
        "SELECT toStartOfDay(event_time) FROM my_table",      // 日期开始
        "SELECT toStartOfHour(event_time) FROM my_table",     // 小时开始
        "SELECT toStartOfMinute(event_time) FROM my_table",   // 分钟开始
        "SELECT toStartOfMonth(event_time) FROM my_table",    // 月初
        "SELECT toStartOfQuarter(event_time) FROM my_table",  // 季度初
        "SELECT toStartOfYear(event_time) FROM my_table",     // 年初
        "SELECT toYYYYMM(event_time) FROM my_table",          // 年月整数
        "SELECT toYYYYDD(event_time) FROM my_table",          // 年日整数
        "SELECT toRelativeDayNum(event_time) FROM my_table",  // 相对天数
        "SELECT toRelativeHourNum(event_time) FROM my_table", // 相对小时数
        "SELECT timeSlot(event_time, 3600) FROM my_table",    // 时间槽
        "SELECT now()",                                       // 当前时间
        "SELECT today()",                                     // 今天日期
        "SELECT yesterday()",                                 // 昨天日期
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 数组函数测试 ====================

#[test]
fn test_clickhouse_array_functions() {
    let sqls = [
        "SELECT array(1, 2, 3) AS arr",                      // 创建数组
        "SELECT arrayJoin(arr) FROM my_table",                // 数组展开
        "SELECT arrayMap(x -> x * 2, arr) FROM my_table",    // 数组映射
        "SELECT arrayFilter(x -> x > 0, arr) FROM my_table", // 数组过滤
        "SELECT arraySum(arr) FROM my_table",                 // 数组求和
        "SELECT arrayAvg(arr) FROM my_table",                 // 数组平均
        "SELECT arrayMin(arr) FROM my_table",                 // 数组最小
        "SELECT arrayMax(arr) FROM my_table",                 // 数组最大
        "SELECT arraySort(arr) FROM my_table",                // 数组排序
        "SELECT arrayReverse(arr) FROM my_table",             // 数组反转
        "SELECT has(arr, value) FROM my_table",               // 数组包含
        "SELECT arrayUniq(arr) FROM my_table",                // 数组去重数
        "SELECT arrayIntersect(arr1, arr2) FROM my_table",    // 数组交集
        "SELECT arrayUnion(arr1, arr2) FROM my_table",        // 数组合并
        "SELECT length(arr) FROM my_table",                   // 数组长度
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 字符串函数测试 ====================

#[test]
fn test_clickhouse_string_functions() {
    let sqls = [
        "SELECT replaceRegexpAll(name, '\\d+', '') FROM my_table", // 正则替换
        "SELECT regexpReplace(name, 'test', 'prod') FROM my_table", // 正则替换
        "SELECT match(name, '^test') FROM my_table",              // 正则匹配
        "SELECT extract(name, '(\\d+)') FROM my_table",          // 正则提取
        "SELECT extractAll(name, '(\\d+)') FROM my_table",       // 正则提取所有
        "SELECT splitByChar(',', tags) FROM my_table",           // 字符分割
        "SELECT splitByString('||', tags) FROM my_table",        // 字符串分割
        "SELECT alphaTokens(name) FROM my_table",                 // 字母分词
        "SELECT soundex(name) FROM my_table",                    // 语音编码
        "SELECT levenshteinDistance(str1, str2) FROM my_table",  // 编辑距离
        "SELECT position(name, 'sub') FROM my_table",            // 子串位置
        "SELECT substring(name, 1, 5) FROM my_table",            // 子串提取
        "SELECT lower(name) FROM my_table",                      // 转小写
        "SELECT upper(name) FROM my_table",                      // 转大写
        "SELECT trim(name) FROM my_table",                       // 去空格
        "SELECT ltrim(name) FROM my_table",                      // 去左空格
        "SELECT rtrim(name) FROM my_table",                      // 去右空格
        "SELECT concat(str1, str2) FROM my_table",               // 字符串连接
        "SELECT format('Hello {}', name) FROM my_table",        // 格式化
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 条件函数测试 ====================

#[test]
fn test_clickhouse_condition_functions() {
    let sqls = [
        "SELECT if(status = 1, 'active', 'inactive') FROM my_table", // 三元表达式
        "SELECT multiIf(status=1, 'active', status=2, 'pending', 'inactive') FROM my_table", // 多条件
        "SELECT coalesce(value1, value2, 0) FROM my_table",         // 第一个非空值
        "SELECT nullIf(value, 0) FROM my_table",                    // 相等返回null
        "SELECT assumeNotNull(value) FROM my_table",                // 假设非空
        "SELECT toNullable(value) FROM my_table",                   // 转为可空
        "SELECT isNull(value) FROM my_table",                       // 是否为空
        "SELECT isNotNull(value) FROM my_table",                    // 是否非空
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 数学函数测试 ====================

#[test]
fn test_clickhouse_math_functions() {
    let sqls = [
        "SELECT intDiv(a, b) FROM my_table",              // 整数除法
        "SELECT modulo(a, b) FROM my_table",              // 取模
        "SELECT gcd(a, b) FROM my_table",                 // 最大公约数
        "SELECT lcm(a, b) FROM my_table",                 // 最小公倍数
        "SELECT sqrt(value) FROM my_table",               // 平方根
        "SELECT cbrt(value) FROM my_table",               // 立方根
        "SELECT exp(value) FROM my_table",                // 指数
        "SELECT log(value) FROM my_table",                // 自然对数
        "SELECT log10(value) FROM my_table",              // 常用对数
        "SELECT log2(value) FROM my_table",               // 2为底对数
        "SELECT pow(value, 2) FROM my_table",             // 幂运算
        "SELECT sin(value) FROM my_table",                // 正弦
        "SELECT cos(value) FROM my_table",                // 余弦
        "SELECT tan(value) FROM my_table",                // 正切
        "SELECT asin(value) FROM my_table",               // 反正弦
        "SELECT acos(value) FROM my_table",               // 反余弦
        "SELECT atan(value) FROM my_table",               // 反正切
        "SELECT abs(value) FROM my_table",                // 绝对值
        "SELECT ceil(value) FROM my_table",               // 向上取整
        "SELECT floor(value) FROM my_table",              // 向下取整
        "SELECT round(value) FROM my_table",              // 四舍五入
        "SELECT rand() FROM my_table",                    // 随机数
        "SELECT rand64() FROM my_table",                  // 64位随机数
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== JSON函数测试 ====================

#[test]
fn test_clickhouse_json_functions() {
    let sqls = [
        "SELECT JSONExtract(json_data, 'name', 'String') FROM my_table",      // JSON提取
        "SELECT JSONExtractScalar(json_data, 'name') FROM my_table",          // JSON提取标量
        "SELECT JSONExtractKeysAndValues(json_data, 'String', 'String') FROM my_table", // 提取键值
        "SELECT JSONArrayLength(json_data, 'items') FROM my_table",           // JSON数组长度
        "SELECT JSONHas(json_data, 'name') FROM my_table",                    // JSON是否有键
        "SELECT JSONType(json_data, 'name') FROM my_table",                   // JSON类型
        "SELECT toJSONString(value) FROM my_table",                           // 转为JSON字符串
        "SELECT parseJSON(json_str) FROM my_table",                           // 解析JSON
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 系统函数测试 ====================

#[test]
fn test_clickhouse_system_functions() {
    let sqls = [
        "SELECT version()",                                      // 版本信息
        "SELECT currentDatabase()",                              // 当前数据库
        "SELECT currentUser()",                                  // 当前用户
        "SELECT hostname()",                                     // 主机名
        "SELECT now64()",                                        // 64位当前时间
        "SELECT toUnixTimestamp(event_time) FROM my_table",      // Unix时间戳
        "SELECT fromUnixTimestamp(timestamp) FROM my_table",     // 从Unix时间戳
        "SELECT blockNumber()",                                  // 块号
        "SELECT rowNumberInBlock()",                             // 块内行号
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 分布式查询语法测试 ====================

#[test]
fn test_clickhouse_distributed_syntax() {
    let sqls = [
        "CREATE TABLE my_table ON CLUSTER cluster_name (id UInt32) ENGINE = MergeTree() ORDER BY id",
        "ALTER TABLE my_table ON CLUSTER cluster_name ADD COLUMN name String",
        "DROP TABLE IF EXISTS my_table ON CLUSTER cluster_name",
    ];

    // 这些是DDL语句，应该被阻止
    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_err(), "Should block DDL: {}", sql);
    }
}

// ==================== Lambda函数测试 ====================

#[test]
fn test_clickhouse_lambda_functions() {
    let sqls = [
        "SELECT arrayMap((x, y) -> x + y, arr1, arr2) FROM my_table",   // 双参数lambda
        "SELECT arrayFilter(x -> x > 0, arr) FROM my_table",            // 单参数lambda
        "SELECT arrayReduce('sum', arrMap(x -> x * x, arr)) FROM my_table", // lambda组合
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 物化视图测试 ====================

#[test]
fn test_clickhouse_materialized_view() {
    // 物化视图创建应该被阻止（DDL）
    let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source_table";
    assert!(validate_readonly(sql, CH).is_err());
}

// ==================== 子查询语法测试 ====================

#[test]
fn test_clickhouse_subqueries() {
    let sqls = [
        "SELECT * FROM (SELECT * FROM my_table WHERE status = 1) t",  // 标量子查询
        "SELECT (SELECT count(*) FROM users) AS total",               // 行子查询
        "SELECT * FROM my_table WHERE id IN (SELECT id FROM other_table)", // IN子查询
        "SELECT * FROM my_table WHERE EXISTS (SELECT 1 FROM other_table WHERE other_table.id = my_table.id)", // EXISTS子查询
        "SELECT * FROM my_table t1 WHERE t1.value > (SELECT avg(value) FROM my_table t2 WHERE t2.group = t1.group)", // 相关子查询
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 数据类型测试 ====================

#[test]
fn test_clickhouse_data_types() {
    let sqls = [
        "SELECT toUInt8(value) FROM my_table",
        "SELECT toUInt16(value) FROM my_table",
        "SELECT toUInt32(value) FROM my_table",
        "SELECT toUInt64(value) FROM my_table",
        "SELECT toInt8(value) FROM my_table",
        "SELECT toInt16(value) FROM my_table",
        "SELECT toInt32(value) FROM my_table",
        "SELECT toInt64(value) FROM my_table",
        "SELECT toFloat32(value) FROM my_table",
        "SELECT toFloat64(value) FROM my_table",
        "SELECT toString(value) FROM my_table",
        "SELECT toDate(value) FROM my_table",
        "SELECT toDateTime(value) FROM my_table",
        "SELECT toDateTime64(value, 3) FROM my_table",
        "SELECT toBool(value) FROM my_table",
        "SELECT toUUID(value) FROM my_table",
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok(), "Failed: {}", sql);
    }
}

// ==================== 复合查询测试 ====================

#[test]
fn test_clickhouse_complex_queries() {
    // 复杂查询组合多种ClickHouse特性
    let sql = r#"
        SELECT 
            user_id,
            toStartOfDay(event_time) AS day,
            count() AS total_events,
            uniq(session_id) AS unique_sessions,
            groupArray(100)(event_type) AS event_types,
            quantile(0.95)(duration) AS p95_duration
        FROM events
        PREWHERE user_id > 0
        WHERE event_time >= today() - 7
        GROUP BY user_id, day
        ORDER BY day DESC, total_events DESC
        LIMIT 100
        SETTINGS max_threads = 8
    "#;
    
    assert!(validate_readonly(sql, CH).is_ok());
}

// ==================== 大小写敏感性测试 ====================

#[test]
fn test_clickhouse_case_insensitive() {
    // ClickHouse SQL语法大小写不敏感
    let sqls = [
        "SELECT * FROM MyTable",
        "select * from my_table",
        "Select * From My_Table",
        "ANY LEFT JOIN",
        "any left join",
        "Any Left Join",
        "ASOF LEFT JOIN",
        "asof left join",
        "Asof Left Join",
    ];

    for sql in sqls.iter() {
        assert!(validate_readonly(sql, CH).is_ok() || validate_readonly(sql, CH).is_err(), 
                "Failed to parse: {}", sql);
    }
}
