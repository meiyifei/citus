# get_shard_id_for_distribution_column函数

### 实现原理

```c
/*
	函数位置:citus-9.1.0/src/backend/distributed/metadata/node_metadata.c

*/
PG_FUNCTION_INFO_V1(get_shard_id_for_distribution_column);
/*
 * get_shard_id_for_distribution_column function takes a distributed table name and a
 * distribution value then returns shard id of the shard which belongs to given table and
 * contains given value. This function only works for hash distributed tables.
 */
Datum
get_shard_id_for_distribution_column(PG_FUNCTION_ARGS)
{
        ShardInterval *shardInterval = NULL;

        CheckCitusVersion(ERROR);

        /*
         * To have optional parameter as NULL, we defined this UDF as not strict, therefore
         * we need to check all parameters for NULL values.
         */
        if (PG_ARGISNULL(0))
        {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                                errmsg("relation cannot be NULL")));
        }

        Oid relationId = PG_GETARG_OID(0);
        EnsureTablePermissions(relationId, ACL_SELECT);

        if (!IsDistributedTable(relationId))
        {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                                errmsg("relation is not distributed")));
        }

        char distributionMethod = PartitionMethod(relationId);
        if (distributionMethod == DISTRIBUTE_BY_NONE)
        {
                List *shardIntervalList = LoadShardIntervalList(relationId);
                if (shardIntervalList == NIL)
                {
                        PG_RETURN_INT64(0);
                }

                shardInterval = (ShardInterval *) linitial(shardIntervalList);
        }
        else if (distributionMethod == DISTRIBUTE_BY_HASH ||
                         distributionMethod == DISTRIBUTE_BY_RANGE)
        {
                DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);

                /* if given table is not reference table, distributionValue cannot be NULL */
                if (PG_ARGISNULL(1))
                {
                        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                                        errmsg("distribution value cannot be NULL for tables other "
                                                                   "than reference tables.")));
                }

                Datum inputDatum = PG_GETARG_DATUM(1);
                Oid inputDataType = get_fn_expr_argtype(fcinfo->flinfo, 1);
                char *distributionValueString = DatumToString(inputDatum, inputDataType);

                Var *distributionColumn = DistPartitionKey(relationId);
                Oid distributionDataType = distributionColumn->vartype;

                Datum distributionValueDatum = StringToDatum(distributionValueString,
                                                                                                         distributionDataType);

                shardInterval = FindShardInterval(distributionValueDatum, cacheEntry);
        }
        else
        {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                errmsg("finding shard id of given distribution value is only "
                                                           "supported for hash partitioned tables, range partitioned "
                                                           "tables and reference tables.")));
        }

        if (shardInterval != NULL)
        {
                PG_RETURN_INT64(shardInterval->shardId);
        }

        PG_RETURN_INT64(0);
    
   -- 最终实现的函数FindShardInterval\FindShardIntervalIndex来处理
/*
	函数位置:citus-9.1.0/src/backend/distributed/utils/shardinterval_utils.c
*/
/*
 * FindShardInterval finds a single shard interval in the cache for the
 * given partition column value. Note that reference tables do not have
 * partition columns, thus, pass partitionColumnValue and compareFunction
 * as NULL for them.
 */
ShardInterval *
FindShardInterval(Datum partitionColumnValue, DistTableCacheEntry *cacheEntry)
{
        Datum searchedValue = partitionColumnValue;

        if (cacheEntry->partitionMethod == DISTRIBUTE_BY_HASH)
        {
                searchedValue = FunctionCall1Coll(cacheEntry->hashFunction,
                                                                                  cacheEntry->partitionColumn->varcollid,
                                                                                  partitionColumnValue);
        }

        int shardIndex = FindShardIntervalIndex(searchedValue, cacheEntry);

        if (shardIndex == INVALID_SHARD_INDEX)
        {
                return NULL;
        }

        return cacheEntry->sortedShardIntervalArray[shardIndex];
}
    
 /*
 * FindShardIntervalIndex finds the index of the shard interval which covers
 * the searched value. Note that the searched value must be the hashed value
 * of the original value if the distribution method is hash.
 *
 * Note that, if the searched value can not be found for hash partitioned
 * tables, we error out (unless there are no shards, in which case
 * INVALID_SHARD_INDEX is returned). This should only happen if something is
 * terribly wrong, either metadata tables are corrupted or we have a bug
 * somewhere. Such as a hash function which returns a value not in the range
 * of [INT32_MIN, INT32_MAX] can fire this.
 */
   
int
FindShardIntervalIndex(Datum searchedValue, DistTableCacheEntry *cacheEntry)
{
        ShardInterval **shardIntervalCache = cacheEntry->sortedShardIntervalArray;
        int shardCount = cacheEntry->shardIntervalArrayLength;
        char partitionMethod = cacheEntry->partitionMethod;
        FmgrInfo *compareFunction = cacheEntry->shardIntervalCompareFunction;
        bool useBinarySearch = (partitionMethod != DISTRIBUTE_BY_HASH ||
                                                        !cacheEntry->hasUniformHashDistribution);
        int shardIndex = INVALID_SHARD_INDEX;

        if (shardCount == 0)
        {
                return INVALID_SHARD_INDEX;
        }

        if (partitionMethod == DISTRIBUTE_BY_HASH)
        {
                if (useBinarySearch)
                {
                        Assert(compareFunction != NULL);

                        shardIndex = SearchCachedShardInterval(searchedValue, shardIntervalCache,
                                                                                                   shardCount, compareFunction);

                        /* we should always return a valid shard index for hash partitioned tables */
                        if (shardIndex == INVALID_SHARD_INDEX)
                        {
                                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                                                                errmsg("cannot find shard interval"),
                                                                errdetail("Hash of the partition column value "
                                                                                  "does not fall into any shards.")));
                        }
                }
                else
                {
                        int hashedValue = DatumGetInt32(searchedValue);
                        uint64 hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

                        shardIndex = (uint32) (hashedValue - INT32_MIN) / hashTokenIncrement;
                        Assert(shardIndex <= shardCount);

                        /*
                         * If the shard count is not power of 2, the range of the last
                         * shard becomes larger than others. For that extra piece of range,
                         * we still need to use the last shard.
                         */
                        if (shardIndex == shardCount)
                        {
                                shardIndex = shardCount - 1;
                        }
                }
        }
        else if (partitionMethod == DISTRIBUTE_BY_NONE)
        {
                /* reference tables has a single shard, all values mapped to that shard */
                Assert(shardCount == 1);

                shardIndex = 0;
        }
        else
        {
                Assert(compareFunction != NULL);

                shardIndex = SearchCachedShardInterval(searchedValue, shardIntervalCache,
                                                                                           shardCount, compareFunction);
        }

        return shardIndex;
}


/*       
思想:
	1、根据输入参数及其类型，得到hash值
		searchedValue = FunctionCall1Coll(cacheEntry->hashFunction,cacheEntry->partitionColumn-			>varcollid, partitionColumnValue);                                                         
			
	2、根据分布表的分片数量，得到每个分片区间大小
		uint64 hashTokenIncrement = HASH_TOKEN_COUNT/shardCount;
		
		根据master_metadata_utility.h可知HASH_TOKEN_COUNT=2^32
		hash范围:[-2^31,2^31-1]
		#define HASH_TOKEN_COUNT INT64CONST(4294967296)
		
	3、根据得到的hash值，确定位于那个分片
		shardIndex = (uint32) (hashedValue - INT32_MIN) / hashTokenIncrement;
		INT32_MIN=-2^32
		# define INT32_MIN		(-2147483647-1)
		  
SQL:
    select s.shardid as shard_id from pg_dist_shard s where s.logicalrelid='test'::regclass and hashint4(12)>=s.shardminvalue::integer and hashint4(12)<=s.shardmaxvalue::integer;
    
  */
```

### 验证测试

```sql
flying=# select count(*) as shard_count from pg_dist_shard where logicalrelid='test'::regclass;
 shard_count 
-------------
           6
(1 row)

-- 分区间隔 
-- hashTokenIncrement=2^32/6=715827882 

-- 分布列的hash值
flying=# select hashint4(12) as searchedValue;
 searchedvalue 
---------------
    1938269380
(1 row)


-- 分区位置
-- shardIndex=（1938269380-(-2147483648)）/715827882=5

-- 得到shardid
-- return cacheEntry->sortedShardIntervalArray[shardIndex];

-- 如果正确的话应该是位于第6个分片上，这个函数应该返回的是102179
-- 验证

flying=# select * from pg_dist_shard where logicalrelid='test'::regclass;
 logicalrelid | shardid | shardstorage | shardminvalue | shardmaxvalue 
--------------+---------+--------------+---------------+---------------
 test         |  102174 | t            | -2147483648   | -1431655767
 test         |  102175 | t            | -1431655766   | -715827885
 test         |  102176 | t            | -715827884    | -3
 test         |  102177 | t            | -2            | 715827879
 test         |  102178 | t            | 715827880     | 1431655761
 test         |  102179 | t            | 1431655762    | 2147483647
(6 rows)

flying=# select get_shard_id_for_distribution_column('test',12);
 get_shard_id_for_distribution_column 
--------------------------------------
                               102179
(1 row)

-- 发现shardid确实等于102179
```

