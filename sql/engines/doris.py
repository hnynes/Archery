# -*- coding: UTF-8 -*-
import logging
import re
import traceback
import MySQLdb

import schemaobject
import sqlparse
from MySQLdb.constants import FIELD_TYPE
from schemaobject.connection import build_database_url

from sql.utils.sql_utils import get_syntax_type, remove_comments
from . import EngineBase
from .models import ResultSet, ReviewResult, ReviewSet
from sql.utils.data_masking import data_masking
from common.utils.timer import FuncTimer
from common.config import SysConfig

logger = logging.getLogger('default')

class DorisEngine(EngineBase):

    def __init__(self, instance=None):
        super(DorisEngine, self).__init__(instance=instance)
        self.config = SysConfig()

    def get_connection(self, db_name=None):
        conversions = MySQLdb.converters.conversions
        #conversions[FIELD_TYPE.BIT] = lambda data: data == b'\x01'
        if self.conn:
            self.thread_id = self.conn.thread_id()
            return self.conn
        if db_name:
            self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        db=db_name, charset=self.instance.charset or 'utf8',
                                        conv=conversions,
                                        connect_timeout=10)
        else:
            self.conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.password,
                                        charset=self.instance.charset or 'utf8',
                                        conv=conversions,
                                        connect_timeout=10)
        self.thread_id = self.conn.thread_id()
        return self.conn

    @property
    def name(self):
        return 'Doris'

    @property
    def info(self):
        return 'Doris engine'

    @property
    def auto_backup(self):
        """是否支持备份"""
        return False

    @property
    def server_version(self):
        sql = "select VARIABLE_VALUE from information_schema.global_variables where VARIABLE_NAME = 'version_comment';"
        result = self.query(sql=sql)
        version = result.rows[0].split(' ')[2]
        return version
    
    @property
    def schema_object(self):
        """获取实例对象信息"""
        url = build_database_url(host=self.host,
                                 username=self.user,
                                 password=self.password,
                                 port=self.port)
        return schemaobject.SchemaObject(url, charset=self.instance.charset or 'utf8mb4')

    def get_table_engine(self, tb_name):
        """获取某个table的engine type"""
        sql = f"""select engine 
                    from information_schema.tables
                   where table_schema='{tb_name.split('.')[0]}' 
                     and table_name='{tb_name.split('.')[1]}'"""
        query_result = self.query(sql=sql)
        if query_result.rows:
            result = {'status': 1, 'engine': query_result.rows[0][0]}
        else:
            result = {'status': 0, 'engine': 'None'}
        return result

    def get_all_databases(self):
        """获取数据库列表, 返回一个ResultSet"""
        sql = "show databases"
        result = self.query(sql=sql)
        db_list = [row[0] for row in result.rows
                   if row[0] not in ('_statistics_', 'INFORMATION_SCHEMA', 'starrocks_monitor')]
        result.rows = db_list
        logger.warning(f"dblist ：{db_list} ")
        return result

    def get_all_tables(self, db_name, **kwargs):
        """获取table 列表, 返回一个ResultSet"""
        sql = "show tables"
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows]
        result.rows = tb_list
        return result

    def get_group_tables_by_db(self, db_name):
        # escape
        db_name = MySQLdb.escape_string(db_name).decode('utf-8')
        data = {}
        sql = f"""SELECT TABLE_NAME,
                            TABLE_COMMENT
                        FROM
                            information_schema.TABLES
                        WHERE
                            TABLE_SCHEMA='{db_name}';"""
        result = self.query(db_name=db_name, sql=sql)
        for row in result.rows:
            table_name, table_cmt = row[0], row[1]
            if table_name[0] not in data:
                data[table_name[0]] = list()
            data[table_name[0]].append([table_name, table_cmt])
        return data

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """数据字典页面使用：获取表格的元信息，返回一个dict{column_list: [], rows: []}"""
        # escape
        db_name = MySQLdb.escape_string(db_name).decode('utf-8')
        tb_name = MySQLdb.escape_string(tb_name).decode('utf-8')
        sql = f"""SELECT
                        TABLE_NAME as table_name,
                        ENGINE as engine,
                        ROW_FORMAT as row_format,
                        TABLE_ROWS as table_rows,
                        AVG_ROW_LENGTH as avg_row_length,
                        round(DATA_LENGTH/1024, 2) as data_length,
                        MAX_DATA_LENGTH as max_data_length,
                        round(INDEX_LENGTH/1024, 2) as index_length,
                        round((DATA_LENGTH + INDEX_LENGTH)/1024, 2) as data_total,
                        DATA_FREE as data_free,
                        AUTO_INCREMENT as auto_increment,
                        TABLE_COLLATION as table_collation,
                        CREATE_TIME as create_time,
                        CHECK_TIME as check_time,
                        UPDATE_TIME as update_time,
                        TABLE_COMMENT as table_comment
                    FROM
                        information_schema.TABLES
                    WHERE
                        TABLE_SCHEMA='{db_name}'
                            AND TABLE_NAME='{tb_name}'"""
        _meta_data = self.query(db_name, sql)
        return {'column_list': _meta_data.column_list, 'rows': _meta_data.rows[0]}

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """获取表格字段信息"""
        sql = f"""SELECT 
                        COLUMN_NAME as '列名',
                        COLUMN_TYPE as '列类型',
                        CHARACTER_SET_NAME as '列字符集',
                        IS_NULLABLE as '是否为空',
                        COLUMN_KEY as '索引列',
                        COLUMN_DEFAULT as '默认值',
                        EXTRA as '拓展信息',
                        COLUMN_COMMENT as '列说明'
                    FROM
                        information_schema.COLUMNS
                    WHERE
                        TABLE_SCHEMA = '{db_name}'
                            AND TABLE_NAME = '{tb_name}'
                    ORDER BY ORDINAL_POSITION;"""
        _desc_data = self.query(db_name, sql)
        return {'column_list': _desc_data.column_list, 'rows': _desc_data.rows}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """获取表格索引信息"""
        sql = f"""SELECT
                        COLUMN_NAME as '列名',
                        INDEX_NAME as '索引名',
                        NON_UNIQUE as '唯一性',
                        SEQ_IN_INDEX as '列序列',
                        CARDINALITY as '基数',
                        NULLABLE as '是否为空',
                        INDEX_TYPE as '索引类型',
                        COMMENT as '备注'
                    FROM
                        information_schema.STATISTICS
                    WHERE
                        TABLE_SCHEMA = '{db_name}'
                    AND TABLE_NAME = '{tb_name}';"""
        _index_data = self.query(db_name, sql)
        return {'column_list': _index_data.column_list, 'rows': _index_data.rows}

    def get_tables_metas_data(self, db_name, **kwargs):
        """获取数据库所有表格信息，用作数据字典导出接口"""
        sql_tbs = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db_name}';"
        tbs = self.query(sql=sql_tbs, cursorclass=MySQLdb.cursors.DictCursor, close_conn=False).rows
        table_metas = []
        for tb in tbs:
            _meta = dict()
            engine_keys = [{"key": "COLUMN_NAME", "value": "字段名"}, {"key": "COLUMN_TYPE", "value": "数据类型"},
                           {"key": "COLUMN_DEFAULT", "value": "默认值"}, {"key": "IS_NULLABLE", "value": "允许非空"},
                           {"key": "EXTRA", "value": "自动递增"}, {"key": "COLUMN_KEY", "value": "是否主键"},
                           {"key": "COLUMN_COMMENT", "value": "备注"}]
            _meta["ENGINE_KEYS"] = engine_keys
            _meta['TABLE_INFO'] = tb
            sql_cols = f"""SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA='{tb['TABLE_SCHEMA']}' AND TABLE_NAME='{tb['TABLE_NAME']}';"""
            _meta['COLUMNS'] = self.query(sql=sql_cols, cursorclass=MySQLdb.cursors.DictCursor, close_conn=False).rows
            table_metas.append(_meta)
        return table_metas
    
    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """获取所有字段, 返回一个ResultSet"""
        sql = f"""SELECT
            COLUMN_NAME,
            COLUMN_TYPE,
            CHARACTER_SET_NAME,
            IS_NULLABLE,
            COLUMN_KEY,
            EXTRA,
            COLUMN_COMMENT
        FROM
            information_schema.COLUMNS
        WHERE
            TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{tb_name}'
        ORDER BY ORDINAL_POSITION;"""
        result = self.query(db_name=db_name, sql=sql)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result

    def describe_table(self, db_name, tb_name, **kwargs):
        """return ResultSet 类似查询"""
        sql = f"show create table `{tb_name}`;"
        result = self.query(db_name=db_name, sql=sql)
        return result

    def query(self, db_name=None, sql='', limit_num=0, close_conn=True, **kwargs):
        """返回 ResultSet """
        result_set = ResultSet(full_sql=sql)
        max_execution_time = kwargs.get('max_execution_time', 0)
        cursorclass = kwargs.get('cursorclass') or MySQLdb.cursors.Cursor
        try:
            conn = self.get_connection(db_name=db_name)
            conn.autocommit(True)
            cursor = conn.cursor(cursorclass)
            try:
                cursor.execute(f"set session max_execution_time={max_execution_time};")
            except MySQLdb.OperationalError:
                pass
            effect_row = cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(size=int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = rows
            result_set.affected_rows = effect_row
        except Exception as e:
            logger.warning(f"Doris语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}")
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_check(self, db_name=None, sql=''):
        # 查询语句的检查、注释去除、切分
        result = {'msg': '', 'bad_query': False, 'filtered_sql': sql, 'has_star': False}
        # 删除注释语句，进行语法判断，执行第一条有效sql
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result['filtered_sql'] = sql.strip()
        except IndexError:
            result['bad_query'] = True
            result['msg'] = '没有有效的SQL语句'
        if re.match(r"^select|^show|^explain", sql, re.I) is None:
            result['bad_query'] = True
            result['msg'] = '不支持的查询语法类型!'
        if '*' in sql:
            result['has_star'] = True
            result['msg'] = 'SQL语句中含有 * '
        # select语句先使用Explain判断语法是否正确
        if re.match(r"^select", sql, re.I):
            explain_result = self.query(db_name=db_name, sql=f"explain {sql}")
            if explain_result.error:
                result['bad_query'] = True
                result['msg'] = explain_result.error

        return result

    def filter_sql(self, sql='', limit_num=0):
        # 对查询sql增加limit限制,limit n 或 limit n,n 或 limit n offset n统一改写成limit n
        sql = sql.rstrip(';').strip()
        if re.match(r"^select", sql, re.I):
            # LIMIT N
            limit_n = re.compile(r'limit\s+(\d+)\s*$', re.I)
            # LIMIT M OFFSET N
            limit_offset = re.compile(r'limit\s+(\d+)\s+offset\s+(\d+)\s*$', re.I)
            # LIMIT M,N
            offset_comma_limit = re.compile(r'limit\s+(\d+)\s*,\s*(\d+)\s*$', re.I)
            if limit_n.search(sql):
                sql_limit = limit_n.search(sql).group(1)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_n.sub(f'limit {limit_num};', sql)
            elif limit_offset.search(sql):
                sql_limit = limit_offset.search(sql).group(1)
                sql_offset = limit_offset.search(sql).group(2)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = limit_offset.sub(f'limit {limit_num} offset {sql_offset};', sql)
            elif offset_comma_limit.search(sql):
                sql_offset = offset_comma_limit.search(sql).group(1)
                sql_limit = offset_comma_limit.search(sql).group(2)
                limit_num = min(int(limit_num), int(sql_limit))
                sql = offset_comma_limit.sub(f'limit {sql_offset},{limit_num};', sql)
            else:
                sql = f'{sql} limit {limit_num};'
        else:
            sql = f'{sql};'
        return sql

    def explain_check(self, check_result, db_name=None, line=0, statement=''):
        """使用explain ast检查sql语法, 返回Review set"""
        result = ReviewResult(id=line, errlevel=0,
                              stagestatus='Audit completed',
                              errormessage='None',
                              sql=statement,
                              affected_rows=0,
                              execute_time=0, )
        explain_result = self.query(db_name=db_name, sql=f"explain ast {statement}")
        if explain_result.error:
            check_result.is_critical = True
            result = ReviewResult(id=line, errlevel=2,
                                stagestatus='驳回未通过检查SQL',
                                errormessage=f'explain语法检查错误：{explain_result.error}',
                                sql=statement)
        return result

    def execute_check(self, db_name=None, sql=''):
        """上线单执行前的检查, 返回Review set"""
        sql = sqlparse.format(sql, strip_comments=True)
        sql_list = sqlparse.split(sql)

        # 禁用/高危语句检查
        check_result = ReviewSet(full_sql=sql)
        line = 1
        critical_ddl_regex = self.config.get('critical_ddl_regex', '')
        p = re.compile(critical_ddl_regex)
        check_result.syntax_type = 2  # TODO 工单类型 0、其他 1、DDL，2、DML

        for statement in sql_list:
            statement = statement.rstrip(';')
            # 禁用语句
            if re.match(r"^select|^show", statement.lower()):
                check_result.is_critical = True
                result = ReviewResult(id=line, errlevel=2,
                                      stagestatus='驳回不支持语句',
                                      errormessage='仅支持DML和DDL语句，查询语句请使用SQL查询功能！',
                                      sql=statement)
            # 高危语句
            elif critical_ddl_regex and p.match(statement.strip().lower()):
                check_result.is_critical = True
                result = ReviewResult(id=line, errlevel=2,
                                      stagestatus='驳回高危SQL',
                                      errormessage='禁止提交匹配' + critical_ddl_regex + '条件的语句！',
                                      sql=statement)
            # alter语句
            elif re.match(r"^alter", statement.lower()):
                # alter table语句
                if re.match(r"^alter\s+table\s+(.+?)\s+", statement.lower()):
                    table_name = re.match(r"^alter\s+table\s+(.+?)\s+", statement.lower(), re.M).group(1)
                    if '.' not in table_name:
                        table_name = f"{db_name}.{table_name}"
                    table_engine = self.get_table_engine(table_name)['engine']
                    table_exist = self.get_table_engine(table_name)['status']
                    
                    if table_exist == 1:
                        result = self.explain_check(check_result, db_name, line, statement)
                    else:
                        check_result.is_critical = True
                        result = ReviewResult(id=line, errlevel=2,
                                              stagestatus='表不存在',
                                              errormessage=f'表 {table_name} 不存在！',
                                              sql=statement)
                # 其他alter语句
                else:
                    result = self.explain_check(check_result, db_name, line, statement)
            # truncate语句
            elif re.match(r"^truncate\s+table\s+(.+?)(\s|$)", statement.lower()):
                table_name = re.match(r"^truncate\s+table\s+(.+?)(\s|$)", statement.lower(), re.M).group(1)
                if '.' not in table_name:
                    table_name = f"{db_name}.{table_name}"
                table_engine = self.get_table_engine(table_name)['engine']
                table_exist = self.get_table_engine(table_name)['status']
                if table_exist == 1:
                    if table_engine in ('View', 'Null'):
                        check_result.is_critical = True
                        result = ReviewResult(id=line, errlevel=2,
                                              stagestatus='驳回不支持SQL',
                                              errormessage='TRUNCATE不支持View和Null表引擎！',
                                              sql=statement)
                    # dorisdb中truncate table不支持使用explain，因此检查完表后为通过
                    else:
                        result = ReviewResult(id=line, errlevel=0,
                                              stagestatus='Audit completed',
                                              errormessage='None',
                                              sql=statement,
                                              affected_rows=0,
                                              execute_time=0, )
                else:
                    check_result.is_critical = True
                    result = ReviewResult(id=line, errlevel=2,
                                          stagestatus='表不存在',
                                          errormessage=f'表 {table_name} 不存在！',
                                          sql=statement)
            # insert语句，explain无法正确判断，暂时只做表存在性检查与简单关键字匹配
            elif re.match(r"^insert", statement.lower()):
                if re.match(r"^insert\s+into\s+(.+?)(\s+|\s*\(.+?)(values|format|select)(\s+|\()", statement.lower()):
                    table_name = re.match(r"^insert\s+into\s+(.+?)(\s+|\s*\(.+?)(values|format|select)(\s+|\()",
                                          statement.lower(), re.M).group(1)
                    if '.' not in table_name:
                        table_name = f"{db_name}.{table_name}"
                    table_exist = self.get_table_engine(table_name)['status']
                    if table_exist == 1:
                        result = ReviewResult(id=line, errlevel=0,
                                              stagestatus='Audit completed',
                                              errormessage='None',
                                              sql=statement,
                                              affected_rows=0,
                                              execute_time=0, )
                    else:
                        check_result.is_critical = True
                        result = ReviewResult(id=line, errlevel=2,
                                              stagestatus='表不存在',
                                              errormessage=f'表 {table_name} 不存在！',
                                              sql=statement)
                else:
                    check_result.is_critical = True
                    result = ReviewResult(id=line, errlevel=2,
                                          stagestatus='驳回不支持SQL',
                                          errormessage='INSERT语法不正确！',
                                          sql=statement)
            # create语句，explain无法正确判断，暂时只做表存在性检查与简单关键字匹配
            elif re.match(r"^create", statement.lower()):
                result = ReviewResult(id=line, errlevel=0,
                                              stagestatus='Audit completed',
                                              errormessage='None',
                                              sql=statement,
                                              affected_rows=0,
                                              execute_time=0, )
            
            elif re.match(r"^update", statement.lower()):
                result = ReviewResult(id=line, errlevel=2,
                                              stagestatus='驳回不支持SQL',
                                              errormessage='UPDATE语法不支持！',
                                              sql=statement)

            # 其他语句使用explain ast简单检查
            else:
                result = self.explain_check(check_result, db_name, line, statement)

            # 没有找出DDL语句的才继续执行此判断
            if check_result.syntax_type == 2:
                if get_syntax_type(statement, parser=False, db_type='mysql') == 'DDL':
                    check_result.syntax_type = 1
            check_result.rows += [result]

            # 遇到禁用和高危语句直接返回
            if check_result.is_critical:
                check_result.error_count += 1
                return check_result
            line += 1
        return check_result

    def execute_workflow(self, workflow):
        """执行上线单，返回Review set"""
        sql = workflow.sqlworkflowcontent.sql_content
        execute_result = ReviewSet(full_sql=sql)
        sqls = sqlparse.format(sql, strip_comments=True)
        sql_list = sqlparse.split(sqls)

        line = 1
        for statement in sql_list:
            with FuncTimer() as t:
                result = self.execute(db_name=workflow.db_name, sql=statement, close_conn=True)
            if not result.error:
                execute_result.rows.append(ReviewResult(
                    id=line,
                    errlevel=0,
                    stagestatus='Execute Successfully',
                    errormessage='None',
                    sql=statement,
                    affected_rows=0,
                    execute_time=t.cost,
                ))
                line += 1
            else:
                # 追加当前报错语句信息到执行结果中
                execute_result.error = result.error
                execute_result.rows.append(ReviewResult(
                    id=line,
                    errlevel=2,
                    stagestatus='Execute Failed',
                    errormessage=f'异常信息：{result.error}',
                    sql=statement,
                    affected_rows=0,
                    execute_time=0,
                ))
                line += 1
                # 报错语句后面的语句标记为审核通过、未执行，追加到执行结果中
                for statement in sql_list[line - 1:]:
                    execute_result.rows.append(ReviewResult(
                        id=line,
                        errlevel=0,
                        stagestatus='Audit completed',
                        errormessage=f'前序语句失败, 未执行',
                        sql=statement,
                        affected_rows=0,
                        execute_time=0,
                    ))
                    line += 1
                break
        return execute_result

    def execute(self, db_name=None, sql='', close_conn=True):
        """原生执行语句"""
        result = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                cursor.execute(statement)
            cursor.close()
        except Exception as e:
            logger.warning(f"Doris语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}")
            result.error = str(e)
        if close_conn:
            self.close()
        return result

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None