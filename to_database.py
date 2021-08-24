# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 11:13:36 2021

@author: user
"""
from modifiedSQLmanageer import *
class submit_to_SQL:

    def submitSQL(self, df, tableName):
        """Modeling data using dynamic query.

        Args:

        Returns:

        """
        db_ins = SQLManager()
        exist_statement = 'SELECT COUNT(*) FROM %s' % tableName
        rows = db_ins.runQuery(exist_statement)
        if rows == 0:
            create_statement = 'CREATE TABLE %s (dummy int NOT NULL)' % tableName
            try :
                db_ins.runQuery(create_statement)
            except Exception as e:
                error_msg = "[Error] Fail to create table.\n :: %s" % e.__str__()
                print(error_msg)

        duplicate_statement = 'SELECT * INTO temp_tr FROM %s' % tableName
        db_ins.runQuery(duplicate_statement)
        delete_statement = 'DROP TABLE temp_tr'

        try:
            df.to_sql(name=tableName, con=db_ins.engine, if_exists='replace', index=False)
            db_ins.runQuery(delete_statement)
        except Exception as e:
            restore_statement = 'CREATE TABLE %s SELECT * FROM temp_tr' % tableName
            db_ins.runQuery(restore_statement)
            db_ins.runQuery(delete_statement)
            error_msg = "[Error] Fail to submit data to table.\n :: %s" % e.__str__()
            print(error_msg)