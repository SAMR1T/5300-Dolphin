/*
	@file sql5300.cpp  - main entry for the SQL Shell of 
						the relation manager
	@author Ruoyang Qiu, Samrit Dhesi
	@ Seattle University, cpsc4300/5300, 2020 Spring
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"


using namespace std;
using namespace hsql;

DbEnv *_DB_ENV;

string expressionToString(Expr *expr);
string executeSelectStatement(const SelectStatement *stmt);

/*
	Convert a hyrise Expression AST stands for operator to equivalent SQL Operator
	@param expr	operator expression 
	@return		return a SQL operator string equivalent to expr
*/
string operatorExpressionToString(Expr *expr) {
	if(expr == NULL) {
		return "null";
	}

	string result;

	// NOT Operator
	if(expr->opType == Expr::NOT)
		result += "NOT ";

	result += expressionToString(expr->expr) + " ";

	// Operator 
	switch(expr->opType) {
		case Expr::AND:
			result += "AND";
			break;
		case Expr::OR:
			result += "OR";
			break;
		default:
			break;
	}
	if(expr->expr2 != NULL)
		result += expressionToString(expr->expr2);

	return result;
}

/*
	Convert a hyrise Expression ASTto equivalent SQL Operator
	@param expr	expression 
	@return		return a SQL string equivalent to expr
*/
string expressionToString(Expr *expr) {
	string result;
	switch(expr->type) {
		case kExprStar:
			result += "*";
			break;
    	case kExprColumnRef:
			if(expr->table != NULL)
				result += string(expr->table) + ".";
			result += string(expr->name);
			break;
    	case kExprLiteralFloat:
      		result += to_string(expr->fval);
      		break;
    	case kExprLiteralInt:
      		result += to_string(expr->ival);
      		break;
    	case kExprLiteralString:
      		result += string(expr->name);
      		break;
    	case kExprFunctionRef:
      		result += string(expr->name);
      		result += string(expr->expr->name);
      		break;
    	case kExprOperator:
      		result += operatorExpressionToString(expr);
      		break;
    	default:
      		result += "Unrecognized expression type %d\n";
      		break;
	}

	if(expr->alias != NULL){
		result += string(" AS ");
		result += string(expr->alias);
	}

	return result;
}

/*
	Convert a hyrise TableRef AST to equivalent SQL string
	@param col	TableRef to unparse
	@return		return a SQL string equivalent to table
*/
string tableRefInfoToString(TableRef *table) {
	string result;
	switch (table->type) {
		case kTableName:
			result += string(table->name);
			if(table->alias != NULL) {
				result += string(" AS ");
				result += string(table->alias);
			}
			break;
		case kTableSelect:
			result += executeSelectStatement(table->select);
			break;
		case kTableJoin:
			result += tableRefInfoToString(table->join->left);
			switch(table->join->type) {
				case kJoinInner:
					result += " JOIN ";
					break;
				case kJoinLeft:
					result += " LEFT JOIN ";
					break;
				case kJoinNatural:
				case kJoinCross:
				case kJoinOuter:
				case kJoinLeftOuter:
				case kJoinRightOuter:
				case kJoinRight:
					result += " RIGHT JOIN ";
					break;
			}
			result += tableRefInfoToString(table->join->right);
			if(table->join->condition != NULL) {
				result += " ON " + expressionToString(table->join->condition);
			}
			break;
		case kTableCrossProduct:
			bool firstElement = true;
			for (TableRef* tbl : *table->list) {
				if(!firstElement)
					result += ", ";
				result += tableRefInfoToString(tbl);
				firstElement = false;
			}
		break;
    }
	return result;
}

/*
	Convert a hyrise ColumnDefinition AST to equivalent SQL string
	@param col	column definition object
	@return		return a SQL string equivalent to col
*/

string columnDefinitionToString(const ColumnDefinition *col) {
	string result(col->name);
	switch(col->type) {
		case ColumnDefinition::DOUBLE:
			result += " DOUBLE";
			break;
		case ColumnDefinition::INT:
			result += " INT";
			break;
		case ColumnDefinition::TEXT:
			result += " TEXT";
			break;
		default:
			result += " ...";
			break;
	}
	return result;
}

/* 
	Excute a Select Statement
	@param 		stmt Hyrise AST for SelectStatement
	@returns	a string of the SQL statement
 */
string executeSelectStatement(const SelectStatement *stmt) {
	string result("SELECT ");

	// check if the current expressopm is the first element
	// if not, a comma should be added to split expressions
	bool firstElement = true; 

	for(Expr *expr : *stmt->selectList) {
		if(!firstElement)
			result += ", ";
		result += expressionToString(expr);
		firstElement = false;
	}

	result += " FROM " + tableRefInfoToString(stmt->fromTable);

	if(stmt->whereClause != NULL) {
		result += " WHERE " + expressionToString(stmt->whereClause);
	}
	return result;
}

/* 
	Excute a Select Statement
	@param 		stmt Hyrise AST for SelectStatement
	@returns	a string of the SQL statement
 */
string executeCreateStatement(const CreateStatement *stmt) {

	string result("CREATE TABLE ");
	// then if the user puts the if not exist check that
	if (stmt->ifNotExists)
		result += "IF NOT EXISTS ";
	// then the table name is written
	result += string(stmt->tableName);
	// now begin the actual create statements, we need parenthesis
	result += " (";
	// now for each column in the query:
	bool firstElement = true;
	for (ColumnDefinition *currCol : *stmt->columns) {
        if (!firstElement)
            result += ", ";
        result += columnDefinitionToString(currCol);
        firstElement = false;
    }
	//close the parenthesis of the query
    result += ")";
	// return results
    return result;
}

/* 
	Excute a Select Statement
	@param 		stmt Hyrise AST for SelectStatement
	@returns	a string of the SQL statement
 */
string executeInsertStatement(const InsertStatement *stmt) {
	string result = "INSERT INTO ";
	result += string(stmt->tableName);
	if(stmt->columns != NULL) {
		result += " (";
		bool firstElement = true;
		for(char* col_name : *stmt->columns) {
			if(!firstElement)
				result += ", ";
			result += string(col_name);
		}
		result += ") ";
	}

	switch(stmt->type) {
		case InsertStatement::kInsertValues: {
			result += " VALUES (";
			bool firstValue = true;
			for(Expr* expr : *stmt->values) {
				if(!firstValue)
					result += ", ";
				result += expressionToString(expr);
				firstValue = false;
			}
			result += ") ";
			break;
		}			
		case InsertStatement::kInsertSelect:
			result += executeSelectStatement(stmt->select);
			break;
	}

	return result; 

}

/* 
	Excute a SQL Statement
	@param 		stmt Hyrise AST for SQL Statement
	@returns	a string of the SQL statement
 */
string execute(const SQLStatement *stmt) {
	switch(stmt->type()) {
		case kStmtSelect:
			return executeSelectStatement((const SelectStatement*) stmt);
		case kStmtCreate:
			return executeCreateStatement((const CreateStatement*) stmt);
		case kStmtInsert:
			return executeInsertStatement((const InsertStatement*) stmt);
		default:
			return "Not Implemented";
	}
}



int main(int argc, char **argv) {
	

	if(argc != 2) {
		cerr << "Usage: cpsc5300: dbenvpath" << endl;
		return 1;
	}

	char *envHome = argv[1];
	cout << "(sql 5300: running with database environment at " << envHome << endl;

	DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	
	try {
		env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException &exc) {
		cerr << "(cpsc5300: " << exc.what() << ")";
		exit(1);
	}

	_DB_ENV = &env;

	while(true) {
		cout << "SQL> ";
		string query;
		getline(cin, query);
		if(query.length() == 0)
			continue;
		if(query == "quit")
			break;
		if (query == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        }
		SQLParserResult *sqlresult = SQLParser::parseSQLString(query);

		if(!sqlresult->isValid()) {
			cout << "invalid SQL: " << query << endl;
			delete sqlresult;
			continue;
		}

		// excute the statement

		for ( uint i = 0; i < sqlresult->size(); ++i) {
			cout << execute(sqlresult->getStatement(i)) << endl;
		}
		delete sqlresult;
	}
	
	
	return EXIT_SUCCESS;
}

