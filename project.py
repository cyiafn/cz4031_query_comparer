from configparser import ConfigParser
from typing import Dict, Any, List, Tuple, Optional

import psycopg2
import sqlparse
import difflib 

SQL_KEYWORDS = set(
    "SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN, NATURAL JOIN, USING, DISTINCT, UNION, INTERSECT, EXCEPT, VALUES, FETCH, NEXT, LAST, FIRST, PRIOR, CURRENT, ROW, ROWS, OVER, PARTITION BY, RANK, DENSE_RANK, ROW_NUMBER, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, CASE, WHEN, THEN, ELSE, END, CAST, COALESCE, NULLIF, GREATEST, LEAST".split(
        ", "))


# Query Tree Nodes =======================================================
class QueryPlanNode:
    def __init__(self, node: Dict[str, Any], left=None, right=None):
        self.node: str = node["Node Type"]
        self.totalCost: int = node["Total Cost"]
        self.ParentRelation: str = node["Parent Relationship"] if "Parent Relationship" in node else ""
        self.left: QueryPlanNode = left
        self.right: QueryPlanNode = right

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanNode):
            return False
        return self.node == other.node and self.totalCost == other.totalCost and \
            self.ParentRelation == other.ParentRelation

    def __str__(self) -> str:
        return f"{self.node}, {self.left}, {self.right}"


class QueryPlanSortNode(QueryPlanNode):
    def __init__(self, node: Dict[str, Any], left: QueryPlanNode = None, right: QueryPlanNode = None):
        super().__init__(node, left, right)
        self.SortKeys: List[str] = node["Sort Key"]

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Sort Key" in node and node["Sort Key"] == "Sort"

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanSortNode):
            return False
        return super().__eq__(other) and self.SortKeys == other.SortKeys


class QueryPlanGroupNode(QueryPlanNode):
    def __init__(self, node: Dict[str, Any], left: QueryPlanNode = None, right: QueryPlanNode = None):
        super().__init__(node, left, right)
        self.GroupKeys: List[str] = node["Group Key"]

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Group Key" in node

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanGroupNode):
            return False
        return super().__eq__(other) and self.GroupKeys == other.GroupKeys


class QueryPlanJoinNode(QueryPlanNode):
    def __init__(self, node: Dict[str, Any], left: QueryPlanNode = None, right: QueryPlanNode = None):
        super().__init__(node, left, right)
        self.JoinType: str = node["Join Type"]
        self.JoinCond: str = self.generateJoinCond(node)

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Join Type" in node

    @staticmethod
    def generateJoinCond(node: Dict[str, Any]) -> str:
        if "Hash Cond" in node:
            return node["Hash Cond"]
        if "Filter" in node:
            return node["Filter"]
        return ""

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanJoinNode):
            return False
        return super().__eq__(other) and self.JoinType == other.JoinType and self.JoinCond == other.JoinCond


class QueryPlanScanNode(QueryPlanNode):
    def __init__(self, node: Dict[str, Any], left: QueryPlanNode = None, right: QueryPlanNode = None):
        super().__init__(node, left, right)
        self.RelationName: str = node["Relation Name"]
        self.IndexName: str = node["Index Name"] if "Index Name" in node else ""
        self.IndexCond: str = node["Index Cond"] if "Index Cond" in node else ""
        self.Filter: str = node["Filter"] if "Filter" in node else ""

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Relation Name" in node

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanScanNode):
            return False
        return super().__eq__(other) and self.RelationName == other.RelationName and \
            self.IndexName == other.IndexName and self.IndexCond == other.IndexCond and \
            self.Filter == other.Filter


# Query Tree =======================================================
class QueryPlan:
    def __init__(self, plan: Dict[str, Any]):
        self.root = self.parseQueryPlan(plan)
        self.originalPlan = plan

    def parseQueryPlan(self, plan: Dict[str, Any]) -> QueryPlanNode:
        left, right = None, None
        if "Plans" in plan:
            if len(plan["Plans"]) > 0:
                left = self.parseQueryPlan(plan["Plans"][0])
            if len(plan["Plans"]) > 1:
                right = self.parseQueryPlan(plan["Plans"][1])

        if QueryPlanSortNode.Am(plan):
            return QueryPlanSortNode(plan, left, right)
        if QueryPlanGroupNode.Am(plan):
            return QueryPlanGroupNode(plan, left, right)
        if QueryPlanJoinNode.Am(plan):
            return QueryPlanJoinNode(plan, left, right)
        if QueryPlanScanNode.Am(plan):
            return QueryPlanScanNode(plan, left, right)
        return QueryPlanNode(plan, left, right)

    def print(self) -> None:
        print(self.root)

    def IsEqual(self, other) -> Tuple[bool, QueryPlanNode, QueryPlanNode]:
        if not isinstance(other, QueryPlan):
            return False, self.root, other.root

        def isEq(node1: QueryPlanNode, node2: QueryPlanNode) -> Tuple[
            bool, Optional[QueryPlanNode], Optional[QueryPlanNode]]:
            if node1 is None and node2 is None:
                return True, None, None
            if node1 is None or node2 is None:
                return False, node1, node2
            if node1 != node2:
                return False, node1, node2
            return isEq(node1.left, node2.left) and isEq(node1.right, node2.right)

        return isEq(self.root, other.root)


# DB
def query(sqlQuery: str) -> Dict[str, Any]:
    conn = psycopg2.connect(**getDBConfig())
    cursor = conn.cursor()
    cursor.execute(f"EXPLAIN (FORMAT JSON) {sqlQuery}")
    queryPlan = cursor.fetchall()

    cursor.close()
    conn.close()
    return queryPlan[0][0][0]


def getDBConfig(configName: str = "database.ini") -> Dict[str, str]:
    cfgParser = ConfigParser()
    cfgParser.read(configName)

    if not cfgParser.has_section("postgresql"):
        raise Exception(f"postgresql section not found in the {configName} file")

    cfg = {}
    for val in cfgParser.items("postgresql"):
        key, value = val
        cfg[key] = value

    return cfg


# SQL =====
def parseSQL(sqlQuery: str) -> Tuple[str, List[List[str]]]:
    formattedSQL = sqlparse.format(sqlQuery, reindent=True, keyword_case='upper', strip_comments=True)
    return formattedSQL, groupFormattedSQLByClause(formattedSQL)


'''
This group is as such, if there is a keyword (non-operator) such as SELECT, WHERE, FROM,
then we will form a new group. Otherwise, we will append the line to the previous group.

So, for example, the following SQL:
SELECT *
FROM Students
WHERE StudentID = 1
  AND StudentName = 'John'

Will be grouped as such:
[['SELECT *'], ['FROM Students'], ['WHERE StudentID = 1', '  AND StudentName = 'John'']]

Note that the spaces are preserved.

This might serve as a more useful format for us to parse the SQL for differences.
'''


def groupFormattedSQLByClause(sqlQuery: str) -> List[List[str]]:
    groupedSQL = []
    tempGroup = []
    splitSQLQuery = sqlQuery.split("\n")
    for line in splitSQLQuery:
        strippedLine = line.strip()
        if strippedLine.split(" ")[0] in SQL_KEYWORDS:
            groupedSQL.append(tempGroup) if len(tempGroup) > 0 else None
            tempGroup = [line]
        else:
            tempGroup.append(line)

    groupedSQL.append(tempGroup)
    return groupedSQL
 
def getDiff(sql1, sql2):
    sql1_formatted, list1 = parseSQL(sql1)
    sql2_formatted, list2 = parseSQL(sql2)
    diff = []
    for i, (sublist1, sublist2) in enumerate(zip(list1, list2)):
        
        for i in range(len(sublist1)):
            sublist1[i] = sublist1[i].replace(",","")
        for i in range(len(sublist2)):
            sublist2[i] = sublist2[i].replace(",","")
        print(sublist1,sublist2)   
        
        if sublist1 != sublist2:
            sm = difflib.SequenceMatcher(lambda x: x in SQL_KEYWORDS, sublist1, sublist2)
            for tag, i1, i2, j1, j2 in sm.get_opcodes():
                sublist1[0]    
                if tag == 'replace':
                    diff.append(f' Modified: {sublist1[i1:i2][0].replace("/", "")} => {sublist2[j1:j2][0].replace("/", "")}')
                elif tag == 'delete':
                    diff.append(f' Removed: {sublist1[i1:i2][0].replace("/", "")}')
                elif tag == 'insert':
                    diff.append(f' Added: {sublist2[j1:j2][0].replace("/", "")}')
              
    return diff


if __name__ == "__main__":
    print(parseSQL(
        "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01' and c_acctbal > 10 and s_acctbal > 20 group by n_name order by revenue desc;"))
    # plan = query(
    #     "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01' and c_acctbal > 10 and s_acctbal > 20 group by n_name order by revenue desc;")
    # q1 = QueryPlan(plan["Plan"])
    # plan = query(
    #     "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01' and c_acctbal > 10 and s_acctbal > 20 group by n_name order by revenue desc;")
    # q2 = QueryPlan(plan["Plan"])
    #
    # print(q1.IsEqual(q2))
    # print(q2[1])
    q1 = "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01' and c_acctbal > 10 and s_acctbal > 20 group by n_name order by revenue desc;"
    q2 = "select n_name from customer, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'nonASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01'  and s_acctbal > 20 and test = 'dfersr' group by n_name order by revenue desc;"
    print(getDiff(q1,q2))           
