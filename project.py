import difflib
from configparser import ConfigParser
from typing import Dict, Any, List, Tuple, Set

import psycopg2
import sqlparse

import interface

SQL_KEYWORDS = {
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "HAVING",
    "ORDER BY",
    "LIMIT",
    "OFFSET",
    "JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "FULL JOIN",
    "CROSS JOIN",
    "NATURAL JOIN",
    "USING",
    "DISTINCT",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "VALUES",
    "FETCH",
    "NEXT",
    "LAST",
    "FIRST",
    "PRIOR",
    "CURRENT",
    "ROW",
    "ROWS",
    "OVER",
    "PARTITION" "BY",
    "RANK",
    "DENSE_RANK",
    "ROW_NUMBER",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "CAST",
    "COALESCE",
    "NULLIF",
    "GREATEST",
    "LEAST",
}


# Query Tree Nodes =======================================================
class QueryPlanNode:
    def __init__(self, node: Dict[str, Any], left=None, right=None):
        self.node: str = node["Node Type"]
        self.totalCost: int = node["Total Cost"]
        self.ParentRelation: str = (
            node["Parent Relationship"] if "Parent Relationship" in node else ""
        )
        self.left: QueryPlanNode = left
        self.right: QueryPlanNode = right

    def __hash__(self):
        return hash(
            self.node + str(self.totalCost) + str(self.ParentRelation) + "true"
            if self.left
            else "false" + "true"
            if self.right
            else "false"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanNode):
            return False
        return self.node == other.node and self.ParentRelation == other.ParentRelation

    def __str__(self) -> str:
        return f"{self.node}, {self.left}, {self.right}"


class QueryPlanSortNode(QueryPlanNode):
    def __init__(
        self,
        node: Dict[str, Any],
        left: QueryPlanNode = None,
        right: QueryPlanNode = None,
    ):
        super().__init__(node, left, right)
        self.SortKeys: List[str] = node["Sort Key"]

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Sort Key" in node and node["Sort Key"] == "Sort"

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __hash__(self):
        return hash(str(super().__hash__()) + str(self.SortKeys))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanSortNode):
            return False
        return super().__eq__(other) and self.SortKeys == other.SortKeys


class QueryPlanGroupNode(QueryPlanNode):
    def __init__(
        self,
        node: Dict[str, Any],
        left: QueryPlanNode = None,
        right: QueryPlanNode = None,
    ):
        super().__init__(node, left, right)
        self.GroupKeys: List[str] = node["Group Key"]

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Group Key" in node

    def __hash__(self):
        return hash(str(super().__hash__()) + str(self.GroupKeys))

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanGroupNode):
            return False
        return super().__eq__(other) and self.GroupKeys == other.GroupKeys


class QueryPlanJoinNode(QueryPlanNode):
    def __init__(
        self,
        node: Dict[str, Any],
        left: QueryPlanNode = None,
        right: QueryPlanNode = None,
    ):
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

    def __hash__(self):
        return hash(str(super().__hash__()) + str(self.JoinType) + str(self.JoinCond))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanJoinNode):
            return False
        return (
            super().__eq__(other)
            and self.JoinType == other.JoinType
            and self.JoinCond == other.JoinCond
        )


class QueryPlanScanNode(QueryPlanNode):
    def __init__(
        self,
        node: Dict[str, Any],
        left: QueryPlanNode = None,
        right: QueryPlanNode = None,
    ):
        super().__init__(node, left, right)
        self.RelationName: str = node["Relation Name"]
        self.Alias: str = node["Alias"]
        self.IndexName: str = node["Index Name"] if "Index Name" in node else ""
        self.IndexCond: str = node["Index Cond"] if "Index Cond" in node else ""
        self.Filter: str = node["Filter"] if "Filter" in node else ""

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Relation Name" in node

    def __str__(self) -> str:
        return f"{super().__str__()}"

    def __hash__(self):
        return hash(
            str(super().__hash__())
            + str(self.RelationName)
            + str(self.IndexName)
            + str(self.IndexCond)
            + str(self.Filter)
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryPlanScanNode):
            return False
        return (
            super().__eq__(other)
            and self.RelationName == other.RelationName
            and self.IndexName == other.IndexName
            and self.IndexCond == other.IndexCond
            and self.Filter == other.Filter
        )


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

    def isEq(
        self,
        node1: QueryPlanNode,
        node2: QueryPlanNode,
        leftTree: Set[QueryPlanNode],
        rightTree: Set[QueryPlanNode],
    ) -> None:
        if node1 is None and node2 is None:
            return
        if node1 != node2:
            if node1 is not None:
                leftTree.add(node1)
            if node2 is not None:
                rightTree.add(node2)

        self.isEq(
            node1.left if node1 is not None else None,
            node2.left if node2 is not None else None,
            leftTree,
            rightTree,
        )
        self.isEq(
            node1.right if node1 is not None else None,
            node2.right if node2 is not None else None,
            leftTree,
            rightTree,
        )

    def IsEqual(self, other) -> Tuple[bool, Set[QueryPlanNode], Set[QueryPlanNode]]:
        if not isinstance(other, QueryPlan):
            return False, {self.root}, {other.root}

        leftTree, rightTree = set(), set()
        self.isEq(self.root, other.root, leftTree, rightTree)

        return len(leftTree) + len(rightTree) == 0, leftTree, rightTree


# DB
def query(sqlQuery: str) -> Dict[str, Any]:
    try:
        conn = psycopg2.connect(**getDBConfig())
        cursor = conn.cursor()
        cursor.execute(f"EXPLAIN (FORMAT JSON) {sqlQuery}")
        queryPlan = cursor.fetchall()
        return queryPlan[0][0][0]
    except psycopg2.Error as e:
        if e.pgcode and e.pgcode.startswith("42"):
            raise Exception(f"Invalid SQL query: {e}")
        else:
            raise Exception(f"Error while querying the database: {e}")
    finally:
        cursor.close()
        conn.close()


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
    formattedSQL = sqlparse.format(
        sqlQuery.replace("(", "").replace(")", ""),
        reindent=True,
        keyword_case="upper",
        strip_comments=True,
    )
    return formattedSQL, groupFormattedSQLByClause(formattedSQL)


"""
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
"""


def groupFormattedSQLByClause(sqlQuery: str) -> List[List[str]]:
    groupedSQL = []
    tempGroup = []
    splitSQLQuery = sqlQuery.split("\n")
    for line in splitSQLQuery:
        strippedLine = line.strip()
        strippedLine = strippedLine.split(" ")
        if (
            strippedLine[0] in SQL_KEYWORDS
            or len(strippedLine) > 1
            and f"{strippedLine[0]} {strippedLine[1]}" in SQL_KEYWORDS
        ):
            groupedSQL.append(tempGroup) if len(tempGroup) > 0 else None
            tempGroup = [line]
        else:
            tempGroup.append(line)

    groupedSQL.append(tempGroup)
    return groupedSQL


def getDiff(sql1: str, sql2: str) -> dict:
    _, list1 = parseSQL(sql1)
    _, list2 = parseSQL(sql2)
    diff = {
        "Modified": [],
        "Removed": [],
        "Added": [],
    }
    for i, (sublist1, sublist2) in enumerate(zip(list1, list2)):
        for i in range(len(sublist1)):
            sublist1[i] = sublist1[i].replace(",", "")
        for i in range(len(sublist2)):
            sublist2[i] = sublist2[i].replace(",", "")

        if sublist1 != sublist2:
            sm = difflib.SequenceMatcher(
                lambda x: x in SQL_KEYWORDS, sublist1, sublist2
            )

            for tag, i1, i2, j1, j2 in sm.get_opcodes():
                str1 = " ".join(sublist1[i1:i2])
                str2 = " ".join(sublist2[j1:j2])
                if tag == "replace":
                    diff["Modified"].extend([[str1, str2]])
                elif tag == "delete":
                    diff["Removed"].extend([str1])
                elif tag == "insert":
                    diff["Added"].extend([str2])

    return diff


if __name__ == "__main__":
    interface.start_ui()
