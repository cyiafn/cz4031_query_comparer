from configparser import ConfigParser
from typing import Dict, Any, List

import psycopg2


# Query Tree Nodes =======================================================
class QueryPlanNode:
    def __init__(self, node: Dict[str, Any], left=None, right=None):
        self.node: str = node["Node Type"]
        self.totalCost: int = node["Total Cost"]
        self.ParentRelation: str = node["Parent Relationship"] if "Parent Relationship" in node else ""
        self.left: QueryPlanNode = left
        self.right: QueryPlanNode = right

    def __eq__(self, other: Any):
        if not isinstance(other, QueryPlanNode):
            return False
        return self.node == other.node and self.totalCost == other.totalCost and \
            self.ParentRelation == other.ParentRelation

    def __str__(self):
        return f"{self.node}, {self.left}, {self.right}"


class QueryPlanSortNode(QueryPlanNode):
    def __init__(self, node: Dict[str, Any], left: QueryPlanNode = None, right: QueryPlanNode = None):
        super().__init__(node, left, right)
        self.SortKeys: List[str] = node["Sort Key"]

    @staticmethod
    def Am(node: Dict[str, any]) -> bool:
        return "Sort Key" in node and node["Sort Key"] == "Sort"

    def __str__(self):
        return f"{super().__str__()}"

    def __eq__(self, other: Any):
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

    def __str__(self):
        return f"{super().__str__()}"

    def __eq__(self, other: Any):
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

    def generateJoinCond(self, node: Dict[str, Any]) -> str:
        if "Hash Cond" in node:
            return node["Hash Cond"]
        if "Filter" in node:
            return node["Filter"]
        return ""

    def __str__(self):
        return f"{super().__str__()}"

    def __eq__(self, other: Any):
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

    def __str__(self):
        return f"{super().__str__()}"

    def __eq__(self, other: Any):
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

    def print(self):
        print(self.root)


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


if __name__ == "__main__":
    plan = query(
        "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01' and c_acctbal > 10 and s_acctbal > 20 group by n_name order by revenue desc;")
    QueryPlan(plan["Plan"]).print()
