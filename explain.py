from project import *
import re

Query = []
diffInQueryPlan = {
    "diffInJoinNode": [],
    "diffInScanNode": [],
    "diffInGroupNode": [],
    "diffInSortNode": [],
    "diffInPlan": [],
}
queries_subset = []


def explain(diffInQuery, diffInPlan):
    # Remove Empty List
    diffInQuery = {i: j for i, j in diffInQuery.items() if j != []}
    print("diffInQuery:", diffInQuery)
    print("diffInPlan:", diffInPlan)

    queryplann(diffInPlan)
    gettingAdd(diffInQuery)

    explaination = ""
    filter_exp = r"(?i)(?:AND|NOT|OR|WHERE)\s+(.*)"

    print("diffInQueryPlan:", diffInQueryPlan)
    print("queries_subset:", queries_subset)

    count = 0
    size = len(set(diffInQueryPlan["diffInJoinNode"]))
    size1 = len(set(diffInQueryPlan["diffInScanNode"]))
    for status, values in diffInQuery.items():
        if count == 1:
            explaination += "And with "
        else:
            explaination += "With "

        if status == "Added":
            explaination += "an addition of "
        elif status == "Removed":
            explaination += "the removal of "
        elif status == "Modified":
            explaination += "modification of "

        count1 = 0
        for value in values:
            if status == "Modified":
                if count1 == 0:
                    explaination += "join condition that changes from "
                if count1 % 2 == 0 and count1 != 0:
                    explaination += "and "
                    count1 = 0
                match_to = re.search(filter_exp, " ".join(value[0].split()))
                match_from = re.search(filter_exp, " ".join(value[1].split()))

                if match_to and match_from:
                    explaination += (
                        f"\"{match_to.group(1)}' to '{match_from.group(1)}\" "
                    )
                count1 += 2
            else:
                if count1 == 0:
                    explaination += "join condition such as "
                if count1 % 2 == 1:
                    explaination += "and "
                    count1 = 0

                match = re.search(filter_exp, " ".join(value.split()))
                if match:
                    explaination += f'"{match.group(1)}" '

                count1 += 1
        count = 1
        if status == "Added":
            explaination = explaination.rstrip()
            explaination += ", extra filter are needed during "
            for i in queries_subset:
                explaination += i
                if size1 == 1:
                    explaination += ". "
                else:
                    explaination += " and "
                    size1 -= 1
        elif status == "Removed":
            explaination = explaination.rstrip()
            explaination += ", lesser to none filter are required for scans. "
        elif status == "Modified":
            explaination = explaination.rstrip()
            explaination += ", extra filtering is not needed. "
    for i in set(diffInQueryPlan["diffInPlan"]):
        if i == "Gather Merge":
            explaination += "The query plan uses a Gather Merge that combines the output of its child nodes, which are executed by parallel workers. "
        elif i == "Gather":
            explaination += "The query plan uses a Gather operation to distribute the scanning of the table among two parallel workers. "
        elif i == "Hash":
            explaination += "Hashing have been used. "
        elif i == "Sort":
            explaination += "Sorting have been used. "
        elif i == "Materialize":
            explaination += "The query plan uses Materialization to the process of creating an intermediate result set. This maybe due to the size of the result set being too large to fit into memory or when multiple operations are performed on the same data. "
        elif i == "Memoize":
            explaination += "The query plan uses Memoize to improve the performance of recursive queries. "
        else:
            explaination += "NEVER SEEN"
    for i in set(diffInQueryPlan["diffInGroupNode"]):
        if i == "Aggregate":
            explaination += "Grouping techniqes such as aggregation are used. "
        else:
            explaination += "NEVER SEEN"
    if size > 0:
        explaination += "Such joined operaion have been used: "
    for i in set(diffInQueryPlan["diffInJoinNode"]):
        explaination += i
        if size == 1:
            explaination += ". "
        else:
            explaination += " and "
            size -= 1
    for i in set(diffInQueryPlan["diffInSortNode"]):
        explaination += i + " ."
    return explaination


def queryplann(output):
    if output[2] == None:
        print("No changes to query plan")
    else:
        Query.append("Changes: ")
        print_nodes(output[2])

    for i in diffInQueryPlan:
        print(diffInQueryPlan[i])


def print_nodes(node):
    Query.append(node.node)
    if type(node) == QueryPlanJoinNode:
        # print(node.JoinCond)
        diffInQueryPlan["diffInJoinNode"].append(node.node)
    if type(node) == QueryPlanScanNode:
        if node.Filter != "":
            diffInQueryPlan["diffInScanNode"].append(
                node.Filter + "- " + node.node + " on: " + node.RelationName
            )
    if type(node) == QueryPlanSortNode:
        diffInQueryPlan["diffInSortNode"].append(node.node)
    if type(node) == QueryPlanGroupNode:
        diffInQueryPlan["diffInGroupNode"].append(node.node)
    if type(node) == QueryPlanNode:
        diffInQueryPlan["diffInPlan"].append(node.node)
    if node.left:
        print_nodes(node.left)
    if node.right:
        print_nodes(node.right)


def gettingAdd(diffInQuery):
    global queries_subset
    matching_indices = []
    for i, s in enumerate(diffInQueryPlan["diffInScanNode"]):
        for q in diffInQuery["Added"]:
            print("s:", s)
            print("q:", q)
            if q in s.replace("(", "").replace(")", "").replace("'", ""):
                matching_indices.append(i)

    queries_subset = [diffInQueryPlan["diffInScanNode"][i] for i in matching_indices]
    queries_subset = [s.split("-")[1].strip() for s in queries_subset]
